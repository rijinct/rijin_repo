import sys
import os, re
import datetime
from commands import getoutput
from xml.dom import minidom

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from csvUtil import CsvUtil
from propertyFileUtil import PropertyFileUtil
from loggerUtil import loggerUtil
from jsonUtils import JsonUtils
from dbUtils import DBUtil

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(", "(\"").replace(")",
                                                                                                                   "\")")
current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name, currentDate=current_time)
LOGGER = loggerUtil.__call__().get_logger(log_file)


def get_services():
    global services
    xmlparser = minidom.parse(r'/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
    parentTag = xmlparser.getElementsByTagName('ServiceStability')[0]
    propertyTag = parentTag.getElementsByTagName('property')
    services = {}
    for property in propertyTag:
        struct = {}
        struct['hostName'] = globals()[property.attributes['hostName'].value]
        struct['searchString'] = property.attributes['searchString'].value
        struct['checkType'] = property.attributes['checkType'].value
        services[property.attributes['Name'].value] = struct


def jpstype_cmd_type(service):
    for host in services[service]['hostName'].split(' '):
        commandstr = '''ssh {0} "jps | grep {1} | gawk -F\\" \\" '{{print $1}}' "'''.format(host,
                                                                                            services[service][
                                                                                                'searchString'])
        if not getoutput(commandstr):
            write_service_down_to_csv(service, host)
        else:
            try:
                pid = int(getoutput(commandstr).split(' ')[0])
                get_service_stats(pid, service, host)
            except:
                write_service_down_to_csv(service, host)


def pseaf_cmd_type(service):
    search_str1, search_str2 = services[service]['searchString'].split(',')
    for host in services[service]['hostName'].split(' '):
        commandstr = '''ssh {0} "ps -eaf | grep {1} | grep ^{2} | gawk -F\\" \\" '{{print $2}}'"'''.format(
            host, search_str1,
            search_str2)
        pid = getoutput(commandstr)
        if not getoutput(commandstr):
            write_service_down_to_csv(service, host)
        else:
            try:
                pid = int(list(filter(lambda x: x, pid.split(' ')))[1])
                get_service_stats(pid, service, host)
            except:
                write_service_down_to_csv(service, host)


def systemctl_cmd_type(service):
    searchstr1, searchstr2 = services[service]['searchString'].split(',')
    for host in services[service]['hostName'].split(' '):
        commandstr = '''ssh {0} "systemctl status {1} | grep \\"{2}\\" | gawk -F\\" \\" '{{print $3}}'"'''.format(host,
                                                                                                                  searchstr1,
                                                                                                                  searchstr2)
        if not getoutput(commandstr):
            write_service_down_to_csv(service, host)
        else:
            try:
                pid = getoutput(commandstr).split(" ")[3]
                get_service_stats(pid, service, host)
            except:
                write_service_down_to_csv(service, host)


def get_mem_of_service(pid, host_name):
    ou_value = getoutput(
        """ssh {0} " jstat -gc {1} | gawk -F\\" \\" '{{print \\$8}}' " """.format(host_name,
                                                                                  pid)).split("\n")[1]
    eu_value = getoutput(
        """ssh {0} " jstat -gc {1} | gawk -F\\" \\" '{{print \\$6}}' " """.format(host_name,
                                                                                  pid)).split("\n")[1]
    s0u_value = getoutput(
        """ssh {0} " jstat -gc {1} | gawk -F\\" \\" '{{print \\$3}}' " """.format(host_name,
                                                                                  pid)).split("\n")[1]
    s1u_value = getoutput(
        """ssh {0} " jstat -gc {1} | gawk -F\\" \\" '{{print \\$4}}' " """.format(host_name,
                                                                                  pid)).split("\n")[1]
    total_memory = (float(ou_value) + float(eu_value) + float(s0u_value) + float(s1u_value)) / 1000
    return '%2f' % (total_memory)


def get_service_stats(pid, service, host_name):
    cpu_use = getoutput(
        ''' ssh %s top -p %s -bn1 | grep "Cpu(s)" | awk '{print 100-$8}' ''' % (host_name, pid))
    openfiles = getoutput('ssh %s "lsof -p %s | wc -l"' % (host_name, pid))
    if check_jstat_service(pid, host_name):
        mem = get_mem_of_service(pid, host_name)
    else:
        mem = getoutput('''ssh %s top -p %s -bn1 | grep "KiB Mem" | awk '{print $4}' ''' % (host_name, pid))
        mem = '%2f' % (float(mem.replace('+total,', '')) / 1024)
    write_to_csv(service, cpu_use, mem, openfiles, host_name)


def check_jstat_service(pid, host_name):
    output = getoutput('''ssh {0} " jstat -gc {1}" '''.format(host_name, pid))
    return output != '' and output != '%s not found' % pid


def write_to_csv(service, cpu, mem, openfiles, host):
    data_to_write = {'Time': current_time, 'Service': service, 'Host': host, 'Status': 'Running', 'CPU_USE': cpu,
                     'TotalMEM': mem, 'OpenFiles': openfiles}
    for data_type in (cpu, mem, openfiles):
        pattern = '[a-zA-Z]'
        try:
            if re.match(pattern, data_type):
                LOGGER.info(data_type)
                for key, value in data_to_write.items():
                    if value == data_type:
                        data_to_write[key] = 'NA'
        except:
            pass
    LOGGER.info('Writing cpu, mem, openfiles of %s to csvfile' % service)
    if host in cemod_portal_hosts.split(' '):
        CsvUtil().writeDictToCsv('PortalStats', data_to_write)
        DBUtil().jsonPushToPostgresDB(('[%s]' % data_to_write).replace("'", '"'), 'PortalStats')
    else:
        CsvUtil().writeDictToCsv('ServiceStats', data_to_write)
        DBUtil().jsonPushToPostgresDB(('[%s]' % data_to_write).replace("'", '"'), 'ServiceStats')


def write_service_down_to_csv(service, host):
    data_to_write = {'Time': current_time, 'Service': service, 'Host': host, 'Status': 'Down', 'CPU_USE': 'NA',
                     'TotalMEM': 'NA',
                     'OpenFiles': 'NA'}
    if host in cemod_portal_hosts.split(' '):
        CsvUtil().writeDictToCsv('PortalStats', data_to_write)
        DBUtil().jsonPushToPostgresDB(('[%s]' % data_to_write).replace("'", '"'), 'PortalStats')
    else:
        CsvUtil().writeDictToCsv('ServiceStats', data_to_write)
        DBUtil().jsonPushToPostgresDB(('[%s]' % data_to_write).replace("'", '"'), 'ServiceStats')


def main():
    get_services()
    for service in PropertyFileUtil('statsForServices', 'HeaderSection').getValueForKey().split(','):
        if services[service]['checkType'] == 'jps':
            jpstype_cmd_type(service)
        elif services[service]['checkType'] == 'ps -eaf':
            pseaf_cmd_type(service)
        elif services[service]['checkType'] == 'systemctl':
            systemctl_cmd_type(service)


if __name__ == '__main__':
    main()
