import subprocess
import commands
import re
import datetime
import sys
import os
import json
import node_statistics
from dateutil import parser
from xml.dom import minidom
from collections import OrderedDict

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dbUtils import DBUtil
from csvUtil import CsvUtil
from htmlUtil import HtmlUtil
from sendMailUtil import EmailUtils
from loggerUtil import loggerUtil
from propertyFileUtil import PropertyFileUtil
from postgres_connection import PostgresConnection

current_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'. \
    format(scriptName=script_name, currentDate=current_date)
LOGGER = loggerUtil.__call__().get_logger(log_file)
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").
     read()).replace("(", "(\"").replace(")", "\")")

email_alert_of_services_down = ['Services Down']


def get_services():
    global var_date
    var_date = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:00')
    xml_parser = minidom.parse(r'/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
    parent_tag = xml_parser.getElementsByTagName('ServiceStability')[0]
    property_tag = parent_tag.getElementsByTagName('property')
    services = {}
    fetch_services_details(property_tag, services, var_date)
    services = OrderedDict(sorted(services.items(),
                                  key=lambda items: items[0]))
    return services


def fetch_services_details(property_tag, services, var_date):
    for property in property_tag:
        struct = {}
        try:
            struct['hostName'] = globals()[property.attributes[
                'hostName'].value]
            struct['searchString'] = property.attributes['searchString'].value
            struct['checkType'] = property.attributes['checkType'].value
            services[property.attributes['Name'].value] = struct
            if 'sshNode' in [propertyItem.name
                             for propertyItem in property.attributes.values()]:
                services[property.attributes['Name'].value]['sshNode'] = \
                    property.attributes['sshNode'].value
        except Exception as e:
            LOGGER.info(e)
            write_to_csv(var_date, host=None, service=property.attributes[
                'Name'].value)


def use_jps_cmd_to_fetch_service_pid(service):
    if services[service].get('sshNode'):
        ssh_parent_node_and_use_jps_to_fetch_service_pid(service)
    else:
        for host in services[service]['hostName'].split(' '):
            commandstr = '''ssh {0} "jps | grep {1} |
            gawk -F\\" \\" '{{print $1}}' "'''.format(host,
                                                      services[service]
                                                      ['searchString'])
            use_cat_head_and_jps_to_fetch_service_pid(commandstr, service,
                                                      host)


def ssh_parent_node_and_use_jps_to_fetch_service_pid(service):
    host = services[service]['hostName'].split(' ')[0]
    sshnode = services[service]['sshNode']
    nodes = commands.getoutput(
        'ssh %s "source /opt/nsn/ngdb/ifw/lib/platform/utils/'
        'platform_definition.sh; echo \\${%s[@]} "' % (host, sshnode))
    for node in nodes.split(' '):
        if services[service]['checkType'] == 'jps':
            commandstr = '''ssh {0} "ssh {1} jps | grep "{2}" |
            gawk -F\\" \\" '{{print $1}}' "'''.format(host, node,
                                                      services[service]
                                                      ['searchString'])
        else:
            commandstr = '''ssh {0} "ssh {1} jps -v |grep -iw {2} |
            gawk -F\\" \\" '{{print $1}}'"'''.format(host, node,
                                                     services[service]
                                                     ['searchString'])
        use_cat_head_and_jps_to_fetch_service_pid(commandstr, service,
                                                  node, host)


def use_systemctl_to_fetch_service_pid(service):
    searchstr1, searchstr2 = services[service]['searchString'].split(',')
    active = False
    for host in services[service]['hostName'].split(' '):
        commandstr = '''ssh {0} "systemctl status {1} | grep \\"{2}\\" |
        gawk -F\\" \\" '{{print $3}}'"'''.format(host, searchstr1, searchstr2)
        output = commands.getoutput(commandstr)
        if output:
            active = True
    
    if not active:
        write_service_down(service, host)
    else:
        write_pid_of_service_using_systemctl(commandstr, host, service)


def write_pid_of_service_using_systemctl(commandstr, host, service):
    try:
        pid = commands.getoutput(commandstr).split(" ")[3]
        write_service_last_restart_time(service, pid, host)
    except Exception as e:
        LOGGER.info(e)
        write_service_down(service, host)


def use_pseaf_to_fetch_service_pid(service):
    searchstr1, searchstr2 = services[service]['searchString'].split(',')
    for host in services[service]['hostName'].split(' '):
        commandstr = '''ssh {0} "ps -eaf | grep {1} | grep ^{2} |
        gawk -F\\" \\" '{{print $2}}'"'''.format(host, searchstr1, searchstr2)
        if not commands.getoutput(commandstr):
            write_service_down(service, host)
        else:
            pid = list(filter(lambda x: x, commands.getoutput(commandstr).
                              split(' ')))[1]
            write_service_last_restart_time(service, pid, host)


def use_cat_to_fetch_service_pid(service):
    BOXI_PID_DIRECTORY = "/opt/nsn/ngdb/BOEnterprise/sap_bobj/serverpids"
    if service == 'BoxiWebiServer':
        CCM_SHELL_PATH = '/opt/nsn/ngdb/BOEnterprise/sap_bobj/'
        cemod_boxi_admin_name = globals()['cemod_boxi_admin_name']
        boxi_decrypted_pass = commands.getoutput(
            'perl /opt/nsn/ngdb/ifw/lib/common/decryption_util.pl %s' %
            globals()['cemod_boxi_cms_pass'])
        for host in services[service]['hostName'].split(' '):
            find_number_of_boxi_webi_servers(BOXI_PID_DIRECTORY,
                                             CCM_SHELL_PATH,
                                             boxi_decrypted_pass,
                                             cemod_boxi_admin_name,
                                             host, service)
    else:
        for host in services[service]['hostName'].split(' '):
            commandstr = 'ssh {0} "cat {1}/*{2}.pid"'. \
                format(host, BOXI_PID_DIRECTORY,
                       services[service]['searchString'])
            write_pid_of_service(commandstr, service, host)


def find_number_of_boxi_webi_servers(BOXI_PID_DIRECTORY, CCM_SHELL_PATH,
                                     boxi_decrypted_pass,
                                     cemod_boxi_admin_name, host,
                                     service):
    boxi_host_name = commands.getoutput('ssh %s "hostname"' % host)
    no_of_config_webi_servers = commands.getoutput(''' ssh {0} "su - bobje -c 'sh {1}/ccm.sh -display -cms {2}:6400 -username {3} -password {4} | grep  \"WebIntelligenceProcessingServer\" | wc -l'" '''.format(host, CCM_SHELL_PATH, boxi_host_name,
                              cemod_boxi_admin_name, boxi_decrypted_pass))
    for i in range(1, int(no_of_config_webi_servers)):
        commandstr = 'ssh {0} "cat {1}/*{2}{3}.pid"'.format(host,
                                                            BOXI_PID_DIRECTORY,
                                                            services[service]
                                                            ['searchString'],
                                                            str(i))
        write_pid_of_service(commandstr, service + str(i), host)


def write_pid_of_service(commandstr, service, host):
    if not commands.getoutput(commandstr):
        write_service_down(service, host)
    elif 'No such file or directory' in commands.getoutput(commandstr):
        write_service_down(service, host)
    else:
        try:
            pid = re.search('\d+', commands.getoutput(commandstr)).group()
        except Exception as e:
            LOGGER.info(e)
            write_service_down(service, host)
        else:
            write_service_last_restart_time(service, pid, host)


def use_cat_head_to_fetch_service_pid(service):
    POSTGRES_PID_PATH = "/opt/nsn/ngdb/postgres-9.5/data"
    if services[service].get('sshNode'):
        host, nodes = get_postgres_nodes(service)
        for node in nodes.split(' '):
            commandstr = '''ssh {0} "ssh {1} cat {2}/postmaster.pid |
            head -1"'''.format(host, node, POSTGRES_PID_PATH)
            use_cat_head_and_jps_to_fetch_service_pid(commandstr, service,
                                                      node, host)
    else:
        for host in services[service]['hostName'].split(' '):
            commandstr = 'ssh {0} "cat {1}/postmaster.pid | head -1"'. \
                format(host, POSTGRES_PID_PATH)
            use_cat_head_and_jps_to_fetch_service_pid(commandstr, service,
                                                      host)


def use_cat_head_and_jps_to_fetch_service_pid(commandstr, service,
                                              node, host=None):
    if not commands.getoutput(commandstr):
        write_service_down(service, node)
    else:
        pid = commands.getoutput(commandstr).split(" ")[0]
        write_service_last_restart_time(service, pid, node, host)


def get_postgres_nodes(service):
    host = services[service]['hostName'].split(' ')[0]
    sshnode = services[service]['sshNode']
    nodes = commands.getoutput(
        'ssh %s "source /opt/nsn/ngdb/ifw/lib/platform/utils/'
        'platform_definition.sh; echo \\${%s[@]} "' % (host, sshnode))
    return host, nodes


def use_ssnlp_to_fetch_service_pid(service):
    for host in services[service]['hostName'].split(' '):
        commandstr = '''ssh {0} "ss -nlp | grep {1} | tr -s ' ' |
        gawk -F\\",\\" '{{print $2}}' | gawk -F\\"=\\" '{{print $2}}'"'''. \
            format(host, services[service]['searchString'])
        if not commands.getoutput(commandstr):
            LOGGER.info('{0} on {1} is DOWN'.format(service, host))
            write_service_down(service, host)
        else:
            fetch_and_write_pid(commandstr, host, service)


def fetch_and_write_pid(commandstr, host, service):
    try:
        opmsg = commands.getoutput(commandstr).split(" ")[6].split(',')[1]
        pid = re.search('\d+', opmsg).group()
    except Exception as e:
        LOGGER.info(e)
        write_service_down(service, host)
    else:
        write_service_last_restart_time(service, pid, host)


def write_service_last_restart_time(service, pid, host=None, ssh_node=None):
    try:
        int(pid)
    except Exception as e:
        LOGGER.info(e)
        write_service_down(service, host)
    else:
        if not ssh_node:
            commandstr = 'ssh {0} "ps -o lstart= -p {1}"'.format(host, pid)
        else:
            commandstr = 'ssh {0} "ssh {1} ps -o lstart= -p {2}"'. \
                format(ssh_node, host, pid)
        last_restart_time = \
            subprocess.Popen(commandstr, shell=True, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE).communicate()[0]
        last_restart_time = parser.parse(last_restart_time)
        uptime = get_date_difference(last_restart_time)
        down_time = 'Service is Up.'
        write_to_csv(var_date, host, service, str(last_restart_time),
                     uptime, down_time)


def write_service_down(service, host):
    LOGGER.info('{0} on {1} is DOWN'.format(service, host))
    email_alert_of_services_down.append('{0} on {1} is DOWN'.
                                        format(service, host))
    query = PropertyFileUtil('lastUptimeQuery', 'PostgresSqlSection'). \
        getValueForKey().replace('service_name', service)
    output = postgres_conn.fetch_records(query)
    if not output:
        query = query.replace('desc', '').replace('not', '')
        output = postgres_conn.fetch_records(query)
    if not output:
        write_to_csv(var_date, host, service, 'Service is Down',
                     'Service is Down', 'NA')
    else:
        last_uptime = parser.parse(output[0][0])
        down_time = get_date_difference(last_uptime)
        write_to_csv(var_date, host, service, 'Service is Down',
                     'Service is Down', down_time)


def get_date_difference(last_restart_time):
    now = datetime.datetime.now()
    delta = now - last_restart_time
    days = delta.days
    hours = delta.seconds // 3600
    minutes = delta.seconds // 60 - hours * 60
    seconds = delta.seconds % 60
    return str('{0} days {1} hours {2} minutes {3} seconds'.
               format(days, hours, minutes, seconds))


def write_to_csv(var_date, host, service, last_restart_time=None,
                 uptime=None, down_time=None):
    util_name = get_util_name(host)
    if not host:
        output = {'Date': var_date, 'Host': 'No Host-service Not configured',
                  'Service': service, 'Last_Restart_Time':
                      'Service Not configured', 'Uptime':
                      'Not configured so uptime not required', 'Downtime':
                      'No-host configured'}
    else:
        output = {'Date': var_date, 'Host': host, 'Service': service,
                  'Last_Restart_Time': last_restart_time, 'Uptime': uptime,
                  'Downtime': down_time}
    CsvUtil().writeDictToCsv(util_name, output)
    convert_to_json(util_name, output)


def convert_to_json(util_name, output):
    if util_name == 'ServiceStability':
        push_services_status_to_db.append(json.dumps(output))
    else:
        push_portal_services_status_to_db.append(json.dumps(output))


def get_util_name(host):
    if host in cemod_portal_hosts.split(' '):
        util_name = 'PortalStability'
    else:
        util_name = 'ServiceStability'
    return util_name


def fetch_service_pid_and_last_restart_time(service):
    if services[service]['checkType'] == 'jps' or \
            services[service]['checkType'] == 'jps -v':
        use_jps_cmd_to_fetch_service_pid(service)
    elif services[service]['checkType'] == 'systemctl':
        use_systemctl_to_fetch_service_pid(service)
    elif services[service]['checkType'] == 'ps -eaf':
        use_pseaf_to_fetch_service_pid(service)
    elif services[service]['checkType'] == 'cat':
        use_cat_to_fetch_service_pid(service)
    elif services[service]['checkType'] == 'catHead':
        use_cat_head_to_fetch_service_pid(service)
    elif services[service]['checkType'] == 'ss -nlp':
        use_ssnlp_to_fetch_service_pid(service)


def push_output_data_to_postgres(hm_type, output_data):
    json_data = '[%s]' % (','.join(output_data))
    DBUtil().jsonPushToPostgresDB(json_data, hm_type)


def main():
    global services, postgres_conn, push_services_status_to_db, \
        push_portal_services_status_to_db
    push_services_status_to_db, push_portal_services_status_to_db = [], []
    postgres_conn = PostgresConnection().get_instance()
    services = get_services()
    for service in services.keys():
        fetch_service_pid_and_last_restart_time(service)
    if len(email_alert_of_services_down) > 1:
        html = HtmlUtil().generateHtmlFromList('Below services are Down',
                                               email_alert_of_services_down)
        EmailUtils().frameEmailAndSend('List of services which are Down', html)
    for hm_type, output_data in [('ServiceStability',
                                  push_services_status_to_db),
                                 ('PortalStability',
                                  push_portal_services_status_to_db)]:
        push_output_data_to_postgres(hm_type, output_data)
    node_statistics.main(push_services_status_to_db)
    postgres_conn.close_connection()


if __name__ == '__main__':
    main()
