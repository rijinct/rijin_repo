'''
Created on 09-Jun-2020

@author: a4yadav
'''
import sys
import os
from commands import getoutput
from datetime import datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from propertyFileUtil import PropertyFileUtil
from sendMailUtil import EmailUtils
from htmlUtil import HtmlUtil
from loggerUtil import loggerUtil
from postgres_connection import PostgresConnection

exec(
    open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").
    read()).replace("(", "(\"").replace(")", "\")")


def main():
    global postgres_conn
    postgres_conn = PostgresConnection().get_instance()
    create_logger()
    job_type, threshold = get_job_type_and_threshold()
    jobs = get_hung_jobs(job_type, threshold)
    if jobs:
        logger.info('Hung jobs: {0}'.format(jobs))
        change_status_to_E(jobs)
        generate_xml_trigger_for_hung_jobs(jobs)
        send_email_alert(jobs)
    else:
        logger.info('No hung jobs')


def create_logger():
    global logger
    log_file_name = os.path.basename(sys.argv[0]).replace('.py', '')
    log_file_time = datetime.now().strftime('%Y-%m-%d')
    logger = loggerUtil.__call__().get_logger('{0}_{1}'.format(
        log_file_name, log_file_time))


def get_job_type_and_threshold():
    try:
        job_type, threshold = sys.argv[1], sys.argv[2]
        if job_type not in ('DAY', 'HOUR'):
            raise IndexError
        logger.info("Getting hung jobs for Type:%s Threshold:%s" %
                    (job_type, threshold))
    except IndexError:
        logger.info('Usage python {} DAY/HOUR THRESHOLD'.format(sys.argv[0]))
        sys.exit()
    return job_type, threshold


def get_hung_jobs(job_type, threshold):
    sql = PropertyFileUtil('getHungJobs',
                           'PostgresSqlSection').getValueForKey()
    time_zone = getoutput(
        '''timedatectl | grep "Time zone" | gawk -F" " '{print $3}' ''')
    sql = sql.replace('job_type', job_type).replace('threshold',
                                                    threshold).replace(
                                                        'time_zone', time_zone)
    output = postgres_conn.fetch_records(sql)
    return {item[1]: str(item[0]) for item in output}


def change_status_to_E(jobs):
    for job_name, proc_id in jobs.items():
        sql = PropertyFileUtil('updateStatusToE',
                               'PostgresSqlSection').getValueForKey()
        sql = sql.replace('jobName', job_name).replace('procId', proc_id)
        postgres_conn.execute_query(sql)


def generate_xml_trigger_for_hung_jobs(jobs):
    for job_name, proc_id in jobs.items():
        fail_job_group = get_fail_job_group(job_name)
        xml_path = generate_xml(job_name, fail_job_group)
        path = PropertyFileUtil('schedulerServicePath',
                                'DirectorySection').getValueForKey()
        command = "sh {0}scheduler-management-service.sh -file {1}".format(
            path, xml_path)
        getoutput(command)
        getoutput("sudo rm %s" % xml_path)


def get_fail_job_group(job_name):
    sql = PropertyFileUtil('getFailJobGroup',
                           'PostgresSqlSection').getValueForKey().replace(
                               'jobName', job_name)
    return postgres_conn.fetch_records(sql)[0][0]


def generate_xml(job_name, fail_job_group):
    conf_path = PropertyFileUtil('confPath',
                                 'DirectorySection').getValueForKey()
    template = '{0}job_trigger.xml'.format(conf_path, job_name)
    output_xml = '{0}{1}.xml'.format(conf_path, job_name)
    with open(template, "rt") as fin:
        data = fin.read()
        with open(output_xml, "wt") as fout:
            fout.write(
                data.replace('FAILJOBNAME',
                             job_name).replace('FAILJOBGROUP', fail_job_group))
    return output_xml


def send_email_alert(jobs):
    body = get_html_body(jobs)
    EmailUtils().frameEmailAndSend(
        "[ALERT] HUNG JOBS FIXED on {0}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")), body)


def get_html_body(jobs):
    data = [{
        'JobName': job_name,
        'ProcId': proc_id
    } for job_name, proc_id in jobs.items()]
    return str(HtmlUtil().generateHtmlFromDictList(
        "Below jobs were found to be in hung state and have been re-triggered",
        data))


if __name__ == '__main__':
    main()
