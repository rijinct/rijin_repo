#! /usr/bin/python
#####################################################################################
#####################################################################################
# (c)2019 NOKIA
# Author:  SDK Team
# Version: 0.1
# Purpose: This script will restart scheduler when "Too many open file issue" arises
# Date:    18-10-2019
#####################################################################################
import psycopg2
import sys
import traceback
import datetime
import logging
import subprocess

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(", "(\"").replace(")", "\")")
currentDate= datetime.datetime.now().strftime("%Y-%m-%d")
filename = "/var/local/monitoring/output/checkOpenFile/monitorScheduler_{}.log".format(currentDate)

def execute_query(sql):
        try:
                con = psycopg2.connect(database=cemod_sdk_db_name, user=cemod_application_sdk_database_linux_user, password="", host=cemod_postgres_sdk_fip_active_host, port=cemod_application_sdk_db_port)
                cur = con.cursor()
                cur.execute(sql)
                result = cur.fetchone()
                return result[0]
        except:
                logIntoFile("Error executing query")
                logIntoFile(traceback.format_exc())
        finally:
                con.close()

def get_last_hour():
        return (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

def monitor_scheduler_restart():
        last_hour = get_last_hour()

        last_hour_job_count_query = "select count(distinct(job_name)) from sairepo.etl_status where start_time >= '{}' and job_name like {}".format(last_hour, "'Usage%'")

        logIntoFile("last_hour_job_count_query: {}".format(last_hour_job_count_query))
        last_hour_job_count = execute_query(last_hour_job_count_query)
        print("Total Usage Jobs in Last Hour",last_hour_job_count)
        logIntoFile("last_hour_job_count: {}".format(last_hour_job_count))

        last_hour_jobs_error_query = "select count(distinct(job_name)) from sairepo.etl_status where start_time >= '{}' and  status='{}' and job_name like '{}' and error_description like '{}'".format(last_hour,'E','Usage%','%Too many%')
        logIntoFile("last_hour_jobs_error_query: {}".format(last_hour_jobs_error_query))
        last_hour_jobs_error_count = execute_query(last_hour_jobs_error_query)
        print("Total Usage Jobs in Last Hour in Error State:",last_hour_jobs_error_count)
        logIntoFile("last_hour_jobs_error_count: {}".format(last_hour_jobs_error_count))

        if (last_hour_job_count == last_hour_jobs_error_count) and (last_hour_job_count!=0 and last_hour_jobs_error_count!=0):
                print("Triggering Scheduler Restart")
                triggerSchedulerRestart()

def triggerSchedulerRestart():
        logIntoFile("Invoked triggerSchedulerRestart() method")
        script_to_restart_scheduler="perl /opt/nsn/ngdb/ifw/bin/application/cem/application_services_controller.pl --service scheduler --level stop"
        scheduler_node = cemod_scheduler_hosts.split(" ")[0]
        ssh = subprocess.Popen(["ssh", "%s" % scheduler_node, script_to_restart_scheduler],shell=False,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        if result == []:
                print("Scheduler Restarted Successfully")
                logIntoFile("Scheduler restarted successfully, check the logs at /opt/nsn/ngdb/scheduler/app/log/scheduler-service* ")
        else:
                error = ssh.stderr.readlines()
                print("Error occured restarting scheduler {0} pls check /opt/nsn/ngdb/scheduler/app/log/scheduler-service/".format(error))
                logIntoFile("Error occured restarting scheduler {0} pls check /opt/nsn/ngdb/scheduler/app/log/scheduler-service/").format(error)

def logIntoFile(logs):
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s,%(message)s',datefmt='%H:%M:%S',filename=filename,filemode='a')
        logging.error(logs)
        l = logging.getLogger()
        for hdlr in l.handlers[:]:
                l.removeHandler(hdlr)


if __name__ == "__main__":
        monitor_scheduler_restart()