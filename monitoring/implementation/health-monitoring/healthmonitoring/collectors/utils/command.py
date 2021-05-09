import subprocess

from enum import Enum
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


class Command(Enum):

    HIVE_SHOW_TABLES = r'''su - {hdfs_user} -c "beeline -u '{hive_url}' --silent=true --showHeader=false --outputformat=csv2 -e 'show tables like \"{table}\";'"'''  # noqa: 501

    HIVE_SHOW_COLUMNS = r'''su - {hdfs_user} -c "beeline -u '{hive_url}' --silent=true --showHeader=false --outputformat=csv2 -e 'show columns from \"{table}\";'"'''  # noqa: 501

    HIVE_COLUMN_COUNT = r'''su - {hdfs_user} -c "beeline -u '{hive_url}' --silent=true --showHeader=false --outputformat=csv2 -e 'SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring; select count(*), count(distinct {column}) from {table} where dt=\'{start_epoch}\';'"'''  # noqa: 501

    HIVE_TOTAL_COUNT = r'''su - {hdfs_user} -c "beeline -u '{hive_url}' --silent=true --showHeader=false --outputformat=csv2 -e 'SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring; select count(*) from {table} where dt=\'{start_epoch}\';'"'''  # noqa: 501

    HIVE_GET_RECORD_COUNT_FROM_TABLE = r'''su - {hdfs_user} -c "beeline -u '{hive_url}' --silent=true --showHeader=false --outputformat=csv2 -e 'SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring; select count(*) from {table};' 2>Logs.txt " | tail -1'''  # noqa: 501

    PORTAL_GET_CONNECTION_COUNT = '''ssh {host} "ss -nap | grep ':{port}' | wc -l"'''  # noqa: 501

    GET_HOST_MEMORY_USAGE = '''ssh {host} "free -g | head -2"'''

    GET_COUNT_OF_OOM_COUNT = '''ssh {host} "grep '{date}' {filename} | grep '{pattern}' | wc -l" '''  # noqa: 501

    GET_SIZE_OF_DIR = ''' du -ks {path} | cut -d' ' -f1 '''

    HIVE_USAGE_TABLE_CONTENT = r'''su - {hdfs_user} -c "beeline -u '{hive_url}' --silent=true --showHeader=false --outputformat=csv2 -e \"SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring;set mapred.reduce.tasks=2; select hour(from_unixtime(floor(CAST(dt AS BIGINT)/1000), 'yyyy-MM-dd HH:mm:ss.SSS')) as created_timestamp,count(*)  from {usage_table} where dt>='{start_epoch}' and dt<='{end_epoch}' group by hour(from_unixtime(floor(CAST(dt AS BIGINT)/1000), 'yyyy-MM-dd HH:mm:ss.SSS'))  order by created_timestamp;\" 2>Logs.txt " '''  # noqa: 501

    ETL_INPUT_TIME = '''hdfs dfs -ls  {dir_path}/{file_path1}/ | grep "^d" | sort -k6,7 | grep -w {file_path2} | tail -1 | tr -s ' ' | cut -d' ' -f6,7'''  # noqa: E501

    ETL_OUTPUT_TIME = '''hdfs dfs -ls  {dir_path}/{file_path1}/{file_path2}/ | grep "^d" | sort -k6,7 | tail -1 | tr -s ' ' | cut -d' ' -f6,7'''  # noqa: E501

    ETL_INPUT_OUTPUT_TIME = '''hdfs dfs -ls  {dir_path}/{file_path}/ | grep "^d" | sort -k6,7 | tail -1 | tr -s ' ' | cut -d' ' -f6,7'''  # noqa: 501

    ETL_CONNECTOR_STATUS = '''ssh {host_name} ps -eaf | grep -i ETLServiceStarter | grep {topology}'''  # noqa: 501

    NODE_PING_STATUS = '''ping -c 1 {node} | grep "packet loss" '''  # noqa: 501

    PROCESS_CHECK = '''ps -eaf | grep "{process_name}" | grep -v grep'''  # noqa: 501

    GET_COUNT_FROM_HDFS = '''su - {hdfs_user} -c "/opt/nsn/ngdb/hadoop/bin/hdfs dfs -ls -R {regex}  2>> /dev/null | wc -l" '''  # noqa: 501

    JPS_CMD_FOR_PID = '''ssh {host} "jps | grep {service_name} | cut -d' ' -f1" '''  # noqa: 501

    PS_EAF_CMD_FOR_PID = '''ssh {host} "ps -eaf | grep '^{user}.*{service_name}'" '''  # noqa: 501

    SYSTEMCTL_CMD_FOR_PID = '''ssh {host} 'systemctl status {service_name} | grep "{pid_type}"' '''  # noqa: 501

    MEM_OF_JSTAT_SERVICE = '''ssh {host} "jstat -gc {pid}" '''

    MEM_OF_SERVICE = '''ssh {host} top -p {pid} -bn1 | grep "KiB Mem"'''

    CPU_IDLE_TIME = '''ssh {host} top -p {pid} -bn1 | grep "Cpu(s)" '''

    OPEN_FILES = '''ssh {host} "lsof -p {pid} | wc -l" '''

    GET_LAG_SUM = '''/usr/bin/kafka-consumer-groups -bootstrap-server {kafka_broker_host}:9092  --describe --group connect-{group_name}'''  # noqa: 501

    GET_CONNECTOR_STATUS = '''ssh {kafka_host} "curl -s http://{kafka_host}:{port}/connectors/{topology}/status"'''  # noqa: 501


class CommandExecutor:

    @staticmethod
    def get_output(command, **cmd_args):
        return CommandExecutor.get_status_output(
            command, **cmd_args)[1]

    @staticmethod
    def get_status_output(command, **cmd_args):
        formatted_command = command.value.format(**cmd_args)
        logger.info(f'Executing command : {formatted_command}')
        return subprocess.getstatusoutput(formatted_command)

    @staticmethod
    def is_process_to_be_executed(module_name, count):
        running_processes = CommandExecutor._get_running_processes(
            module_name)
        return not CommandExecutor._is_old_process_still_running(
            running_processes, count)

    @staticmethod
    def _get_running_processes(module_name):
        values_dict = {
            'process_name': module_name
        }
        process_detail = CommandExecutor.get_output(
            Command.PROCESS_CHECK, **values_dict)
        if process_detail:
            return process_detail.split('\n')
        return []

    @staticmethod
    def _is_old_process_still_running(processes, count):
        return len(processes) > count
