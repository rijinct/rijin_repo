import sys
import os
import datetime
import commands
from concurrent import futures

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from propertyFileUtil import PropertyFileUtil
from loggerUtil import loggerUtil
from postgres_connection import PostgresConnection

current_time = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name,
                                               currentDate=current_time)
LOGGER = loggerUtil.__call__().get_logger(log_file)

INSTANCE_FOR_HIVE_QUERIES = PostgresConnection('hive_metastore')


def get_zero_size_partitions(table):
    query = get_query('zeroSizePartitionsquery', 'hiveSqlSection'). \
        replace('table_name', table)
    LOGGER.info('getting all zero size partitions of table {}'.format(table))
    return INSTANCE_FOR_HIVE_QUERIES.fetch_records(query)


def clean_up_partitions_from_hive_metastore_and_hdfs_dir(table):
    UTIL_SECTION = 'hiveSqlSection'
    util_names = 'deletePartition_Params,deletePartition_Key_Vals,' \
                 'deletePartitions'
    query_to_get_hdfs_dir = get_query('getHiveTableDir', 'hiveSqlSection'). \
        replace('table_name', table)
    try:
        dir_output = INSTANCE_FOR_HIVE_QUERIES.fetch_records(
            query_to_get_hdfs_dir)[0][0]
        LOGGER.info(get_zero_size_partitions(table))
        fetch_all_zero_size_partitions_and_delete(UTIL_SECTION, dir_output,
                                                  table, util_names)
    except Exception as e:
        LOGGER.info(e)


def fetch_all_zero_size_partitions_and_delete(UTIL_SECTION, dir_output,
                                              table, util_names):
    for partition in get_zero_size_partitions(table):
        for util_name in util_names.split(','):
            clean_up_partition(UTIL_SECTION, partition,
                               table, util_name)
        clean_up_partition_from_hdfs_dir(dir_output, partition[3])


def clean_up_partition(UTIL_SECTION, partition, table, util_name):
    query = get_query(util_name, UTIL_SECTION). \
        replace('table_name', table).replace('partition', partition[3])
    INSTANCE_FOR_HIVE_QUERIES.execute_query(query)
    LOGGER.info('partition {0}, {1}:{2} is deleted from {3}'.
                format(partition[3], partition[-2],
                       partition[-1], util_name.split('delete')[1]))


def get_query(util_name, util_section):
    return PropertyFileUtil(util_name, util_section).getValueForKey()


def clean_up_partition_from_hdfs_dir(dir_output, partition):
    command_to_delete_partition = 'hdfs dfs -rm -r "{0}/{1}" '. \
        format(dir_output, partition)
    output = commands.getoutput(command_to_delete_partition)
    LOGGER.info(output)


def get_tables_list():
    query_to_get_tables = get_query('getTables', 'hiveSqlSection')
    return INSTANCE_FOR_HIVE_QUERIES.fetch_records(query_to_get_tables)


def main():
    with futures.ThreadPoolExecutor() as executor:
        executor.map(clean_up_partitions_from_hive_metastore_and_hdfs_dir,
                     list(map(lambda table: table[0], get_tables_list())))

    INSTANCE_FOR_HIVE_QUERIES.close_connection()


if __name__ == '__main__':
    main()
