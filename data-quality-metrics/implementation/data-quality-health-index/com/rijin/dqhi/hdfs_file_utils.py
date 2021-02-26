import os
from subprocess import Popen, PIPE

import common_utils
import constants


LOGGER = common_utils.get_logger()


def read_data(hiveTablePath, lb, ub):
    if constants.ENABLE_LOCAL_EXECUTION is True:
        data_files_path = [os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test/test-data/dt=1486316100000/tz=Default/000000_0.csv"))]
        return data_files_path

    locs = []
    if hiveTablePath is not None:
        hdfsCommand = 'hdfs dfs -ls ' + hiveTablePath + ' | grep -v Found | cut -d "=" -f 2';
        process = Popen(hdfsCommand, shell=True, stdout=PIPE, stderr=PIPE)
        std_out, std_err = process.communicate()
        if "b''" not in str(std_out) and "/ngdb/es/" in hiveTablePath:
            locs.append(hiveTablePath)
        else:
            for part in std_out.splitlines():
                if represents_int(part) and int(part) >= lb and int(part) < ub:
                    part_info = hiveTablePath + '/dt=' + str(int(part)) + '/tz=Default/'
                    locs.append(part_info)
        LOGGER.debug("Records present in Hive Table Path : {} is : {} ".format(hiveTablePath, locs))
    else:
        LOGGER.info("Hive Table path is empty !!")
    return locs


def read_process_monitor_data(dir1, lb, ub):
    LOGGER.debug("Executing read_process_monitor_data")
    hdfsCommand = 'hdfs dfs -ls ' + dir1 + ' | grep -v Found | cut -d "_" -f 3 | tr -d ".dqm"' ;
    LOGGER.debug("Executing command: {}".format(hdfsCommand))
    process = Popen(hdfsCommand, shell=True, stdout=PIPE, stderr=PIPE)
    std_out, std_err = process.communicate()
    LOGGER.debug("std_out: {}".format(std_out))
    locs = []
    for part in std_out.splitlines():
        if represents_int(part) and int(part) >= lb and int(part) < ub:
            part_info = dir1 + 'pm_' + str(int(part)) + '.dqm'
            locs.append(part_info)
    LOGGER.debug("Process Monitor Locations{} ".format(locs))
    return locs


def represents_int(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
    
def removeDirectory():
    hdfsCommand = "hdfs dfs -rmr dqhi"
    process = Popen(hdfsCommand, shell=True, stdout=PIPE, stderr=PIPE)
    std_out, std_err = process.communicate()

if __name__ == '__main__':
    read_data('hdfs://projectcluster/ngdb/es/FL_DIMENSION_1/FL_THROUGHPUT_BIN_1', '', '')
