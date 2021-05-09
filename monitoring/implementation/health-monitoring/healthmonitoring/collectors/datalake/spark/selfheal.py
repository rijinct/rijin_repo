'''
Created on 11-May-2020

@author: deerakum
'''
from pathlib import Path

from healthmonitoring.collectors import config
from logger import Logger

from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.collectors.utils.file_util import FileUtils
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.command import Command, CommandExecutor

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


def main():
    print(_execute())


def _execute():
    raw_data = Collector().collect()
    return Presenter(raw_data).present()


class Collector:

    def __init__(self):
        self._log_dir = Path("/var/log/")

    def collect(self):
        self._spark_oom = []
        spark_log_details = (("sparkthrift-flexi",
                              "spark-flexi"), ("sparkthrift", "spark-portal"),
                             ("sparkthriftapp", "spark-app"))
        for spark_log_dir, spark_type in spark_log_details:
            self._find_oom(Path(spark_log_dir), spark_type)
        return self._spark_oom

    def _find_oom(self, specific_log_dir, spark_type):
        spark_hosts = [
            host.address for host in SpecificationUtil.get_hosts(spark_type)
        ]
        latest_log_file = FileUtils.get_latest_file(self._log_dir /
                                                    specific_log_dir)
        for host in spark_hosts:
            arg_dict = {
                "date": DateTimeUtil.get_date_till_hour(),
                "filename": latest_log_file,
                "pattern": "OutOfMemory",
                "host": host
            }
            self._spark_oom.append(
                (host, spark_type,
                 CommandExecutor.get_output(Command.GET_COUNT_OF_OOM_COUNT,
                                            **arg_dict)))


class Presenter:

    def __init__(self, processed_data):
        self._processed_data = processed_data

    def present(self):
        output_list = []
        for spark_item in self._processed_data:
            temp_dict = {
                "host": spark_item[0],
                "spark-type": spark_item[1],
                "value": spark_item[2]
            }
            output_list.append(temp_dict)
        logger.debug("spark restart details are %s", output_list)
        return output_list


if __name__ == "__main__":
    main()
