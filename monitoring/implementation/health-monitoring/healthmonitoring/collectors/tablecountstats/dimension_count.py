'''
Created on 22-Apr-2020

@author: deerakum
'''
import re

from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def main():
    print(_execute())


def _execute():
    hdfs_user = SpecificationUtil.get_field("hdfs", "cemod_hdfs_user")
    hive_url = SpecificationUtil.get_field("hive", "cemod_hive_url")
    raw_data = Collector(hdfs_user, hive_url).collect()
    logger.debug("Raw data from collector is %s", raw_data)
    return Presenter(raw_data).present()


class Collector:

    def __init__(self, hdfs_user, hive_url):
        self._table_prop_dict = {"hdfs_user": hdfs_user, "hive_url": hive_url}

    def collect(self):
        tables = self._get_dimension_tables()
        return self._get_count_of_records(tables)

    def _get_dimension_tables(self):
        self._table_prop_dict["table"] = "es*"
        output = CommandExecutor.get_output(Command.HIVE_SHOW_TABLES,
                                            **self._table_prop_dict)
        return [table for table in output if re.match(r"^es", table)]

    def _get_count_of_records(self, tables_list):
        output_list = []
        for table in tables_list:
            self._table_prop_dict["table"] = table
            output = CommandExecutor.get_output(
                Command.HIVE_GET_RECORD_COUNT_FROM_TABLE,
                **self._table_prop_dict)
            output_list.append((table, output))
        return output_list


class Presenter:

    def __init__(self, processed_data):
        self._processed_data = dict(processed_data)

    def present(self):
        output_list = []
        for table, count in self._processed_data.items():
            temp_dict = {
                "Date": DateTimeUtil.get_current_hour_date(),
                "Dimension Table": table,
                "Count": count
            }
            output_list.append(temp_dict)
        logger.debug("Dimension Counts are %s", output_list)
        return output_list


if __name__ == "__main__":
    main()
