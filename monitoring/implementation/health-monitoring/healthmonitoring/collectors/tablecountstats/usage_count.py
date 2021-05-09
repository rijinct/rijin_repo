'''
Created on 25-May-2020

@author: praveenD
'''
from collections import defaultdict

from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def main():
    print(_execute())


def _execute():
    raw_data = Collector().collect()
    logger.debug("Raw data from collector is %s", raw_data)
    processed_data = Processor(raw_data).process()
    return Presenter(processed_data).present()


class Collector():

    def __init__(self):
        self._properties = UsageTablesProperties()

    def collect(self):
        return self._get_table_content(
            self._properties.mobile_usage_tables), self._get_table_content(
                self._properties.fixed_line_usage_tables)

    def _get_table_content(self, usage_tables):
        content = []
        args = {
            'hdfs_user': self._properties.hdfs_user,
            'hive_url': self._properties.hive_url,
            'start_epoch': self._properties.start_epoch,
            'end_epoch': self._properties.end_epoch
        }
        for table in usage_tables:
            args['usage_table'] = table
            content.append(
                (table,
                 CommandExecutor.get_output(Command.HIVE_USAGE_TABLE_CONTENT,
                                            **args)))

        return content


class UsageTablesProperties:

    def __init__(self):
        self._hdfs_user = SpecificationUtil.get_field("hdfs",
                                                      "cemod_hdfs_user")
        self._hive_url = SpecificationUtil.get_field("hive", "cemod_hive_url")
        self._start_epoch, self._end_epoch = DateTimeUtil.get_partition_info(
            'DAY')[1:]
        self._mobile_usage_tables, self._fixed_line_usage_tables = \
            self._get_usage_tables()

    @property
    def hdfs_user(self):
        return self._hdfs_user

    @property
    def hive_url(self):
        return self._hive_url

    @property
    def start_epoch(self):
        return self._start_epoch

    @property
    def end_epoch(self):
        return self._end_epoch

    @property
    def mobile_usage_tables(self):
        return self._mobile_usage_tables

    @property
    def fixed_line_usage_tables(self):
        return self._fixed_line_usage_tables

    def _get_usage_tables(self):
        data = SpecificationUtil.get_property(('USAGETABLE'))
        usage_tables = defaultdict(list)
        for item in data.items():
            if item[1]['value']:
                usage_tables[item[1]['type']].append(item[0])
        return usage_tables['mobile'], usage_tables['fixed_line']


class Processor:

    def __init__(self, raw_data):
        self._mobile_usage_tables_data, \
            self._fixed_line_usage_tables_data = raw_data

    def process(self):
        mobile_usage_tables_data = self._get_table_values(
            self._mobile_usage_tables_data)
        fixed_line_usage_tables_data = self._get_table_values(
            self._fixed_line_usage_tables_data)
        mobile_usage_tables_data[-1][0] = 'sum_of_mobile_usage_row_values'
        fixed_line_usage_tables_data[-1][
            0] = 'sum_of_fixed_line_usage_row_values'
        return mobile_usage_tables_data, fixed_line_usage_tables_data

    def _get_table_values(self, tables_data):
        processed_data = []
        temp_data = []
        for table, content in tables_data:
            self.format_data(content, processed_data, table, temp_data)
        processed_data.append(
            ['sum_of_row_values',
             list(map(sum, zip(*temp_data)))])
        return processed_data

    def format_data(self, content, processed_data, table, temp_data):
        hours_in_content = set()
        list_data = []
        try:
            for data in content.split('\n'):
                if 'Java HotSpot' not in data:
                    hours_in_content.add(data.split(',')[0])
                    list_data.append(
                        [data.split(',')[0],
                         int(data.split(',')[1])])
        except IndexError:
            logger.debug('data is not available for {}'.format(table))
        hours = set([str(i) for i in range(24)])
        hours_not_in_content = hours - hours_in_content
        for hour in hours_not_in_content:
            list_data.append([hour, 0])
        self.write_sum_of_values(list_data, processed_data, table, temp_data)

    def write_sum_of_values(self, list_data, processed_data, table, temp_data):
        sum_of_hours_values = sum([int(item[1]) for item in list_data])
        list_data.append(['24', sum_of_hours_values])
        list_data.sort(key=lambda item: int(item[0]))
        processed_data.append([table, list_data])
        temp_data.append([int(item[1]) for item in list_data])


class Presenter:

    def __init__(self, data):
        self._mobile_usage_data, self._fixed_line_usage_data = data

    def present(self):
        output_list = []
        data_list = self.get_combined_data()
        temp_data = list(zip(*data_list))
        headers_list = temp_data.pop(0)
        output = list(zip(*temp_data[0]))
        return self.write_to_output_list(headers_list, output, output_list)

    def get_combined_data(self):
        for data in [self._mobile_usage_data, self._fixed_line_usage_data]:
            for item in data[:-1]:
                item[1] = list(zip(*item[1]))
        data_list = []
        for data in self._mobile_usage_data[:-1] + \
                self._fixed_line_usage_data[:-1]:
            data_list.append([data[0], data[1][1]])
        data_list = data_list + [
            self._mobile_usage_data[-1], self._fixed_line_usage_data[-1]
        ]
        return data_list

    def write_to_output_list(self, headers_list, output, output_list):
        hour = 0
        for data in output:
            temp_dict = {}
            temp_dict['Date'] = DateTimeUtil.get_current_hour_date()
            temp_dict['Hour'] = hour
            temp_dict.update(dict(list(zip(headers_list, data))))
            output_list.append(temp_dict)
            hour += 1
        logger.debug("Usage table Counts are {}".format(output_list))
        return output_list


if __name__ == '__main__':
    main()
