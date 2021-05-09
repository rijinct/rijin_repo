'''
Created on 05-Jun-2020

@author: praveenD
'''
from healthmonitoring.collectors.utils.queries import Queries, QueryExecutor
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors.utils.enum_util import ETLTopologyStatus
from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.framework.specification.defs import DBType

from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def main():
    print(_execute())


def _execute():
    raw_data = Collector().collect()
    return Presenter(raw_data).present()


class Collector:

    SOURCE, SINK = 'source', 'sink'

    def __init__(self):
        self._topologies_data = Collector.get_topologies_data()
        self._status = []

    def collect(self):
        return self._get_topologies_status()

    @staticmethod
    def get_topologies_data():
        return QueryExecutor.execute(DBType.POSTGRES_SDK,
                                     Queries.TOPOLOGIES_DIR_INFO)

    def _get_topologies_status(self):
        for data in self._topologies_data:
            topology_status = []
            self._write_input_output_delays(data, topology_status)
            self._write_status_on_nodes(topology_status, data)
            self._write_partition_level_and_usage_stats(topology_status, data)
            topology_status.append(('Topology', data[0]))
            topology_status.append(
                ('Time', DateTimeUtil.get_current_hour_date()))
            self._status.append(topology_status)
        return self._status

    def _write_input_output_delays(self, data, topology_status):
        DELIMITER = '/'
        self._write_delay_time(
            _DelayTime.get_delay_time(data[1].split(DELIMITER)[0],
                                      data[1].split(DELIMITER)[1],
                                      '/mnt/staging/import'),
            'input_delay_time', topology_status)
        self._write_delay_time(
            _DelayTime.get_delay_time(data[2].split(DELIMITER)[0],
                                      data[2].split(DELIMITER)[1],
                                      '/ngdb/us/import'), 'output_delay_time',
            topology_status)

    def _write_delay_time(self, output_and_delay_time, delay_type,
                          topology_status):
        DELIMITER = '_'
        delay_time = output_and_delay_time[1]
        if delay_time is not None:
            time = delay_time
        else:
            time = ETLTopologyStatus.NO_DELAY_TIME.value
        topology_status.append((delay_type, time))
        topology_status.append((DELIMITER.join(
            delay_type.split(DELIMITER)[0::2]), output_and_delay_time[0]))

    def _write_status_on_nodes(self, topology_status, data):
        nodes_down = 0
        nodes = [
            node.address for node in SpecificationUtil.get_hosts('kafka-hosts')
        ]
        for node in nodes:
            ping_status = CommandExecutor.get_output(Command.NODE_PING_STATUS,
                                                     **{'node': node})
            if '0% packet loss' in ping_status:
                self._fetch_and_write_status(node, data[0], topology_status)
            else:
                nodes_down += 1
                topology_status.append(
                    (node, ETLTopologyStatus.SOURCE_AND_SINK_DOWN.value))
        if nodes_down == len(SpecificationUtil.get_hosts('kafka-hosts')):
            topology_status.append(
                ('delay_time', ETLTopologyStatus.ALL_NODES_DOWN.value))

    def _fetch_and_write_status(self, node, data, topology_status):
        pid_list = _ConnectorStatus.get_connector_status(data, node)
        self._fetch_status_on_nodes(node, topology_status, pid_list)

    def _fetch_status_on_nodes(self, node, topology_status, pid_list):
        if pid_list:
            connectors = [
                item[1] for item in pid_list if int(item[0]) and len(item) > 1
            ]
            self._write_connector_status(connectors, node, topology_status)
        else:
            topology_status.append(
                (node, ETLTopologyStatus.SOURCE_AND_SINK_DOWN.value))

    def _write_connector_status(self, connectors, node, topology_status):
        if self.SOURCE in connectors and self.SINK in connectors:
            topology_status.append(
                (node, ETLTopologyStatus.SOURCE_AND_SINK_UP.value))
        elif self.SOURCE in connectors or self.SINK in connectors:
            for connector in connectors:
                self._write_status_up(connector, node, topology_status)
        else:
            topology_status.append(
                (node, ETLTopologyStatus.SOURCE_AND_SINK_DOWN.value))

    def _write_status_up(self, connector, node, topology_status):
        if connector == self.SOURCE:
            status = ETLTopologyStatus.SOURCE_UP.value
        else:
            status = ETLTopologyStatus.SINK_UP.value
        topology_status.append((node, status))

    def _write_partition_level_and_usage_stats(self, topology_status, data):
        topology_status.extend([('usage_job', data[3]), ('partition', data[4]),
                                ('usage_boundary', str(data[5])),
                                ('usage_delay',
                                 Collector.get_usage_delay(data[5]))])

    @staticmethod
    def get_usage_delay(boundary):
        if boundary:
            return '%.f' % (
                (DateTimeUtil.now() - boundary).total_seconds() / 60)
        else:
            return str(ETLTopologyStatus.DELAY_WHEN_BOUNDARY_NULL.value)


class _DelayTime:

    @staticmethod
    def get_delay_time(file_path1, file_path2, dir_path):
        args = {
            'dir_path': dir_path,
            'file_path1': file_path1,
            'file_path2': file_path2
        }
        if dir_path == '/mnt/staging/import':
            cmd_str = Command.ETL_INPUT_TIME
        else:
            cmd_str = Command.ETL_OUTPUT_TIME

        output_time = CommandExecutor.get_output(cmd_str, **args)
        return _DelayTime.get_delay_in_min(dir_path, file_path1, output_time)

    @staticmethod
    def get_delay_in_min(dir_path, file_path, output_time):
        if not output_time:
            output_time = _DelayTime.get_output_time(dir_path, file_path)
        return output_time, _DelayTime.calculate_delay(output_time)

    @staticmethod
    def get_output_time(dir_path, file_path):
        return CommandExecutor.get_output(
            Command.ETL_INPUT_OUTPUT_TIME, **{
                'dir_path': dir_path,
                'input1': file_path
            })

    @staticmethod
    def calculate_delay(output_time):
        try:
            delay_time = DateTimeUtil.now() - DateTimeUtil.parse_date(
                output_time, '%Y-%m-%d %H:%M')
            if delay_time.days:
                return int(
                    round(delay_time.days * (24 * 60) +
                          (delay_time.seconds / 60)))
            else:
                return int(round(delay_time.seconds / 60))
        except ValueError as error:
            logger.error(error)


class _ConnectorStatus:

    @staticmethod
    def get_connector_status(toplology, hostname):
        status_output = CommandExecutor.get_output(
            Command.ETL_CONNECTOR_STATUS, **{
                'host_name': hostname,
                'topology': toplology
            })
        if status_output:
            return _ConnectorStatus.get_pid(status_output)

    @staticmethod
    def get_pid(output):
        status_info = list(
            filter(lambda x: 'nohup' not in x,
                   output.strip().splitlines()))
        return _ConnectorStatus.fetch_pid_from_status_info(status_info)

    @staticmethod
    def fetch_pid_from_status_info(status_info):
        pid_list = []
        try:
            for data in status_info:
                data = data.split()
                _ConnectorStatus.write_pid(data, pid_list)
        except Exception as e:
            logger.error(e)
        return pid_list

    @staticmethod
    def write_pid(data, pid_list):
        for index, item in enumerate(data):
            if 'connectortype' in item:
                pid_list.append((data[1], data[index + 1]))


class Presenter:

    def __init__(self, raw_data):
        self._data = raw_data

    def present(self):
        output = []
        for data in self._data:
            dict_data = dict(data)
            output.append(dict_data)
        return output


if __name__ == '__main__':
    main()
