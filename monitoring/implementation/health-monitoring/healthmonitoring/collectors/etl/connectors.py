from collections import defaultdict
import json
import re

import argparse

from healthmonitoring.collectors import _LocalLogger
from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.collectors.utils.command import \
    Command, CommandExecutor
from healthmonitoring.collectors.utils.string import StringUtils
from healthmonitoring.collectors.utils.enum_util import CheckConnectors
from healthmonitoring.collectors.utils.application_config import \
    ApplicationConfig
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil

logger = _LocalLogger.get_logger(__name__)


def main():
    print(_execute())


def _execute():
    args = _process_args()
    if args.type == 'status':
        return _execute_connector_status_fetch()
    elif args.type == 'lag':
        return _execute_lag_count()


def _process_args():
    p = argparse.ArgumentParser(
        description="""Usage: Check status/lag count for connectors""")
    p.add_argument("-t", "--type", help="status/lag")
    return p.parse_args()


def _execute_connector_status_fetch():
    raw_data = ConnectorStatusCollector().collect()
    processed_output = ConnectorStatusProcessor(raw_data).process()
    return ConnectorStatusPresenter(processed_output).present()


def _execute_lag_count():
    raw_data = LagCountCollector().collect()
    processed_output = LagCountProcessor(raw_data).process()
    return LagCountPresenter(processed_output).present()


class LagCountCollector:

    _CLOUDERA_LAG_COL_NUM = 4

    _COMMON_LAG_COL_NUM = 5

    def collect(self):
        lag_count_list = []
        configured_topologies = SpecificationUtil.get_property('TOPOLOGIES')
        logger.info('Number of topologies configured are : %s',
                    len(configured_topologies))
        for topology_name in configured_topologies:
            group_name = f'{topology_name}_SINK_CONN'
            lag_count_list.append(
                LagCountCollector._get_lag_count(group_name))
        return lag_count_list

    @staticmethod
    def _get_lag_count(group_name):
        logger.info('Getting lag_count for %s', group_name)
        value_dict = {
            "kafka_broker_host": SpecificationUtil.get_host(
                'kakfa_broker').address,
            "group_name": group_name
        }
        lag_sum = LagCountCollector._get_lag_sum(CommandExecutor.get_output(
            Command.GET_LAG_SUM, **value_dict))
        return (lag_sum, group_name)

    @staticmethod
    def _get_lag_sum(lag_detail):
        lag_sum = 0
        lag_index = LagCountCollector._get_index_for_lag()
        if not lag_detail or 'Error:' in lag_detail:
            return -1
        for line in re.sub(r"\s\s+", " ", lag_detail).split('\n'):
            if line:
                lag_value = StringUtils.get_word(
                    line, index=lag_index)
                if lag_value.isnumeric():
                    lag_sum += int(lag_value)
        return lag_sum

    @staticmethod
    def _get_index_for_lag():
        if SpecificationUtil.get_field(
                "platform_definition",
                "cemod_platform_distribution_type") == "cloudera":
            return LagCountCollector._CLOUDERA_LAG_COL_NUM
        return LagCountCollector._COMMON_LAG_COL_NUM


class ConnectorStatusCollector:

    def collect(self):
        connector_status = []
        configured_topologies = SpecificationUtil.get_property(
            'TOPOLOGIES')
        for topology_name in configured_topologies:
            connector_task_status = \
                ConnectorStatusCollector.\
                _fetch_connectors_tasks_status(topology_name)
            if connector_task_status:
                connector_status.append(connector_task_status)
        return connector_status

    @staticmethod
    def _fetch_connectors_tasks_status(topology_name):
        try:
            return ConnectorStatusCollector.\
                    _get_connector_type_and_status(
                        topology_name)
        except AttributeError:
            logger.exception(
                "Could not find connector status for %s",
                topology_name)

    @staticmethod
    def _get_connector_type_and_status(topology_name):
        source_and_sink_conn_list = ConnectorStatusCollector.\
            _get_source_sink_list(topology_name)
        connector_status = []
        kafka_host = SpecificationUtil.get_host(
            "kakfa_broker").address
        for item in source_and_sink_conn_list:
            value_dict = {
                "kafka_host": kafka_host,
                "port": item[1],
                "topology": item[0]
                }
            status = CommandExecutor.get_status_output(
                Command.GET_CONNECTOR_STATUS, **value_dict)
            connector_status.append((item[0], status))
        return connector_status

    @staticmethod
    def _get_source_sink_list(topology_name):
        ports = ApplicationConfig().get_kafka_adap_port(topology_name)
        return [
            (f'{topology_name}_SOURCE_CONN',
             StringUtils.get_word(ports, ",")),
            (f'{topology_name}_SINK_CONN',
             StringUtils.get_word(ports, ",", 1))
        ]


class LagCountProcessor:

    def __init__(self, data):
        self._data = data

    def process(self):
        processed_output = []
        for lag_count_info in self._data:
            lag_sum, group_name = lag_count_info
            message_count = -1
            records = 0
            if lag_sum > -1:
                message_count = lag_sum
                records = lag_sum * 1000
            processed_output.append(
                [group_name, message_count, records])
        return processed_output


class ConnectorStatusProcessor:

    def __init__(self, data):
        self._data = data

    def process(self):
        processed_output = []
        for connector_status_list in self._data:
            self._update_connector_task_status(
                connector_status_list, processed_output)
        return processed_output

    def _update_connector_task_status(
            self, connector_status_list, processed_output):
        for connector_status in connector_status_list:
            status, json_data = connector_status[1]
            if status:
                processed_output.append(
                    self._get_connector_tasks_status_down(
                        connector_status))
            else:
                processed_output.append(
                    self._get_connector_task_status(
                        connector_status, json_data))

    def _get_connector_task_status(self, connector_down_status, status):
        connector_up_status = json.loads(status)
        if connector_up_status.get('error_code'):
            return self.\
                _get_connector_tasks_status_down(connector_down_status)
        return self.\
            _get_connector_tasks_status_up(connector_up_status)

    def _get_connector_tasks_status_down(self, data):
        connector_type = data[0]
        connector_state = CheckConnectors.RUNNING_STATUS_NOK.value
        tasks_status = CheckConnectors.RUNNING_STATUS_NOK.value
        active_tasks = 0
        connector_running_on_node = ''
        tasks_running_on_nodes = ''
        total_tasks = 0
        return [connector_type, connector_state, tasks_status,
                active_tasks, connector_running_on_node,
                tasks_running_on_nodes, total_tasks]

    def _get_connector_tasks_status_up(self, status_data):
        active_tasks, total_tasks, tasks_status = \
            self._get_task_info(status_data)
        running_tasks = self._get_running_tasks(
            status_data['tasks'])
        connector_state = self._get_connector_state(
            status_data['connector']['state'])
        connector_type = status_data['name']
        connector_running_on_node = \
            status_data['connector']['worker_id']
        return [connector_type, connector_state, tasks_status,
                active_tasks, connector_running_on_node,
                running_tasks, total_tasks]

    def _get_running_tasks(self, tasks):
        tasks_running_on_nodes = []
        for worker_id in list(
                self._get_tasks_worker_id(tasks).items()):
            tasks_running_on_nodes.append(
                '{}-{}'.format(worker_id[0], worker_id[1]))
        return ' '.join(tasks_running_on_nodes)

    def _get_connector_state(self, state):
        connector_state = CheckConnectors.RUNNING_STATUS_NOK.value
        if state == 'RUNNING':
            connector_state = CheckConnectors.RUNNING_STATUS_OK.value
        return connector_state

    def _get_tasks_worker_id(self, tasks_data):
        worker_id = defaultdict(int)
        for task in tasks_data:
            worker_id[task['worker_id']] += 1
        return worker_id

    def _get_task_info(self, status_data):
        if len(status_data['tasks']) == 0:
            active_tasks, tasks_status = (
                0, CheckConnectors.RUNNING_STATUS_NOK.value)
            return active_tasks, len(status_data['tasks']), tasks_status
        active_tasks, tasks_status = \
            self._get_task_status(status_data['tasks'])
        return active_tasks, len(status_data['tasks']), tasks_status

    def _get_task_status(self, tasks_data):
        active_tasks = defaultdict(int)
        RUNNING_STATE = 'RUNNING'
        for data in tasks_data:
            if list(data.items())[0][1] == RUNNING_STATE:
                active_tasks[RUNNING_STATE] += 1
        if active_tasks[RUNNING_STATE] == 0:
            return active_tasks[RUNNING_STATE],\
                CheckConnectors.RUNNING_STATUS_NOK.value
        elif active_tasks[RUNNING_STATE] < len(tasks_data):
            return active_tasks[RUNNING_STATE],\
                CheckConnectors.RUNNING_STATUS_PARTIAL_OK.value
        return active_tasks[RUNNING_STATE], \
            CheckConnectors.RUNNING_STATUS_OK.value


class LagCountPresenter:

    def __init__(self, processed_output):
        self._processed_data = processed_output

    def present(self):
        current_datetime = DateTimeUtil.get_current_hour_date()
        output = []
        for lag_count_info in self._processed_data:
            group, message_count, records = lag_count_info
            output.append({
                "Time": current_datetime,
                "Connector": group,
                "LagCount(Messages)": str(message_count),
                "LagCount(Records)": str(records)
            })
        return output


class ConnectorStatusPresenter:

    def __init__(self, processed_output):
        self._processed_data = processed_output

    def present(self):
        current_datetime = DateTimeUtil.get_current_hour_date()
        output = []
        for data in self._processed_data:
            connector_type, connector_status, task_status,\
                active_tasks, connector_node_info,\
                tasks_node_info, total_tasks = data
            output.append({
                'Time': current_datetime,
                'Connector_Type': connector_type,
                'Connector_Status': str(connector_status),
                'Tasks_Status': str(task_status),
                'Active_Tasks': str(active_tasks),
                'Connector_Running_On_Node': connector_node_info,
                'Tasks_Running_on_Nodes': tasks_node_info,
                'Total_Tasks': str(total_tasks)
            })
        return output


if __name__ == "__main__":
    main()
