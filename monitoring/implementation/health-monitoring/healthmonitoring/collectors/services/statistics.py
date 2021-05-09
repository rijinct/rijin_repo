'''
Created on 17-Jun-2020

@author: praveenD
'''
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.collectors.utils.command import Command, CommandExecutor

from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def main():
    print(_execute())


def _execute():
    raw_data = Collector().collect()
    logger.debug("Raw data from collector is %s", raw_data)
    return Presenter(raw_data).present()


class Collector:

    SERVICES = SpecificationUtil.get_property('Statistics', 'Service',
                                              'Names').split(',')
    PORTAL_SERVICES = SpecificationUtil.get_property('Statistics',
                                                     'PortalService',
                                                     'Names').split(',')

    def __init__(self):
        self._service_details = Collector._get_service_details()
        self._statistics = []

    def collect(self):
        for service in Collector.SERVICES + Collector.PORTAL_SERVICES:
            hosts = [
                node.address for node in SpecificationUtil.get_hosts(
                    self._service_details[service]['hostName'])
            ]
            for host in hosts:
                self._statistics.append(self._get_service_stats(service, host))
        return self._statistics

    @staticmethod
    def _get_service_details():
        service_details = {}
        for service in Collector.SERVICES + Collector.PORTAL_SERVICES:
            service_details[service] = SpecificationUtil.get_property(
                'ServiceStability')[service]
        return service_details

    def _get_service_stats(self, service, host):
        pid = self._get_pid_of_service(service, host)
        if pid:
            return [
                service, host, 'Running',
                _Statistics(host, pid).get_cpu_used_percent(),
                _Statistics(host, pid).get_mem_of_service(),
                _Statistics(host, pid).get_open_file_count()
            ]
        else:
            return [service, host, 'DOWN', 'NA', 'NA', 'NA']

    def _get_pid_of_service(self, service, host):
        search_str = self._service_details[service]['searchString'].split(',')
        check_type = self._service_details[service]['checkType']
        pid_extractor = _ServicePidExtractor(service, host, search_str)
        if check_type == 'jps':
            return pid_extractor.get_pid_using_jps()
        elif check_type == 'ps -eaf':
            return pid_extractor.get_pid_using_ps()
        elif check_type == 'systemctl':
            return pid_extractor.get_pid_using_systemctl()


class _ServicePidExtractor:
    def __init__(self, service, host, search_str):
        self._service = service
        self._host = host
        self._search_str = search_str

    def get_pid_using_jps(self):
        args = {'host': self._host, 'service_name': self._search_str[0]}
        output = CommandExecutor.get_output(Command.JPS_CMD_FOR_PID, **args)
        try:
            return int(output.split()[0])
        except (ValueError, IndexError):
            logger.debug(
                f'{self._service} is down on {self._host}\n output: {output}')

    def get_pid_using_ps(self):
        args = {
            'host': self._host,
            'service_name': self._search_str[0],
            'user': self._search_str[1]
        }
        output = CommandExecutor.get_output(Command.PS_EAF_CMD_FOR_PID, **args)
        try:
            return int(output.split()[1])
        except (ValueError, IndexError):
            logger.debug(
                f'{self._service} is down on {self._host}\n output: {output}')

    def get_pid_using_systemctl(self):
        args = {
            'host': self._host,
            'service_name': self._search_str[0],
            'pid_type': self._search_str[1]
        }
        output = CommandExecutor.get_output(Command.SYSTEMCTL_CMD_FOR_PID,
                                            **args)
        try:
            return int(output.split()[2])
        except (ValueError, IndexError):
            logger.debug(
                f'{self._service} is down on {self._host}\n output: {output}')


class _Statistics:
    def __init__(self, host, pid):
        self._host = host
        self._pid = pid

    def get_cpu_used_percent(self):
        args = {'host': self._host, 'pid': self._pid}
        try:
            return round(
                100 - float(
                    CommandExecutor.get_output(Command.CPU_IDLE_TIME, **
                                               args).split(',')[3].split()[0]),
                2)
        except (ValueError, IndexError):
            return 'NA'

    def get_mem_of_service(self):
        args = {'host': self._host, 'pid': self._pid}
        output = CommandExecutor.get_output(Command.MEM_OF_JSTAT_SERVICE,
                                            **args)
        if output not in (f'{self._pid} not found', ''):
            memory_data = dict(
                zip(*[line.strip().split() for line in output.splitlines()]))
            return round(
                (float(memory_data['OU']) + float(memory_data['EU']) +
                 float(memory_data['S0U']) + float(memory_data['S1U'])) / 1000,
                2)
        else:
            return round(
                float(
                    CommandExecutor.get_output(Command.MEM_OF_SERVICE, **
                                               args).split()[3].split('+')[0])
                / 1024, 2)

    def get_open_file_count(self):
        args = {'host': self._host, 'pid': self._pid}
        try:
            return int(CommandExecutor.get_output(Command.OPEN_FILES, **args))
        except ValueError:
            return 'NA'


class Presenter:
    def __init__(self, raw_data):
        self._data = raw_data

    def present(self):
        output = []
        time = DateTimeUtil.get_current_hour_date()
        for list_data in self._data:
            service, host, status, cpu, mem, open_files = list_data
            output.append({
                'Time': time,
                'Service': service,
                'Host': host,
                'Status': status,
                'CPU_Use': cpu,
                'Total_MEM': mem,
                'Open_Files': open_files
            })
        return output


if __name__ == "__main__":
    main()
