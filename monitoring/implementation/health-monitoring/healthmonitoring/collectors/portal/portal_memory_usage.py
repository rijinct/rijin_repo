'''
Created on 21-Apr-2020

@author: a4yadav
'''
from datetime import datetime

from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.collectors.utils.collection import CollectionUtil
from healthmonitoring.collectors.utils.command import CommandExecutor, Command
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil

from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def main():
    hosts = SpecificationUtil.get_hosts("portal").fields['cemod_portal_hosts']
    if hosts:
        _execute(hosts)
    else:
        logger.info('Portal Nodes are empty, nothing to parse')


def _execute(hosts):
    raw_data = Collector(hosts).collect()
    processed_data = Processor(raw_data).process()
    print(Presenter(processed_data).present())


class Collector:

    def __init__(self, hosts):
        self._hosts = hosts

    def collect(self):
        raw_data = []
        for host in self._hosts:
            logger.info('Getting values for host: %s' % host)
            value_dict = {'host': host}
            mem = CommandExecutor.get_output(Command.GET_HOST_MEMORY_USAGE,
                                             **value_dict)
            raw_data.append((host, mem))
        return self._raw_data


class Processor:

    def __init__(self, raw_data):
        self._raw_data = raw_data

    def process(self):
        processed_data = []
        for host, memory in self._raw_data:
            memory = self._get_memory_as_dict(memory)
            processed_data.append({
                'host': host,
                'total': memory['total'],
                'free': memory['free'],
                'cache': memory['buff/cache']
            })
        return processed_data

    def _get_memory_as_dict(self, output):
        keys, values = output.split('\n')
        keys = CollectionUtil.get_list_without_null_values(keys)
        values = CollectionUtil.get_list_without_null_values(values)[1:]
        values = CollectionUtil.map_items_to_int(values)
        return CollectionUtil.frame_dict_using_lists(keys, values)


class Presenter:

    def __init__(self, processed_data):
        self._processed_data = processed_data

    def present(self):
        presented_data = []
        for data in self._processed_data:
            host_dict = dict(Date=DateTimeUtil.to_date_string(
                datetime.now(), DateTimeUtil.DATE_TRUNC_BY_HOUR_FORMAT),
                             Host=data['host'],
                             TotalMemory=data['total'],
                             FreeMemory=data['free'],
                             FreeBufferCache=data['cache'])
            presented_data.append(host_dict)
        return presented_data


if __name__ == '__main__':
    main()
