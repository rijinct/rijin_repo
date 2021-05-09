'''
Created on 27-Apr-2020

@author: khv
'''
import sys
import itertools
from xml.dom import minidom

from healthmonitoring.collectors.utils.queries import Queries, QueryReplacer
from healthmonitoring.collectors.utils.connection import ConnectionFactory
from healthmonitoring.framework.specification.defs import DBType
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def main():
    _execute()


def _execute():
    raw_data = Collector().collect()
    processed_data = Processor(raw_data).process()
    print(Presenter(processed_data).present())


class Collector:

    def __init__(self):
        self._input_arg = self._get_input_arg()

    def collect(self):
        job_names = self._get_job_names()
        trigger_names = self._get_trigger_names()
        return [job_names, trigger_names]

    def _get_input_arg(self):
        try:
            if sys.argv[1] in ('Usage', 'Perf'):
                return sys.argv[1]
            else:
                raise IndexError
        except IndexError:
            logger.info('Usage: python %s Usage|Perf' % sys.argv[0])
            sys.exit(1)

    def _get_job_names(self):
        xmldoc = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        elements = xmldoc.getElementsByTagName(
            'MANDATORYJOBSCHEDULING')[0].getElementsByTagName('property')
        return [
            val.attributes['value'].value.split(',') for val in elements
            if val.attributes['Name'].value == self._input_arg
        ][0]

    def _get_trigger_names(self):
        arg_dict = {"input_arg": self._input_arg}
        replaced_sql = QueryReplacer.get_replaced_sql(
            Queries.DISTINCT_JOB_NAMES, **arg_dict)
        connection = ConnectionFactory.get_connection(DBType.POSTGRES_SDK)
        return connection.fetch_records(replaced_sql)


class Processor:

    def __init__(self, raw_data):
        self._job_names, self._trigger_names = raw_data

    def process(self):
        trigger_names_list = list(itertools.chain(*self._trigger_names))
        return list(set(self._job_names) - set(trigger_names_list))


class Presenter:

    def __init__(self, data):
        self._data = data

    def present(self):
        output_list = []
        for job in self._data:
            temp_dict = {"JobName": job}
            output_list.append(temp_dict)
        return output_list


if __name__ == '__main__':
    main()
