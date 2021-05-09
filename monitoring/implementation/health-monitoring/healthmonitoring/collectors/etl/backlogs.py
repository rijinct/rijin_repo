import inspect
from pathlib import PurePath

from healthmonitoring.collectors import _LocalLogger
from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil

logger = _LocalLogger.get_logger(__name__)

VALID_NUMBER_OF_PROCESSES_TO_RUN = 3


def main():
    print(_execute())


def _execute():
    output = []
    if CommandExecutor.is_process_to_be_executed(
            inspect.getmodulename(PurePath(__file__).name),
            VALID_NUMBER_OF_PROCESSES_TO_RUN):
        raw_data = Collector(
            SpecificationUtil.get_property('BACKLOG')).\
                collect()
        processed_output = Processor(raw_data).process()
        output = Presenter(processed_output).present()
    return output


class Collector:

    def __init__(self, data):
        self._data = data
        self._hdfs_user = SpecificationUtil().get_field(
            'hdfs', 'cemod_hdfs_user')

    def collect(self):
        output = []
        for adaptation_name in self._data.keys():
            output.append(self._get_adaptation_info(
                adaptation_name))
        return output

    def _get_adaptation_info(
            self, adaptation_name):
        import_directory_from_conf = \
            self._data.get(adaptation_name).get('Import')
        ngdb_directory_from_conf = \
            self._data.get(adaptation_name).get('Ngdb')
        mnt_import_count = self._get_mnt_count(
            import_directory_from_conf, '*dat')
        mntImportErrorCount = self._get_mnt_count(
            import_directory_from_conf, '*error')
        mntProcessingCount = self._get_mnt_count(
            import_directory_from_conf, '*processing')
        ngdbUsCount = self._get_ngdb_us_count(
            ngdb_directory_from_conf)
        ngdbUsWorkCount = self._get_ngdb_us_work_count(
            ngdb_directory_from_conf)
        return [adaptation_name, mnt_import_count,
                mntImportErrorCount, mntProcessingCount,
                ngdbUsCount, ngdbUsWorkCount]

    def _get_mnt_count(self, import_directory_from_conf, search_pattern):
        return self._get_hdfs_count(
            f'/mnt/staging/import/{import_directory_from_conf}/{search_pattern}'  # noqa: 501
            )

    def _get_ngdb_us_count(self, ngdb_directory_from_conf):
        return self._get_hdfs_count(
            f'/ngdb/us/import/{ngdb_directory_from_conf}|grep .dat')

    def _get_ngdb_us_work_count(self, ngdb_directory_from_conf):
        return self._get_hdfs_count(
            f'/ngdb/us/work/{ngdb_directory_from_conf}/*processing')

    def _get_hdfs_count(self, regex):
        value_dict = {
            'hdfs_user': self._hdfs_user,
            'regex': regex
            }
        return CommandExecutor.get_output(
            Command.GET_COUNT_FROM_HDFS, **value_dict)


class Processor:

    def __init__(self, raw_data):
        self._raw_data = raw_data

    def process(self):
        return self._raw_data


class Presenter:

    def __init__(self, data):
        self._data = data

    def present(self):
        output = []
        current_date = DateTimeUtil.get_current_hour_date()
        for row in self._data:
            adaptation, mntImportCount, mntImportErrorCount, \
                mntProcessingCount, ngdbUsCount, ngdbUsWorkCount = row
            row_dict = {
                "Time": current_date,
                "DataSource": adaptation,
                "datFileCountMnt(/mnt/staging/import)":
                mntImportCount,
                "errorDatFileCountMnt(/mnt/staging/import)":
                mntImportErrorCount,
                "processingDatFileCountMnt(/mnt/staging/import)":
                mntProcessingCount,
                "datFileCountNgdbUsage(/ngdb/us/import)":
                ngdbUsCount,
                "datFileCountNgdbWork(/ngdb/us/work)":
                ngdbUsWorkCount
            }
            output.append(row_dict)
        return output


if __name__ == "__main__":
    main()
