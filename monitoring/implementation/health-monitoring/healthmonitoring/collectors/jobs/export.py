'''
Created on 16-May-2020

@author: deerakum
'''
import sys
from pathlib import Path

from healthmonitoring.collectors import _LocalLogger
from healthmonitoring.collectors.utils.queries import Queries, QueryExecutor
from healthmonitoring.framework.specification.defs import DBType
from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.file_util import FileUtils

logger = _LocalLogger.get_logger(__name__)


def main():
    print(_execute())


def _execute():
    raw_data = Collector().collect()
    return Presenter(raw_data).present()


class Collector:

    def collect(self):
        self._input_arg = self._get_input_arg()
        if self._input_arg == 'sdk':
            return self._get_sdk_export_info()
        elif self._input_arg == 'cct':
            return self._get_cct_export_info()

    def _get_sdk_export_info(self):
        self._sdk_export = []
        sdk_dict = {"pattern": self._input_arg}
        sdk_export_info = QueryExecutor.execute(DBType.POSTGRES_SDK,
                                                Queries.SDK_EXPORT_JOBS_INFO,
                                                **sdk_dict)
        for sdk_exp_item in sdk_export_info:
            self._populate_sdk_files_info(sdk_exp_item)
        return self._sdk_export

    def _populate_sdk_files_info(self, sdk_exp_item):
        path = sdk_exp_item[1]
        arg_dict = {"path": path}
        files_count = FileUtils.get_files_count_in_dir(path)
        dir_size = CommandExecutor.get_output(Command.GET_SIZE_OF_DIR,
                                              **arg_dict)
        self._sdk_export.append((sdk_exp_item[0], path, files_count, dir_size))

    def _get_cct_export_info(self):
        self._cct_export = []
        cct_file_path = "/mnt/cctexport/*.zip"
        files = FileUtils.get_files_in_dir(Path(cct_file_path).parent,
                                           pattern=Path(cct_file_path).name)
        filenames = [
            i[1 + i.index('_'):i.rindex('_', 0, i.rindex('_'))] for i in files
        ]
        self._populate_cct_files_info(
            Path(cct_file_path).parent, files, filenames)
        return self._cct_export

    def _populate_cct_files_info(self, path, files, filenames):
        filenames_dict = {i: files.count(i) for i in set(filenames)}
        current_time = DateTimeUtil.get_current_hour_date()
        for file_name in filenames_dict:
            file_size = FileUtils.get_file_size_in_mb(Path(path) / file_name)
            self._cct_export.append((current_time, file_name,
                                     filenames_dict[file_name], file_size))

    def _get_input_arg(self):
        try:
            if sys.argv[1] in ('sdk', 'cct'):
                return sys.argv[1]
            else:
                raise IndexError
        except IndexError:
            logger.info('Usage: python %s sdk|cct', sys.argv[0])
            sys.exit(1)


class Presenter:

    def __init__(self, processed_data):
        self._data = processed_data

    def present(self):
        output = []
        current_date = DateTimeUtil.get_current_hour_date()
        for export_details in self._data:
            temp_dict = {
                "Date": current_date,
                "Export Job": export_details[1],
                "No. of Files": export_details[2],
                "Size": export_details[3]
            }
            output.append(temp_dict)
        logger.debug("Output list has %s", output)
        return output


if __name__ == "__main__":
    main()
