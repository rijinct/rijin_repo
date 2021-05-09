import os
import sys
import traceback

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from propertyFileUtil import PropertyFileUtil
from dateTimeUtil import DateTimeUtil
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

class FileCleaner:
    def __init__(self, days=7):
        self._days = days

    def clean(self):
        list_of_directories = PropertyFileUtil().getAllDirectories()
        for directory in list_of_directories:
            try:
                for file in os.listdir(directory):
                    self._remove_file(directory, file)
            except Exception:
                traceback.print_exc()
        logger.info("Cleaned files in the directories '%s' ", list_of_directories)

    def _remove_file(self, directory, file):
        file_path = os.path.join(directory, file)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            if os.stat(file_path).st_mtime < DateTimeUtil(
            ).get_epoch_time_of_past_days(self._days):
                os.remove(file_path)
                logger.debug("Successfully removed old file '%s' ", file_path)


if __name__ == '__main__':
    FileCleaner().clean()
