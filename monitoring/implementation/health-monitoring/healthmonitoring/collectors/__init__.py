'''
Created on 17-Apr-2020

@author: deerakum
'''
from datetime import datetime as _datetime
import sys
import traceback

from healthmonitoring.collectors import config
from logger import Logger as _Logger
sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')


class _LocalLogger:
    _logger = None

    @staticmethod
    def create_logger(filename):

        TYPE = "FILE_LOGGER"
        BACKUP_COUNT = 10
        filename = _LocalLogger._get_file_name_with_date(filename)

        try:
            _LocalLogger._logger = _Logger.create(type=TYPE,
                                                  filename=filename,
                                                  backupCount=BACKUP_COUNT)
        except RuntimeError:
            traceback.print_exc()

    @staticmethod
    def get_logger(name):
        logger = _Logger.getLogger(name)

        _LocalLogger._copy_handlers(logger)

        logger.setLevel(config.log_level)
        return logger

    @staticmethod
    def _get_file_name_with_date(file_name):
        return "/var/local/monitoring/log/{}_{}.log".format(
            file_name,
            _datetime.now().strftime("%Y-%m-%d"))

    @staticmethod
    def _copy_handlers(logger):
        for handler in _LocalLogger._logger.handlers:
            logger.addHandler(handler)
