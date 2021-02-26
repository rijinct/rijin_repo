import logging
from logging.handlers import RotatingFileHandler
import unified_logging
import os
from builtins import staticmethod


class Logger:
    __logger = None
    __is_ulog = False

    STDOUT_FILE_LOGGER = "STDOUT_FILE_LOGGER"
    STDOUT_STREAM_LOGGER = "STDOUT_STREAM_LOGGER"
    FILE_LOGGER = "FILE_LOGGER"
    ROTATING_FILE_LOGGER = "ROTATING_FILE_LOGGER"
    DEFAULT_FILENAME = "logging.log"
    DEFAULT_MAX_BYTES = 1024 * 1024 * 100
    DEFAULT_BACKUP_COUNT = 5

    @staticmethod
    def create(**kargs):
        if Logger.__logger and Logger.__is_ulog:
            raise RuntimeError("Multiple instantiation of Logger!")

        logger_type = kargs.get("type", Logger.STDOUT_FILE_LOGGER)
        Logger.__is_ulog = kargs.get("ulog", False)

        if Logger.__is_ulog:
            Logger._setup_unified_logger(logger_type, kargs)
            return Logger.__logger
        else:
            return Logger._setup_basic_logger(logger_type, kargs)

    @staticmethod
    def getLogger(name):
        if Logger.__is_ulog:
            logger = Logger.__logger
        else:
            logger = logging.getLogger(name)
        return logger

    @staticmethod
    def _setup_unified_logger(logger_type, kargs):
        if logger_type == Logger.STDOUT_FILE_LOGGER:
            Logger.__logger = unified_logging.Logger(
                StdoutFileLoggingConfig(kargs).data)
        elif logger_type == Logger.STDOUT_STREAM_LOGGER:
            Logger.__logger = unified_logging.Logger(
                StdoutStreamLoggingConfig(kargs).data)
        elif logger_type == Logger.FILE_LOGGER:
            Logger.__logger = unified_logging.Logger(
                FileLoggingConfig(kargs).data)
        elif logger_type == Logger.ROTATING_FILE_LOGGER:
            Logger.__logger = unified_logging.Logger(
                RotatingFileLoggingConfig(kargs).data)
        else:
            raise Exception("Unknown logger type: '%s'" % str(logger_type))

        if 'level' in kargs:
            level = Logger._str2level(kargs['level'])
            Logger.__logger.setLevel(level)

        componentName = kargs.get('componentName', "Unknown")
        Logger.__logger.set({
            'systemid': componentName,
            'host': os.uname().nodename,
            'system': 'analytics'
        })

    @staticmethod
    def str2level(string):
        d = {
            "NOTSET": logging.NOTSET,
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        return d[string]

    @staticmethod
    def _setup_basic_logger(logger_type, kargs):
        fmt = '%(asctime)s %(name)s %(levelname)s: %(message)s'

        if logger_type == Logger.FILE_LOGGER:
            logging.basicConfig(format=fmt,
                                filename=kargs.get('filename',
                                                   Logger.DEFAULT_FILENAME))
        elif logger_type in (Logger.STDOUT_FILE_LOGGER,
                            Logger.STDOUT_STREAM_LOGGER):
            logging.basicConfig(format=fmt)

        logger = logging.getLogger(__name__)
        if 'level' in kargs:
            level = Logger.str2level(kargs['level'])
            logger.setLevel(level)

        if logger_type == Logger.ROTATING_FILE_LOGGER:
            filename = kargs.get('filename', Logger.DEFAULT_FILENAME)
            maxBytes = int(kargs.get('maxBytes', Logger.DEFAULT_MAX_BYTES))
            backupCount = int(
                kargs.get('backupCount', Logger.DEFAULT_BACKUP_COUNT))
            handler = RotatingFileHandler(filename,
                                          maxBytes=maxBytes,
                                          backupCount=backupCount)
            handler.setFormatter(logging.Formatter(fmt))
            logger.addHandler(handler)

        return logger


class StdoutFileLoggingConfig(object):

    def __init__(self, kargs):
        self.__data = \
{
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
        'fileFormater': {
            'format': '%(message)s'
        }
        },
        'handlers': {
        'fileHandler':{
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'fileFormater',
            'filename': '/dev/stdout',
            'mode': 'w',
        }
        },
        'loggers': {
        'unified_logging': {
            'handlers': ['fileHandler'],
            'propagate': True,
            'level': 'DEBUG',
        }
        }
        }
        if 'level' in kargs:
            self.__data['handlers']['fileHandler']['level'] = kargs['level']
            self.__data['loggers']['unified_logging']['level'] = kargs['level']

    @property
    def data(self):
        assert isinstance(self.__data,
                          dict), 'The data of LoggingConfig is not a dict'
        return self.__data


class StdoutStreamLoggingConfig(object):

    def __init__(self, kargs):
        self.__data = \
{
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
        'consoleFormater': {
            'format': '%(message)s'
        }
        },
        'handlers': {
        'consoleHandler':{
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'consoleFormater',
            'stream': 'ext://sys.stdout',
        }
        },
        'loggers': {
        'unified_logging': {
            'handlers': ['consoleHandler'],
            'propagate': True,
            'level': 'DEBUG',
        }
        }
        }
        if 'level' in kargs:
            self.__data['handlers']['consoleHandler']['level'] = kargs['level']
            self.__data['loggers']['unified_logging']['level'] = kargs['level']

    @property
    def data(self):
        assert isinstance(self.__data,
                          dict), 'The data of LoggingConfig must is not dict'
        return self.__data


class FileLoggingConfig(object):

    def __init__(self, kargs):
        self.__data = \
{
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
        'fileFormater': {
            'format': '%(message)s'
        }
        },
        'handlers': {
        'fileHandler':{
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'fileFormater',
            'filename': 'logging.log',
            'mode': 'a',
        }
        },
        'loggers': {
        'unified_logging': {
            'handlers': ['fileHandler'],
            'propagate': True,
            'level': 'DEBUG',
        }
        }
        }
        if 'level' in kargs:
            self.__data['handlers']['fileHandler']['level'] = kargs['level']
            self.__data['loggers']['unified_logging']['level'] = kargs['level']
        if 'filename' in kargs:
            self.__data['handlers']['fileHandler']['filename'] = kargs[
                'filename']

    @property
    def data(self):
        assert isinstance(self.__data,
                          dict), 'The data of LoggingConfig is not a dict'
        return self.__data


class RotatingFileLoggingConfig(object):

    def __init__(self, kargs):
        self.__data = \
{
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
        'fileFormater': {
            'format': '%(message)s'
        }
        },
        'handlers': {
        'fileHandler':{
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'fileFormater',
            'filename': 'logging.log',
            'mode': 'a',
            'maxBytes': 50 * 1024 * 1024,
            'backupCount': 5,
        }
        },
        'loggers': {
        'unified_logging': {
            'handlers': ['fileHandler'],
            'propagate': True,
            'level': 'DEBUG',
        }
        }
        }
        if 'level' in kargs:
            self.__data['handlers']['fileHandler']['level'] = kargs['level']
            self.__data['loggers']['unified_logging']['level'] = kargs['level']
        if 'filename' in kargs:
            self.__data['handlers']['fileHandler']['filename'] = kargs[
                'filename']
        if 'maxBytes' in kargs:
            self.__data['handlers']['fileHandler']['maxBytes'] = kargs[
                'maxBytes']
        if 'backupCount' in kargs:
            self.__data['handlers']['fileHandler']['backupCount'] = kargs[
                'backupCount']

    @property
    def data(self):
        assert isinstance(self.__data,
                          dict), 'The data of LoggingConfig is not a dict'
        return self.__data
