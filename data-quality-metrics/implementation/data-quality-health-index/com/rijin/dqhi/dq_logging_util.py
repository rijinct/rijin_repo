import logging
import traceback
import os
import constants


class SingletonType(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonType, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class loggerUtil(object):
    __metaclass__ = SingletonType 
    _logger = None
 
    def __init__(self, streamingConsole, logLevel):
        self._logger = logging.getLogger('Logger')
        global streamHandler, formatter, enableConsole
        enableConsole = streamingConsole
        if enableConsole:
            streamHandler = logging.StreamHandler()
        self._logger.setLevel(logLevel)
        formatter = logging.Formatter('%(asctime)s: %(filename)s: %(levelname)s: %(message)s')
        for hdlr in self._logger.handlers[:]:  # remove all old handlers
            self._logger.removeHandler(hdlr)

    def get_logger(self, filename):
        if constants.ENABLE_LOCAL_EXECUTION is True:
            fileHandler = logging.FileHandler('{}.log'.format(filename))
        else:
            if not os.path.exists(constants.MONITORING_DIR):
                log_file=filename
            else:
                log_file='{d}/{f}.log'.format(d=constants.MONITORING_DIR,f=filename)
            fileHandler = logging.FileHandler(log_file)
        fileHandler.setFormatter(formatter)
        self._logger.addHandler(fileHandler)
        if enableConsole:
            self._logger.addHandler(streamHandler)
        return self._logger
