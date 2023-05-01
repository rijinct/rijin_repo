#! /usr/bin/python
#####################################################################################
#####################################################################################
# (c)2016 NOKIA
# Author:  SDK Team
# Version: 0.1
# Purpose: This script creates a single logger interface
# Date:    21-10-2019
#####################################################################################
import logging		

class SingletonType(type):
 _instances = {}

 def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonType, class_).__call__(*args, **kwargs)
        return cls._instances[class_]
	
class loggerUtil(object):
 __metaclass__ = SingletonType 
 _logger = None
 
 def __init__(self):
    self._logger = logging.getLogger('Logger')
    global streamHandler,formatter
    streamHandler = logging.StreamHandler()
    self._logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s: %(filename)s: %(levelname)s: %(message)s')
    for hdlr in self._logger.handlers[:]:  # remove all old handlers
        self._logger.removeHandler(hdlr)

 def get_logger(self,filename):
    fileHandler = logging.FileHandler(r'C:\Users\rithomas\Desktop\myCygwin\dqhi\log\{}.log'.format(filename))
    fileHandler.setFormatter(formatter)
    self._logger.addHandler(fileHandler)
    self._logger.addHandler(streamHandler)
    return self._logger

