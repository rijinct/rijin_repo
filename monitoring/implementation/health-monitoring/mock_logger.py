'''
Created on 30-Apr-2020

@author: nageb
'''

from logger import Logger as _Logger


class Logger:

    @staticmethod
    def create(type=None, filename=None, backupCount=None):
        return _Logger.create()

    @staticmethod
    def getLogger(module_name):
        return _Logger.getLogger(module_name)
