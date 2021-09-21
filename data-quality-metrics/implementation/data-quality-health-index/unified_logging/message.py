'''
Created on 16-Sep-2021

@author: rithomas
'''
from .message_abc import MessageABC, LogType
import sys
import locale

class Message(MessageABC):
    KEY_LOG_FIELD_NAME = 'log'
    KEY_MSG = 'message'
    KEY_CATEGORY = 'category'
    KEY_CLASS = 'class' #deprecated
    KEY_STACK = 'stacktrace'

    KEY_LOG_MSG = KEY_LOG_FIELD_NAME+'.'+KEY_MSG
    KEY_LOG_CATEGORY = KEY_LOG_FIELD_NAME+'.'+KEY_CATEGORY
    KEY_LOG_STACK = KEY_LOG_FIELD_NAME+'.'+KEY_STACK

    def __init__(self, level=None, systemid=None, host=None, system=None):
        '''
        Constructor of Message
        :param level: log level, of value as enum SyslogLevel, which is a mandatory field
        :param neid: unique ID assigned to the system producing the event, which is a mandatory field
        :param host: name of the host on which the container producing the event is running, whic is a mandatory field
        :param system: name of the system in which the event occurred, which is a mandatory field
        '''
        super(Message, self).__init__(level, systemid, host, system)
        self._mandatory_fileds.append(self.KEY_LOG_FIELD_NAME)
        self.set_type(LogType.LOG)
        self.os_encoding = locale.getpreferredencoding()

    def set_message(self, msg):
        '''
        Set the text of the log message
        :param msg: the text of the log messages, which is a mandatory field for the message of type log
        :return: self
        :raise TypeError: if the parameter msg is not a str
        '''
        if not isinstance(msg, str):
            raise TypeError('The parameter log must be a str')
        self._msg.set(self.KEY_LOG_MSG, msg)
        return self

    def set_log(self, log):
        '''
        Set the text of the log message
        :param log: the text of the log messages, which is a mandatory field for the message of type log
        :return: self
        :raise TypeError: if the parameter log is not a str
        '''
        if sys.version_info[0] >= 3:
            if isinstance(log, bytes):
                log=log.decode(self.os_encoding)
            if not isinstance(log, str):
                raise TypeError('The parameter log must be a str')
        else:
            if not isinstance(log, str)and not isinstance(log, unicode):
                raise TypeError('The parameter log must be a str')
        self._msg.set(self.KEY_LOG_MSG, log)
        return self

    def set_class(self, clazz):
        '''
        Deprecated.
        Set message category, used to categorize log messages
        :param clazz: the class(category) of log message
        :return: self
        :raise TypeError: if the parameter cls is not a str
        '''
        if not isinstance(clazz, str):
            raise TypeError('The parameter clazz must be a str')
        return self.set_category(clazz)

    def set_category(self, category):
        '''
        Set message category, used to categorize log messages
        :param category: the category of log message
        :return: self
        :raise TypeError: if the parameter category is not a str
        '''
        if not isinstance(category, str):
            raise TypeError('The parameter category must be a str')
        self._msg.set(self.KEY_LOG_CATEGORY, category)
        return self

    def set_stacktrace(self, stack):
        '''
        Set stack backtrace
        :param stack: stack backtrace
        :return: self
        :raise TypeError: if the parameter stack is not a str
        '''
        if not isinstance(stack, str):
            raise TypeError('The parameter stack must be a str')
        self._msg.set(self.KEY_LOG_STACK, stack)
        return self

    def set_field(self, key, value):
        '''
        Set a field in message
        :param key: field's key
        :param value: field's value
        :return: self
        :raise TypeError: if the parameter stack is not a str
        '''
        if not isinstance(key, str):
            raise TypeError('The parameter key must be a str')
        self._msg.set(key, value)
        return self

    def validate(self):
        '''
        Check if any of the mandatory fields are missing.
        :return: None
        :raise MissingMandatoryField: if any of the mandatory fields is missing
        '''
        self._msg.validate(self._mandatory_fileds)
