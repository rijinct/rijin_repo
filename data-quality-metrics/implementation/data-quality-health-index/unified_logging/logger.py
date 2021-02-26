import sys
import os
import logging
import logging.config
import logging.handlers
import traceback
from .message_abc import MessageABC
from .message import Message
from .syslog_level import SyslogLevel
import unified_logging
from logging import Formatter

class Logger(object):

    def __init__(self, config_dic, name=None):
        """
        Initialize unified_logging Logger
        :param config_dic: a dictionary of logging configuration
        """
        if not isinstance(config_dic, dict):
            raise TypeError('The parameter config_dic must be a dict')

        if (name is None and
                'loggers' in config_dic and
                'unified_logging' in config_dic['loggers']):
            #for backward compatible, if unified_logging is configured, still use it.
            name='unified_logging'

        logging.config.dictConfig(config_dic)
        self._logger = logging.getLogger(name)
        self._default_fields = {}

    def log(self, level, msg, *args, **kwargs):
        self._log(level, msg, args, **kwargs)

    def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False):
        if not self._logger.isEnabledFor(level):
            return
        if isinstance(msg, MessageABC):
            if exc_info or extra or stack_info:
                raise ValueError("too many arguments.")
            self._log_obj(level, msg)
        else:
            self._log_str(level, msg, args, exc_info=exc_info, extra=extra, stack_info=stack_info)

    def _log_str(self, level, msg, args, exc_info=None, extra=None, stack_info=False):
        unified_logging._acquireLock()
        fields=self._default_fields.copy()
        unified_logging._releaseLock()

        if args:
            msg = msg % args

        if extra:
            if not isinstance(extra, dict):
                raise TypeError('The parameter extra must be a dict')
            fields.update(extra)

        msg_obj=Message()

        for k in fields:
            if k == Message.KEY_CATEGORY or k == Message.KEY_CLASS:
                msg_obj.set_category(fields[k])
            elif k == Message.KEY_NEID:
                msg_obj.set_systemid(fields[k])
            elif k == Message.KEY_EXTENSION:
                msg_obj.set_extension_dict(fields[k])
            else:
                msg_obj.set_field(k, fields[k])

        if stack_info:
            stack=traceback.format_list(traceback.extract_stack())
            stack=stack[:-3]  # discard stack from this Logger library
            msg_obj.set_stacktrace(''.join(stack))
        if exc_info:
            if isinstance(exc_info, BaseException):
                if sys.version_info[0] >= 3:
                    exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
                else:
                    exc_info = (type(exc_info), exc_info, "")
            elif not isinstance(exc_info, tuple):
                exc_info = sys.exc_info()
            format=Formatter()
            msg+="\n"
            msg+=format.formatException(exc_info)

        msg_obj.set_log(msg)
        msg_obj.set_level(level)
        msg_obj._set_time().validate()
        self._logger.log(level, msg_obj.tojson())

    def _log_obj(self, level, msg):
        #set fields from ENV for obj style parameter
        unified_logging._acquireLock()
        fields=self._default_fields.copy()
        unified_logging._releaseLock()
        if not msg.contains_key(MessageABC.KEY_HOST) and MessageABC.KEY_HOST in fields:
            msg.set_host(fields[MessageABC.KEY_HOST])
        if not msg.contains_key(MessageABC.KEY_SYSTEM) and MessageABC.KEY_SYSTEM in fields:
            msg.set_system(fields[MessageABC.KEY_SYSTEM])
        if not msg.contains_key(MessageABC.KEY_SYSTEMID) and MessageABC.KEY_SYSTEMID in fields:
            msg.set_systemid(fields[MessageABC.KEY_SYSTEMID])
        msg.set_level(level)
        msg._set_time().validate()
        self._logger.log(level, msg.tojson())
        msg._unset_time()

    def debug(self, msg, *args, **kwargs):
        """
        Log a message with severity 'DEBUG'.
        :param msg: the message string to be logged
        :param args: log 'msg % args'.
        :param kwargs: set fields per message, such as 'level', 'system', 'systemid', 'host', 'categrory', extension

        example:
            logger.debug("debug:%s", "debug message ... ")
            logger.debug("value=%d", 10000, extra={"system":"system-name"})
        """
        self._log(SyslogLevel.DEBUG, msg, args, **kwargs)

    def info(self, msg, *args, **kwargs):
        """
        Log a message with severity 'INFO'.
        """
        self._log(SyslogLevel.INFO, msg, args, **kwargs)

    def notice(self, msg, *args, **kwargs):
        """
        Log a message with severity 'NOTICE'.
        """
        self._log(SyslogLevel.NOTICE, msg, args, **kwargs)


    def warning(self, msg, *args, **kwargs):
        """
        Log a message with severity 'WARNING'.
        """
        self._log(SyslogLevel.WARNING, msg, args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        """
        Log a message with severity 'WARNING'.
        """
        self._log(SyslogLevel.WARNING, msg, args, **kwargs)


    def error(self, msg, *args, **kwargs):
        """
        Log a message with severity 'ERROR'.
        """
        self._log(SyslogLevel.ERROR, msg, args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """
        Log a message with severity 'CRITICAL'.
        """
        self._log(SyslogLevel.CRITICAL, msg, args, **kwargs)

    def alert(self, msg, *args, **kwargs):
        """
        Log a message with severity 'ALERT'.
        """
        self._log(SyslogLevel.ALERT, msg, args, **kwargs)

    def emerg(self, msg, *args, **kwargs):
        """
        Log a message with severity 'ALERT'.
        """
        self._log(SyslogLevel.EMERG, msg, args, **kwargs)

    def set(self, fields):
        """
        set message's fields, which will be included in all logs produced by this logger.
        :param fields: dict with fields 'level', 'system', 'systemid', 'host', 'categrory', etc.

        example:
            mandatory_fields={"level":SyslogLevel.INFO,"systemid":"system-id","host":"host-name","system":"system-name"}
            logger.set(mandatory_fields)
        """
        if not isinstance(fields, dict):
            raise TypeError('The parameter fields must be a dict')

        unified_logging._acquireLock()
        self._default_fields.update(fields)
        unified_logging._releaseLock()

    def setDefaultByGetenv(self):
        """
        set message's fields system, systemid, host from environment variable.
        """
        fields={}
        host=os.getenv('HOST')
        if host:
            fields[MessageABC.KEY_HOST]=host
        system=os.getenv('SYSTEM')
        if system:
            fields[MessageABC.KEY_SYSTEM]=system
        systemid=os.getenv('SYSTEMID')
        if systemid:
            fields[MessageABC.KEY_SYSTEMID]=systemid

        if fields:
            unified_logging._acquireLock()
            self._default_fields.update(fields)
            unified_logging._releaseLock()

    def clean(self):
        """
        clean all fields set by method set().
        """
        unified_logging._acquireLock()
        self._default_fields = {}
        unified_logging._releaseLock()

    def isEnabledFor(self, level):
        """
        check whether logging level for this logger.
        :param logging level.
        """
        return self._logger.isEnabledFor(level)

    def setLevel(self, level):
        """
        Sets logging level for this logger.
        :param logging level.
        """

        self._logger.setLevel(level)

    def getEffectiveLevel(self):
        """
        get the effective logging level for this logger.
        """

        return self._logger.getEffectiveLevel()

    def exception(self, msg, *args, **kwargs):
        """
        Logs a exception with level ERROR on this logger.
        This method should only be called from an exception handler.
        """

        if 'exc_info' not in kwargs:
            kwargs['exc_info'] = True

        self.error(msg, *args, **kwargs)
