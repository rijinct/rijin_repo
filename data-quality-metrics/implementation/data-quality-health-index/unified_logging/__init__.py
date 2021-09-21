'''
Created on 16-Sep-2021

@author: rithomas
'''

import logging
from .logger import Logger
from .message import Message
from .measurement import Measurement
from .alarm import Alarm, AlarmTask, PerceivedSeverity
from .alarm_task import AlarmTask
from .perceived_severity import PerceivedSeverity
from .syslog_level import SyslogLevel
from .missing_mandatory_filed import MissingMandatoryField
from .time_zone import Timezone

try:
    import thread
    import threading
except ImportError:
    thread = None

if thread:
    _lock = threading.RLock()
else:
    _lock = None


def _acquireLock():
    if _lock:
        _lock.acquire()

def _releaseLock():
    if _lock:
        _lock.release()

#level value used in logger api, which is mapped to python logging level
EMERG = logging.CRITICAL+20
ALERT = logging.CRITICAL+10
CRITICAL = logging.CRITICAL
CRIT = logging.CRITICAL
ERROR = logging.ERROR
WARN = logging.WARN
WARNING = WARN
NOTICE = logging.INFO+5
INFO = logging.INFO
DEBUG = logging.DEBUG

#in python logging module, predefined levels has name: CRITICAL, ERROR, WARNING, INFO, DEBUG
#following new levels with name are added
logging.addLevelName(EMERG, "EMERG")
logging.addLevelName(ALERT, "ALERT")
logging.addLevelName(NOTICE, "NOTICE")

nameToSyslogLevel={
    "EMERG" :   SyslogLevel.LOG_EMERG,
    "ALERT" :   SyslogLevel.LOG_ALERT,
    "CRITICAL": SyslogLevel.LOG_CRIT,
    "ERROR" :   SyslogLevel.LOG_ERR,
    "WARNING" : SyslogLevel.LOG_WARNING,
    "NOTICE" :  SyslogLevel.LOG_NOTICE,
    "INFO" :    SyslogLevel.LOG_INFO,
    "DEBUG" :   SyslogLevel.LOG_DEBUG,
}

levelToPrintName={
    EMERG : 'emerg',
    ALERT : 'alert',
    CRIT : 'crit',
    CRITICAL : 'crit',
    ERROR : 'err',
    WARN : 'warning',
    WARNING : 'warning',
    NOTICE : 'notice',
    INFO : 'info',
    DEBUG : 'debug'
}
