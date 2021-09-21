'''
Created on 16-Sep-2021

@author: rithomas
'''
import logging

class SyslogLevel():
    '''
    The enum of SyslogLevel, according to rfc5424 (https://tools.ietf.org/html/rfc5424)
        Numerical Code     Severity
        0       Emergency: system is unusable
        1       Alert: action must be taken immediately
        2       Critical: critical conditions
        3       Error: error conditions
        4       Warning: warning conditions
        5       Notice: normal but significant condition
        6       Informational: informational messages
        7       Debug: debug-level messages
        Table 2. Syslog Message Severities
    '''
    LOG_EMERG     = 0
    LOG_ALERT     = 1
    LOG_CRIT      = 2
    LOG_ERR       = 3
    LOG_WARNING   = 4
    LOG_NOTICE    = 5
    LOG_INFO      = 6
    LOG_DEBUG     = 7

    #deprecated, keep for backward compatible, same as level defined in __init__.py
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
