import datetime
import abc
import sys
from enum import Enum,unique
from .message_impl import MessageImpl
from .time_zone import Datetime, Timezone
from .syslog_level import SyslogLevel
import unified_logging

@unique
class LogType(Enum):
    '''
    The type of the event log entry ("log", "alarm" or "counter")
    '''
    LOG = 'log'
    ALARM = 'alarm'
    COUNTER = 'counter'

if sys.version_info[0] >= 3:
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta('ABC', (), {})

class MessageABC(ABC):
    KEY_TYPE = 'type'
    KEY_LEVEL = 'level'
    KEY_FACILITY = 'facility'
    KEY_TIME = 'time'
    KEY_SERVICE = 'service'
    KEY_PROCESS = 'process'
    KEY_SYSTEM = 'system'
    KEY_SYSTEMID = 'systemid'
    KEY_NEID = 'neid'
    KEY_CONTAINER = 'container'
    KEY_HOST = 'host'
    KEY_TIMEZONE = 'timezone'
    KEY_VERSION = 'version'
    KEY_EXTENSION = 'extension'

    ver='1.0'

    def __init__(self, level=None, systemid=None, host=None, system=None):
        '''
        Constructor of MessageABC
        :param level: log level, of value as enum SyslogLevel, which is a mandatory field
        :param systemid: unique ID assigned to the system producing the event, which is a mandatory field
        :param host: name of the host on which the container producing the event is running, which is a mandatory field
        :param system: name of the system in which the event occurred, which is a mandatory field
        '''
        self._mandatory_fileds = [self.KEY_TIME, self.KEY_TYPE, self.KEY_LEVEL]
        self._date = None
        self._msg = MessageImpl()
        self._msg.set(self.KEY_VERSION, self.ver)
        if level:
            self.set_level(level)
        if systemid:
            self.set_systemid(systemid)
        if host:
            self.set_host(host)
        if system:
            self.set_system(system)

    def set_type(self, typ):
        '''
        Set the type of the event log entry ('log', 'alarm' or 'counter')
        :type typ: LogType
        :param typ: an enum of values as log, alarm, or counter
        :return: self
        :raise TypeError: if the parameter typ is not a member of LogType Enum
        '''
        if not isinstance(typ, LogType):
            raise TypeError('The parameter typ must be a member of LogType Enum')
        self._msg.set(self.KEY_TYPE, typ.value)
        return self

    def set_level(self, level):
        '''
        Set log level, corresponds to the syslog severity defined in RFC 5424
        :type level: str
        :param level: log level, of value as enum SyslogLevel, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter level is not a member of SyslogLevel Enum
        '''
        if level not in unified_logging.levelToPrintName:
            raise ValueError('invalid %d The parameter level must be a member of %s' %(level, unified_logging.levelToPrintName.keys()))
        self._msg.set(self.KEY_LEVEL, unified_logging.levelToPrintName[level])
        return self

    def set_systemid(self, systemid):
        '''
        Set the unique ID assigned to the system producing the event
        :type systemid: str
        :param systemid: unique ID assigned to the system producing the event, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter systemid is not a str
        '''
        if not isinstance(systemid, str):
            raise TypeError('The parameter systemid must be a str')
        self._msg.set(self.KEY_SYSTEMID, systemid)
        return self

    def set_neid(self, neid):
        '''
        Set the unique ID assigned to the system producing the event
        :type neid: str
        :param neid: unique ID assigned to the system producing the event, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter neid is not a str
        '''
        return self.set_systemid(neid)

    def set_host(self, host):
        '''
        Set name of the host on which the container producing the event is running
        :type host: str
        :param host: name of the host on which the container producing the event is running, whic is a mandatory field
        :return: self
        :raise TypeError: if the parameter host is not a str
        '''
        if not isinstance(host, str):
            raise TypeError('The parameter host must be a str')
        self._msg.set(self.KEY_HOST, host)
        return self

    def set_system(self, system):
        '''
        Set name of the host on which the container producing the event is running
        :type system: str
        :param system: name of the system in which the event occurred, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter system is not a str
        '''
        if not isinstance(system, str):
            raise TypeError('The parameter system must be a str')
        self._msg.set(self.KEY_SYSTEM, system)
        return self

    def set_facility(self, facility):
        '''
        Set the facility corresponding to syslog facilities defined in RFC 5424
        :type facility: str
        :param facility: the syslog facility
        :return: self
        :raise TypeError: if the parameter facility is not a str
        '''
        if not isinstance(facility, str):
            raise TypeError('The parameter facility must be a str')
        self._msg.set(self.KEY_FACILITY, facility)
        return self

    def set_process(self, process):
        '''
        Set name and/or ID of the process producing the event
        :type process: str
        :param process: name and/or ID of the process producing the event
        :return: self
        :raise TypeError: if the parameter process is not a str
        '''
        if not isinstance(process, str):
            raise TypeError('The parameter process must be a str')
        self._msg.set(self.KEY_PROCESS, process)
        return self

    def set_service(self, service):
        '''
        Set name of the service to which the event is related
        :type service: str
        :param service: name of the service to which the event is related
        :return: self
        :raise TypeError: if the parameter service is not a str
        '''
        if not isinstance(service, str):
            raise TypeError('The parameter service must be a str')
        self._msg.set(self.KEY_SERVICE, service)
        return self

    def set_container(self, container):
        '''
        Set short ID of the container from which the event originates
        :type container: str
        :param container: short ID of the container from which the event originates
        :return: self
        :raise TypeError: if the parameter container is not a str
        '''
        if not isinstance(container, str):
            raise TypeError('The parameter container must be a str')
        self._msg.set(self.KEY_CONTAINER, container)
        return self

    def set_timezone(self, timezone):
        '''
        Set time zone of the host's local time in the form of Area/Location, according to IANA definitions.
        :type timezone: str
        :param timezone: time zone.
        :return: self
        :raise TypeError: if the parameter container is neither a timezone nor None
        '''
        if timezone is None or not isinstance(timezone, str):
            raise TypeError('The parameter timezone must be a str')
        self._msg.set(self.KEY_TIMEZONE, timezone)
        return self

    def set_time(self, date):
        '''
        Set the time for this message
        :param date: the datetime. Note: the datetime will be reset after it is sent via the Logger.
        :return: self
        :raise TypeError: if the parameter date is not a object of datetime.date
        '''
        if not isinstance(date, datetime.date):
            raise TypeError('The parameter date must be an isinstance of datetime.date')
        self._date = date
        return self

    def set_extension(self, key, value):
        '''
        Set extension's sub field in message
        :param key: the sub key of extension field
        :param value: the value of sub key
        '''
        if not isinstance(key, str):
            raise TypeError('The parameter key must be an str')
        self._msg.set(self.KEY_EXTENSION+'.'+key, value)
        return self

    def set_extension_dict(self, ext):
        '''
        Set whole extension field in message
        :param ext: dict for extension
        '''
        if not isinstance(ext, dict):
            raise TypeError('The parameter ext must be an dict')
        self._msg.set(self.KEY_EXTENSION, ext)
        return self

    def _set_time(self):
        self._msg.set(self.KEY_TIME, Datetime(self._date).tostring())
        return self

    def _unset_time(self):
        self._date = None
        return self

    def tojson(self):
        '''
        Convert the message to a JSON string
        :return: a JSON string
        '''
        return self._msg.tojson()

    @abc.abstractmethod
    def validate(self):
        '''
        Check if any of the mandatory fields are missing.
        :return: None
        :raise MissingMandatoryField: if any of the mandatory fields is missing
        '''
        pass

    def set_mandatory_fields(self, neid, host, system, timezone):
        '''
        (deprecated, not mandatory fieldS in API level)
        Set some mandatory fields of the message
        :param neid: unique ID assigned to the system producing the event, which is a mandatory field
        :param host: name of the host on which the container producing the event is running, whic is a mandatory field
        :param system: name of the system in which the event occurred, which is a mandatory field
        :param timezone: time zone of the host's local time in the form of Area/Location, according to IANA definitions
        :return: self
        :raise TypeError: if the parameters type mismatch
        '''
        self.set_neid(neid)
        self.set_host(host)
        self.set_system(system)
        self.set_timezone(timezone)
        return self

    def contains_key(self, key):
        return self._msg.contains_key_dotted(key)