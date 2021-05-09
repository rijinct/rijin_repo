"""
Copyright (C) 2018 Nokia. All rights reserved.
"""
from .message_abc import MessageABC, LogType
from .alarm_task import AlarmTask
from .perceived_severity import PerceivedSeverity

class Alarm(MessageABC):
    KEY_ALARM_TASK = 'alarm.task'
    KEY_ALARM_ID = 'alarm.id'
    KEY_ALARM_NAME = 'alarm.name'
    KEY_ALARM_SEVERITY = 'alarm.severity'
    KEY_ALARM_DATA = 'alarm.data'
    KEY_ALARM_KEY = 'alarm.key'
    KEY_ALARM_TEXT = 'alarm.text'
    KEY_ALARM_EVENT_TYPE = 'alarm.event-type'
    KEY_ALARM_PROBABLE_CAUSE = 'alarm.probable-cause'

    def __init__(self, level=None, neid=None, host=None, system=None):
        '''
        Constructor of Alarm
        :param level: log level, of value as enum SyslogLevel, which is a mandatory field
        :param neid: unique ID assigned to the system producing the event, which is a mandatory field
        :param host: name of the host on which the container producing the event is running, whic is a mandatory field
        :param system: name of the system in which the event occurred, which is a mandatory field
        '''
        super(Alarm, self).__init__(level, neid, host, system)
        self._mandatory_fileds.append(self.KEY_ALARM_TASK)
        self._mandatory_fileds.append(self.KEY_ALARM_NAME)
        self._mandatory_fileds.append(self.KEY_ALARM_SEVERITY)
        self._mandatory_fileds.append(self.KEY_ALARM_KEY)
        self._mandatory_fileds.append(self.KEY_ALARM_TEXT)
        self._mandatory_fileds.append(self.KEY_ALARM_EVENT_TYPE)
        self._mandatory_fileds.append(self.KEY_ALARM_PROBABLE_CAUSE)
        self.set_type(LogType.ALARM)

    def set_alarm_task(self, task):
        '''
        Set the task of alarm to be raised or cleared via notify, acknowledge, or unacknowledge actions
        :param task: the task of alarm to be raised, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter task is not a member of AlarmTask
        '''
        if not isinstance(task, AlarmTask):
            raise TypeError('The parameter task must be a member of AlarmTask')
        self._msg.set(self.KEY_ALARM_TASK, task.value)
        return self

    def set_alarm_id(self, id):
        '''
        Set numerical ID of the alarm
        :param id: Numerical ID of the alarm.
        :return: self
        :raise TypeError: if the parameter id is not an int
        '''
        if not isinstance(id, int):
            raise TypeError('The parameter id must be an integer')
        self._msg.set(self.KEY_ALARM_ID, id)
        return self

    def set_alarm_name(self, name):
        '''
        Set Short textual ID of the alarm
        :param name: Short textual ID of the alarm, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter name is not an str
        '''
        if not isinstance(name, str):
            raise TypeError('The parameter name must be a str')
        self._msg.set(self.KEY_ALARM_NAME, name)
        return self

    def set_alarm_severity(self, severity):
        '''
        Set ITU Perceived severity of the alarm, according to RFC 5674
        :param severity: ITU Perceived severity of the alarm, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter severity is not an member of PerceivedSeverity
        '''
        if not isinstance(severity, PerceivedSeverity):
            raise TypeError('The parameter severity must be a member of PerceivedSeverity')
        self._msg.set(self.KEY_ALARM_SEVERITY, severity.value)
        return self

    def set_alarm_data(self, data):
        '''
        Set alarm instance specific information about the actual problem
        :param data: Alarm instance specific information about the actual problem
        :return: self
        :raise TypeError: if the parameter data is not an str
        '''
        if not isinstance(data, str):
            raise TypeError('The parameter data must be a str')
        self._msg.set(self.KEY_ALARM_DATA, data)
        return self

    def set_alarm_key(self, key):
        '''
        Set the key that identifies a particular alarm instance to correlate actions on the given alarm
        :param key: A key that identifies a particular alarm instance to correlate actions on the given alarm, which is a mandatory field.
        :return: self
        :raise TypeError: if the parameter key is not an str
        '''
        if not isinstance(key, str):
            raise TypeError('The parameter key must be a str')
        self._msg.set(self.KEY_ALARM_KEY, key)
        return self

    def set_alarm_text(self, text):
        '''
        Set the brief summary of what has happened, defined in the alarm definition.
        :param text: the brief summary of what has happened, defined in the alarm definition, which is a mandatory field.
        :return: self
        :raise TypeError: if the parameter text is not an str
        '''
        if not isinstance(text, str):
            raise TypeError('The parameter text must be a str')
        self._msg.set(self.KEY_ALARM_TEXT, text)
        return self

    def set_alarm_event_type(self, event_type):
        '''
        Set the event type defined in the alarm definition according to ITU-T REC X.733 (possible values are defined in further standards as well).
        :param event_type: the event type defined in the alarm definition, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter event_type is not an str
        '''
        if not isinstance(event_type, str):
            raise TypeError('The parameter event_type must be a str')
        self._msg.set(self.KEY_ALARM_EVENT_TYPE, event_type)
        return self

    def set_alarm_probable_cause(self, probable_cause):
        '''
        Set the probable cause defined in the alarm definition according to ITU-T REC X.733 (possible values are defined in further standards as well).
        :param probable_cause: the probable cause defined in the alarm definition, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter probable_cause is not an str
        '''
        if not isinstance(probable_cause, str):
            raise TypeError('The parameter probable_cause must be a str')
        self._msg.set(self.KEY_ALARM_PROBABLE_CAUSE, probable_cause)
        return self

    def set_alarm_mandatory_fields(self, task, name, severity, key, text, event_type, probable_cause):
        '''
        Set some mandatory fields of the alarm message, task, name, severity, key, text, event-type, and probable-cause.
        :param task: the task of alarm to be raised, which is a mandatory field
        :param name: Short textual ID of the alarm, which is a mandatory field
        :param severity: ITU Perceived severity of the alarm, which is a mandatory field
        :param key: A key that identifies a particular alarm instance to correlate actions on the given alarm, which is a mandatory field.
        :param text: the brief summary of what has happened, defined in the alarm definition, which is a mandatory field.
        :param event_type: the event type defined in the alarm definition, which is a mandatory field
        :param probable_cause: the probable cause defined in the alarm definition, which is a mandatory field
        :return:self
        :raise TypeError: if the parameters type mismatch
        '''
        self.set_alarm_task(task)
        self.set_alarm_name(name)
        self.set_alarm_severity(severity)
        self.set_alarm_key(key)
        self.set_alarm_text(text)
        self.set_alarm_event_type(event_type)
        self.set_alarm_probable_cause(probable_cause)
        return self

    def validate(self):
        '''
        Check if any of the mandatory fields are missing.
        :return: None
        :raise MissingMandatoryField: if any of the mandatory fields is missing
        '''
        self._msg.validate(self._mandatory_fileds)
