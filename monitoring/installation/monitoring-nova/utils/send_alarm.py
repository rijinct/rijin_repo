'''
Created on 18-Aug-2020

@author: deerakum
'''
import os
import sys

from yaml import safe_load

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from logger_util import *
from unified_logging.alarm import Alarm
from unified_logging.alarm_task import AlarmTask
from unified_logging.perceived_severity import PerceivedSeverity

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))


class SnmpAlarm:

    @staticmethod
    def send_alarm(alarm_key, alarm_data, alarm_severity=None):
        hm_alarms = []
        hm_alarms_spec = SnmpAlarm.get_healthmonitoring_alarms()
        for alarm in hm_alarms_spec:
            hm_alarms.append(list(alarm.keys())[0])
        if alarm_key in hm_alarms and SnmpAlarm.get_enable_prop(
                alarm_key)['snmp']:
            SnmpAlarm.send_snmp_alarm(hm_alarms_spec, alarm_key, alarm_data,
                                      alarm_severity)
        else:
            logger.info(
                "Alarm Key is not present in Alarm Spec/Yaml or Alarm is disabled"  # noqa: 501
            )

    @staticmethod
    def get_healthmonitoring_alarms():
        alarm_config = "/monitoring/alarms/alarms"
        with open(alarm_config) as alarm_reader:
            content = alarm_reader.read()
        return safe_load(content)['health-monitoring-alarms']

    @staticmethod
    def get_enable_prop(alarm_key):
        hm_alarms_spec = SnmpAlarm.get_healthmonitoring_alarms()
        alarm_prop = SnmpAlarm.get_alarm_properties(hm_alarms_spec, alarm_key)
        return {
            "snmp": alarm_prop[alarm_key]['snmp_enabled'],
            "email": alarm_prop[alarm_key]['email_enabled']
        }

    @staticmethod
    def send_snmp_alarm(hm_alarms_spec, alarm_key, alarm_data, alarm_severity):
        alarm_properties = SnmpAlarm.get_alarm_properties(
            hm_alarms_spec, alarm_key)
        name, key, text, event_type, probable_cause, \
 = [alarm_key, alarm_properties[alarm_key]['key'],
           alarm_properties[alarm_key]['text'],
           alarm_properties[alarm_key]['eventType'],
           alarm_properties[alarm_key]['probableCause']]
        alarm = Alarm()
        alarm.set_alarm_mandatory_fields(
            AlarmTask.NOTIFY, name,
            SnmpAlarm.get_alarm_severity(alarm_properties, alarm_key,
                                         alarm_severity), key, text,
            str(event_type), probable_cause)
        alarm.set_alarm_id(int(alarm_properties[alarm_key]['id']))
        alarm.set_alarm_data(str(alarm_data))
        alarm.set_extension_dict(
            {"Enterprise": "{0}".format(os.environ.get('RELEASE_NAMESPACE')),
             "Source": "healthmonitoring"})
        logger.critical(alarm)
        logger.info("Alarm is sent")

    @staticmethod
    def get_alarm_properties(hm_alarms_spec, alarm_key):
        alarm_properties = {}
        for index, content in enumerate(hm_alarms_spec):
            if alarm_key in content:
                alarm_properties = hm_alarms_spec[index]

        return alarm_properties

    @staticmethod
    def get_alarm_severity(alarm_properties, alarm_key, alarm_severity):
        alarm_severity_dict = {
            'critical': PerceivedSeverity.CRIT,
            'major': PerceivedSeverity.MAJOR,
            'minor': PerceivedSeverity.MINOR,
            'warning': PerceivedSeverity.WARNING
        }
        if not alarm_severity:
            alarm_severity = alarm_properties[alarm_key]['severity']
        return alarm_severity_dict[alarm_severity.lower()]

    @staticmethod
    def get_severity_for_email(alarm_key):
        hm_alarms_spec = SnmpAlarm.get_healthmonitoring_alarms()
        alarm_prop = SnmpAlarm.get_alarm_properties(hm_alarms_spec, alarm_key)
        return alarm_prop[alarm_key]['severity'].upper()
