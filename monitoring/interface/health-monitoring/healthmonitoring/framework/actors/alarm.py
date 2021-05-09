'''
Created on 13-Jul-2020

@author: nageb
'''

from unified_logging.alarm import Alarm
from unified_logging.alarm_task import AlarmTask
from unified_logging.perceived_severity import PerceivedSeverity
from healthmonitoring.framework import config

from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class AlarmActor:

    def act(self, notification_content):
        self._notification_content = notification_content
        self._raise_alert()

    def _raise_alert(self):
        name, key, text, event_type, probable_cause, id = [
            self._notification_content.get(key) for key in
            ['name', 'key', 'text', 'event_type', 'probable_cause', 'id']]

        alarm = Alarm()
        alarm.set_alarm_mandatory_fields(
            AlarmTask.NOTIFY, name,
            self._notification_content.get('severity', PerceivedSeverity.CRIT),
            key, text, event_type, probable_cause)
        alarm.set_alarm_id(id)
        logger.critical(alarm)
