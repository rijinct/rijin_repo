'''
Created on 13-Jul-2020

@author: nageb
'''
import unittest

from mock import patch

from healthmonitoring.framework.actors.alarm import AlarmActor
from unified_logging.alarm_task import AlarmTask
from unified_logging.perceived_severity import PerceivedSeverity


class TestAlarm(unittest.TestCase):

    @patch('healthmonitoring.framework.actors.alarm.Alarm.set_alarm_mandatory_fields')  # noqa: E501
    def test_without_severity(self, mock_set_alarm_mandatory_fields):
        NAME, KEY, TEXT, EVENT_TYPE, PROBABLE_CAUSE = map(str, range(5))
        ID = 100000

        notification_content = {
            'name': NAME,
            'key': KEY,
            'text': TEXT,
            'event_type': EVENT_TYPE,
            'probable_cause': PROBABLE_CAUSE,
            'id': ID
            }
        AlarmActor().act(notification_content)

        mock_set_alarm_mandatory_fields.assert_called_with(
            AlarmTask.NOTIFY, NAME, PerceivedSeverity.CRIT, KEY, TEXT,
            EVENT_TYPE, PROBABLE_CAUSE)

    @patch('healthmonitoring.framework.actors.alarm.Alarm.set_alarm_mandatory_fields')  # noqa: E501
    def test_with_severity(self, mock_set_alarm_mandatory_fields):
        NAME, KEY, TEXT, EVENT_TYPE, PROBABLE_CAUSE, SEVERITY = \
            map(str, range(6))
        ID = 100000

        notification_content = {
            'name': NAME,
            'key': KEY,
            'text': TEXT,
            'event_type': EVENT_TYPE,
            'probable_cause': PROBABLE_CAUSE,
            'severity': SEVERITY,
            'id': ID
            }
        AlarmActor().act(notification_content)

        mock_set_alarm_mandatory_fields.assert_called_with(
            AlarmTask.NOTIFY, NAME, SEVERITY, KEY, TEXT,
            EVENT_TYPE, PROBABLE_CAUSE)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
