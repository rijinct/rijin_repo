'''
Created on 07-Feb-2020

@author: nageb
'''
import unittest

from mock import patch

from healthmonitoring.framework import config
from healthmonitoring.framework.actors import email
from healthmonitoring.framework.actors.email import EmailAction, \
    EmailActor, EmailMedia
from healthmonitoring.framework.util.specification import SpecificationUtil
from tests.healthmonitoring.framework.utils import TestUtil


@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
    return_value="./../../../resources")
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts')
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
    return_value=0)
class TestEmailAction(unittest.TestCase):
    def _send_mail(self, formatted_email):
        self.formatted_email = formatted_email

    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
        return_value="./../../resources")
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts'  # noqa: E501
    )
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
        return_value=0)
    def setUp(self, mock_get_config_dir_path, mock_repopulate_hosts,
              mock_get_config_map_time_stamp):
        TestUtil.load_specification()
        email.hosts_spec = config.hosts_spec

    def test_email_media_created_only_once(self, mock_get_config_dir_path,
                                           mock_repopulate_hosts,
                                           mock_get_config_map_time_stamp):
        action1 = EmailAction()
        media = action1._media
        action2 = EmailAction()
        self.assertEqual(action2._media, media)

    def _get_formatted_email(self, sender, recepients, email_subject,
                             email_body):
        message = """From: {}
To: {}
MIME-Version: 1.0
Content-type: text/html
Subject: {}
<p style="font-size: large; font-style: bold">****AlertDetails****</p>
{}
""".format(sender, recepients, email_subject, email_body)
        return message

    def test_email_action(self, mock_get_config_dir_path,
                          mock_repopulate_hosts,
                          mock_get_config_map_time_stamp):
        notification_content = \
            config.events_spec.get_event(
                "event1").get_notification('email_notification')
        email = SpecificationUtil.get_host("email")
        sender = email.fields['cemod_smtp_sender_emailids']
        recepients = email.fields['cemod_smtp_receiver_emailids']
        expected_email_template = self._get_formatted_email(
            sender, recepients, notification_content["subject"],
            notification_content["body"])
        action = EmailAction()
        action._send_mail = self._send_mail
        action.execute(notification_content)
        self.assertEqual(expected_email_template, self.formatted_email)


class TestEmailActor(unittest.TestCase):
    class MockEmailAction:
        def execute(self, notification_content):
            TestEmailActor.notification_content = notification_content

    def test_act(self):
        actor = EmailActor()
        notification_content = {
            "subject": "Test subject",
            "body": "Test body",
            "event": None
        }
        original = email.EmailAction
        email.EmailAction = TestEmailActor.MockEmailAction
        actor.act(notification_content)
        self.assertEqual(TestEmailActor.notification_content,
                         notification_content)
        email.EmailAction = original


@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
    return_value="./../../../resources")
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts')
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
    return_value=0)
class TestEmailMedia(unittest.TestCase):
    def test_getters(self, mock_get_config_dir_path, mock_repopulate_hosts,
                     mock_get_config_map_time_stamp):
        media = EmailMedia()
        url = "URL"
        sender = "SENDER"
        recepients = "RECEPIENTS"
        media._setup(url, sender, recepients)
        self.assertEqual(media.smtp_url, url)
        self.assertEqual(media.sender, sender)
        self.assertEqual(media.recepients, recepients)


if __name__ == "__main__":
    unittest.main()
