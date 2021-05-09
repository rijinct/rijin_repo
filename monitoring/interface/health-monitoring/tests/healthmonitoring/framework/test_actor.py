'''
Created on 10-Jan-2020

@author: nageb
'''
import unittest

from healthmonitoring.framework import config
from healthmonitoring.framework.actors.restart_container import \
    RestartContainerActor
from healthmonitoring.framework.specification.defs import Severity
from healthmonitoring.framework.analyzer import Event
from healthmonitoring.framework.actor import Actor
from healthmonitoring.framework.actors.email import EmailActor

from tests.healthmonitoring.framework.utils import TestUtil


class TestActor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def setUp(self):
        self.events_spec = config.events_spec
        self.event_spec = self.events_spec.get_event("event1")
        self.event = Event(self.event_spec, Severity.CRITICAL, {})

    def test_get_actor_instance(self):
        instance = Actor._get_actor_instance("email")
        self.assertIsInstance(instance, EmailActor)

    def test_get_actor_instance_when_action_conatains_underscore(self):
        instance = Actor._get_actor_instance("restart_container")
        self.assertIsInstance(instance, RestartContainerActor)

    def test_get_notification(self):
        notification_content = Actor._get_notification_content(
            "email", self.event_spec)
        self.assertEqual(notification_content, {
            'subject': 'None',
            'body': 'None'
        })

    def test_given_event_creates_suitable_actions(self):
        events_spec = config.events_spec
        event_spec = events_spec.get_event("event1")
        event = Event(event_spec, Severity.CRITICAL, {})

        actions = []

        def take_action(self, event, action):
            actions.append(action)

        old_func = Actor._take_action
        Actor._take_action = take_action

        actor = Actor()
        actor.take_actions(event)

        Actor._take_action = old_func

        self.assertEqual(actions, ["email", "restart"])

    def test_take_action(self):

        class MockActor:
            notification_content = None

            @staticmethod
            def _get_actor_instance(actor_name):
                return MockActor()

            @staticmethod
            def _get_notification_content(actor_name, event_spec):
                return Actor._get_notification_content(actor_name, event_spec)

            def act(self, notification_content):
                MockActor.notification_content = notification_content

        actor = Actor()
        original = Actor._get_actor_instance
        Actor._get_actor_instance = MockActor._get_actor_instance
        actor._take_action(self.event, "email")
        expected = {'subject': 'None', 'body': 'None', 'event': self.event}
        self.assertEqual(MockActor.notification_content, expected)
        Actor._get_actor_instance = original
