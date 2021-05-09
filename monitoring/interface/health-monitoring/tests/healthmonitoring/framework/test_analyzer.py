'''
Created on 10-Jan-2020

@author: nageb
'''
import time
import unittest

from healthmonitoring.framework import config
from healthmonitoring.framework.analyzer import Analyzer, Event, \
    _EventEvaluator, _EventProcessor, _TTLEventQueue
from healthmonitoring.framework.specification.defs import \
    TriggerSpecification, Severity, SpecInjector
from healthmonitoring.framework.store import Trigger
from healthmonitoring.framework.util.trigger_util import TriggerUtil

from tests.healthmonitoring.framework.utils import TestUtil


class DummyActor():

    def __init__(self):
        self.called = 0

    def take_actions(self, event):
        self.called += 1

    def was_called(self):
        return bool(self.called)


class TestAnalyzer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def setUp(self):
        self.spec = config.events_spec
        self.trigger_specification = TriggerSpecification(
            "trigger1", {"item1": "${app1.item1}"}, "{item1} > 100")

    def test_generate_an_event(self):
        counter_before_trigger = self.spec.get_event("event1").counter
        trigger = Trigger(self.trigger_specification)
        trigger.state = True
        analyzer = Analyzer(DummyActor(), self.spec)
        analyzer.inject_trigger(trigger)
        counter_after_trigger = self.spec.get_event("event1").counter
        self.assertGreater(counter_after_trigger, counter_before_trigger,
                           'Failure in generating an event')

    def test_escalate_an_event(self):
        current_severity_before_trigger = self.spec.get_event(
            "event1").current_severity
        trigger = Trigger(self.trigger_specification)
        trigger.state = True
        analyzer = Analyzer(DummyActor(), self.spec)
        analyzer.inject_trigger(trigger)
        analyzer.inject_trigger(trigger)
        analyzer.inject_trigger(trigger)
        analyzer.inject_trigger(trigger)
        severity_after_multiple_triggers = self.spec.get_event(
            "event1").current_severity
        self.assertNotEqual(severity_after_multiple_triggers,
                            current_severity_before_trigger,
                            'Failure occurred while escalating an event')

    def test_escalate_an_event_above_max_severity(self):
        trigger_specification = TriggerSpecification(
            "trigger4", {"item1": "${app1.item1}"}, "{item1} > 100")
        trigger = Trigger(trigger_specification)
        trigger.state = True
        analyzer = Analyzer(DummyActor(), self.spec)
        analyzer.inject_trigger(trigger)
        analyzer.inject_trigger(trigger)
        analyzer.inject_trigger(trigger)
        analyzer.inject_trigger(trigger)
        severity_after_multiple_triggers = self.spec.get_event(
            "event4").current_severity
        self.assertEqual(
            severity_after_multiple_triggers, Severity.get_max_severity(),
            'Failure occurred while escalating an event above max severity')

    def test_generate_clear_event(self):
        trigger = Trigger(self.trigger_specification)
        trigger.state = False
        analyzer = Analyzer(DummyActor(), self.spec)
        event = self.spec.get_event("event1")
        event.counter = 1
        self.spec.set_event(event)
        analyzer.inject_trigger(trigger)
        counter_after_trigger = self.spec.get_event("event1").counter
        severity_after_clear_trigger = self.spec.get_event(
            "event1").current_severity
        self.assertEqual(
            counter_after_trigger, 0,
            'Failure on resetting counter in an event of clear event')
        self.assertEqual(
            severity_after_clear_trigger, Severity(0),
            'Failure in setting severity to clear while clearing an event')

    def test_generate_event_after_clear_event(self):
        trigger = Trigger(self.trigger_specification)
        trigger.state = False
        analyzer = Analyzer(DummyActor(), self.spec)
        counter_before_trigger = self.spec.get_event("event1").counter
        analyzer.inject_trigger(trigger)
        trigger.state = True
        analyzer.inject_trigger(trigger)
        counter_after_trigger = self.spec.get_event("event1").counter
        start_level_severity = self.spec.get_event("event1").start_level
        severity_after_clear_trigger = self.spec.get_event(
            "event1").current_severity
        self.assertEqual(
            counter_after_trigger,
            counter_before_trigger + 1,
            'Failure in incrementing an event counter after a clear event has occurred'  # noqa:E501
        )
        self.assertEqual(
            severity_after_clear_trigger,
            start_level_severity,
            'Failure in setting severity to start level after an event is occurred right after a clear event'  # noqa:E501
        )

    def test_get_event_names_to_be_fired(self):
        trigger = Trigger(self.trigger_specification)
        trigger.state = True
        analyzer = Analyzer(DummyActor(), self.spec)
        analyzer._triggers[trigger.name] = trigger
        event_processor = _EventProcessor(analyzer._events_spec,
                                          analyzer._triggers, trigger.name)
        event_processor.process()
        event_names = [spec.name for spec in event_processor.events_to_fire]
        self.assertEqual(event_names, ["event1", "event6"])

    def test_reset_triggers(self):
        trigger1 = Trigger(self.trigger_specification)
        trigger1.state = True
        trigger3 = Trigger(TriggerSpecification("trigger3", {}, ""))
        trigger3.state = True

        analyzer = Analyzer(DummyActor(), self.spec)
        analyzer.inject_trigger(trigger1)
        analyzer.inject_trigger(trigger3)

        self.assertTrue(analyzer._triggers["trigger1"])
        self.assertFalse("trigger3" in analyzer._triggers)

    def test_trigger_with_suffix(self):
        TRIGGER_NAME = "execution_duration_12"
        value = TriggerUtil.remove_suffix_from_trigger_name(TRIGGER_NAME)
        self.assertEqual("execution_duration", value)

    def test_trigger_without_suffix(self):
        TRIGGER_NAME = "execution_duration"
        value = TriggerUtil.remove_suffix_from_trigger_name(TRIGGER_NAME)
        self.assertEqual("execution_duration", value)

    def _get_specific_trigger_list(self, specific_trigger_name):
        TRIGGER_NAMES = [
            "execution_duration_breached_trigger_12", "failed_jobs_trigger_1",
            "failed_jobs_trigger_2"
        ]
        actual = TriggerUtil.get_specific_trigger_list(specific_trigger_name,
                                                       TRIGGER_NAMES)
        return actual, TRIGGER_NAMES

    def test_get_single_instance_trigger_list(self):
        actual, TRIGGER_NAMES = self._get_specific_trigger_list(
            "execution_duration_breached_trigger")
        self.assertEqual(actual, [TRIGGER_NAMES[0]])

    def test_get_multiple_instances_trigger_list(self):
        actual, TRIGGER_NAMES = self._get_specific_trigger_list(
            "failed_jobs_trigger")
        self.assertEqual(actual, [TRIGGER_NAMES[1], TRIGGER_NAMES[2]])

    def test_is_index_trigger_on_index_trigger(self):
        self.assertTrue(TriggerUtil.is_index_trigger("trigger_1"))

    def test_is_index_trigger_on_non_index_trigger(self):
        self.assertFalse(TriggerUtil.is_index_trigger("trigger1"))

    def _create_or_get_trigger(
            self, trigger_name, variables={}, condition=""):
        if not hasattr(self, "_triggers"):
            self._triggers = {}

        if trigger_name not in self._triggers:
            self._triggers[trigger_name] = Trigger(
                TriggerSpecification(trigger_name, variables, condition))
        return self._triggers[trigger_name]

    def _create_event_processor(self, trigger_names_to_set,
                                trigger_name_to_fire):
        analyzer = Analyzer(DummyActor(), self.spec)
        for trigger_name in trigger_names_to_set:
            trigger = self._create_or_get_trigger(trigger_name)
            trigger.state = True
            analyzer._triggers[trigger_name] = trigger

        event_processor = _EventProcessor(analyzer._events_spec,
                                          analyzer._triggers,
                                          trigger_name_to_fire)
        event_processor.process()
        return event_processor

    def test_should_event_fire_on_index_when_consolidation_is_no(self):
        event_processor = self._create_event_processor(
            ["trigger1_1"], "trigger1_1")
        self.assertFalse(event_processor.events_to_fire)

    def test_should_event_fire_on_consolidation_when_consolidation_is_no(self):
        event_processor = self._create_event_processor(
            ["trigger1_1", "trigger1"], "trigger1")
        self.assertTrue(event_processor.events_to_fire)

    def test_should_event_fire_on_index_when_consolidation_is_yes(self):
        event_processor = self._create_event_processor(["trigger_cons1_1"],
                                                       "trigger_cons1_1")
        self.assertFalse(event_processor.events_to_fire)

    def test_should_event_fire_on_cons_when_cons_is_yes(self):
        event_processor = self._create_event_processor(
            ["trigger_cons1_1", "trigger_cons1"], "trigger_cons1")
        self.assertTrue(event_processor.events_to_fire)


class TestEvent(unittest.TestCase):
    EVENT_NAME = "event1"
    SEVERITY = Severity.MINOR

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def setUp(self):
        self.spec = config.events_spec
        self.trigger_spec1 = TriggerSpecification("trigger1", {}, "")
        self.trigger_spec2 = TriggerSpecification("trigger2", {}, "")

    def test_create_event_and_get_name(self):
        event = Event(self.spec.get_event(TestEvent.EVENT_NAME),
                      TestEvent.SEVERITY, [])
        self.assertEqual(event.name, TestEvent.EVENT_NAME)

    def test_create_event_and_get_severity(self):
        event = Event(self.spec.get_event(TestEvent.EVENT_NAME),
                      TestEvent.SEVERITY, [])
        self.assertEqual(event.severity, TestEvent.SEVERITY)

    def test_create_event_and_get_event_triggers(self):
        TRIGGERS = [Trigger(self.trigger_spec1), Trigger(self.trigger_spec2)]
        event = Event(self.spec.get_event(TestEvent.EVENT_NAME),
                      TestEvent.SEVERITY, TRIGGERS)
        self.assertEqual(event.triggers, TRIGGERS)


class TestEventEvaluator(unittest.TestCase):

    def test_given_empty_condition_no_names_are_present(self):
        condition = ""
        evaluator = _EventEvaluator(condition)

        actual = evaluator.get_referenced_names()
        expected = set()
        self.assertEqual(actual, expected)

    def test_given_condition_with_only_name(self):
        condition = "trigger1"
        evaluator = _EventEvaluator(condition)

        actual = evaluator.get_referenced_names()
        expected = {"trigger1"}
        self.assertEqual(actual, expected)

    def test_given_condition_with_names_and_operators(self):
        condition = "trigger1 and trigger2 or not trigger3"
        evaluator = _EventEvaluator(condition)

        actual = evaluator.get_referenced_names()
        expected = {"trigger1", "trigger2", "trigger3"}
        self.assertEqual(actual, expected)

    def test_given_condition_with_duplicate_names_and_operators(self):
        condition = "trigger1 and trigger2 or not trigger1"
        evaluator = _EventEvaluator(condition)

        actual = evaluator.get_referenced_names()
        expected = {"trigger1", "trigger2"}
        self.assertEqual(actual, expected)

    def test_given_condition_with_names_operators_and_parentheses(self):
        condition = "(trigger1 and trigger2) or ((trigger1))"
        evaluator = _EventEvaluator(condition)

        actual = evaluator.get_referenced_names()
        expected = {"trigger1", "trigger2"}
        self.assertEqual(actual, expected)

    def test_substitution_of_triggers_by_values(self):
        condition = "(trigger1 and trigger2) or (trigger3)"
        evaluator = _EventEvaluator(condition)

        evaluator.substitute({
            "trigger1": True,
            "trigger2": False,
            "trigger3": False
        })
        actual = evaluator._condition
        expected = "(True and False) or (False)"
        self.assertEqual(actual, expected)

    def test_evaluate(self):
        condition = "(trigger1 and not trigger2) or trigger3"

        for run in (((0, 0, 0), 0), ((0, 0, 1), 1), ((0, 1, 1), 1),
                    ((1, 0, 0), 1), ((1, 0, 1), 1), ((1, 1, 0), 0), ((1, 1, 1),
                                                                     1)):

            trig_values, expected = run
            evaluator = _EventEvaluator(condition)
            evaluator.substitute({
                "trigger1": trig_values[0],
                "trigger2": trig_values[1],
                "trigger3": trig_values[2]
            })
            actual = evaluator.evaluate()
            self.assertEqual(actual, expected)


class TestTTLEventQueue(unittest.TestCase):

    def setUp(self):
        TestUtil.load_specification()
        self.spec = config.events_spec

    def test_when_non_consolidate_event_fires_it_is_not_queued(self):
        actor = DummyActor()
        analyzer = Analyzer(actor, self.spec)
        trigger1 = Trigger(TriggerSpecification("trigger1", {}, ""))
        trigger1.state = True
        analyzer.inject_trigger(trigger1)
        self.assertFalse(analyzer._ttl_queue.event_names)
        self.assertTrue(actor.was_called())

    def test_when_consolidate_event_fires_it_is_queued(self):
        actor = DummyActor()
        analyzer = Analyzer(actor, self.spec)
        trigger1 = Trigger(TriggerSpecification("trigger_cons1", {}, ""))
        trigger1.state = True
        analyzer.inject_trigger(trigger1)
        self.assertEqual(analyzer._ttl_queue.event_names, ["event7"])
        self.assertFalse(actor.was_called())

    def test_when_multiple_triggers_occur_single_cons_event_is_queued(self):
        actor = DummyActor()
        analyzer = Analyzer(actor, self.spec)
        trigger1 = Trigger(TriggerSpecification("trigger_cons1", {}, ""))
        trigger1.state = True
        trigger2 = Trigger(TriggerSpecification("trigger_cons2", {}, ""))
        trigger2.state = True
        analyzer.inject_trigger(trigger1)
        analyzer.inject_trigger(trigger2)
        self.assertEqual(analyzer._ttl_queue.event_names, ["event7"])
        self.assertFalse(actor.was_called())

    def test_setting_of_target_time(self):
        actor = DummyActor()
        analyzer = Analyzer(actor, self.spec)
        trigger1 = Trigger(TriggerSpecification("trigger_cons1", {}, ""))
        trigger1.state = True
        time_now = time.time()
        analyzer.inject_trigger(trigger1)
        self.assertGreater(
            analyzer._ttl_queue._events["event7"][_TTLEventQueue.TARGET_TIME],
            time_now)

    def test_extraction_of_due_events(self):
        actor = DummyActor()
        analyzer = Analyzer(actor, self.spec)
        trigger = Trigger(TriggerSpecification("trigger", {}, ""))
        analyzer._ttl_queue.append("event7", trigger, 0)
        self.assertEqual(analyzer._ttl_queue.extract_due_events(),
                         {'event7': trigger})

    def test_generation_of_event_after_ttl(self):
        actor = DummyActor()
        analyzer = Analyzer(actor, self.spec)
        trigger1 = Trigger(TriggerSpecification("trigger_cons1", {}, ""))
        trigger1.state = True
        analyzer.events_spec._events["event7"].ttl = 0
        analyzer.inject_trigger(trigger1)
        self._inject_ttl_trigger(analyzer)
        self.assertTrue(actor.was_called())

    def _inject_ttl_trigger(self, analyzer):
        ttl_trigger = Trigger(
            TriggerSpecification(SpecInjector.TTL_TRIGGER, {}, ""))
        ttl_trigger.state = True
        analyzer.inject_trigger(ttl_trigger)


if __name__ == "__main__":
    unittest.main()
