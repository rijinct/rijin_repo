'''
Created on 11-Dec-2019

@author: nageb
'''
from copy import deepcopy
import re
import time

from healthmonitoring.framework import config
from healthmonitoring.framework.specification.defs import Severity, \
    SpecInjector
from healthmonitoring.framework.util.trigger_util import TriggerUtil

from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class Analyzer:

    def __init__(self, actor, events_spec):
        self._actor = actor
        self._events_spec = events_spec
        self._triggers = {}
        self._ttl_queue = _TTLEventQueue()

    @property
    def actor(self):
        return self._actor

    @property
    def events_spec(self):
        return self._events_spec

    @property
    def triggers(self):
        return self._triggers

    @property
    def ttl_queue(self):
        return self._ttl_queue

    def inject_trigger(self, trigger):
        self._triggers[trigger.name] = trigger
        if trigger.name == SpecInjector.TTL_TRIGGER:
            self._inject_ttl_trigger()
        else:
            self._inject_non_ttl_trigger(trigger)

    def raise_queued_event(self, event_name, trigger):
        dispatcher = _EventDispatcher(self, trigger)
        dispatcher.raise_events([self.events_spec.get_event(event_name)],
                                from_queue=True)

    def _inject_ttl_trigger(self):
        for event_name, trigger in self.ttl_queue.extract_due_events().items():
            self.raise_queued_event(event_name, trigger)

    def _inject_non_ttl_trigger(self, trigger):
        processor, dispatcher = self._create_processor_and_dispatcher(trigger)

        processor.process()
        dispatcher.raise_events(processor.events_to_fire)
        dispatcher.raise_clear_events(processor.events_to_clear)
        return processor, dispatcher

    def _create_processor_and_dispatcher(self, trigger):
        processor = _EventProcessor(self._events_spec, self._triggers,
                                    trigger.name)
        dispatcher = _EventDispatcher(self, trigger)
        return processor, dispatcher


class _EventDispatcher:

    def __init__(self, analyzer, trigger):
        self._analyzer = analyzer
        self._trigger = trigger

    def raise_events(self, event_specs, from_queue=False):
        _RaiseEventDispatcher(event_specs, self._analyzer,
                              self._trigger).run(from_queue)

    def raise_clear_events(self, event_specs):
        _ClearEventDispatcher(event_specs, self._analyzer, self._trigger).run()


class _ActionDispatcher:

    def __init__(self, event_specs, analyzer, trigger):
        self._event_specs = event_specs
        self._analyzer = analyzer
        self._trigger = trigger
        self._event_spec = None

    def _take_actions(self):
        event = self._create_event()
        self._analyzer.actor.take_actions(event)

    def _create_event(self):
        logger.info("Generated event: %s", self._event_spec.name)
        return Event(self._event_spec, self._event_spec.current_severity,
                     self._analyzer.triggers)


class _RaiseEventDispatcher(_ActionDispatcher):

    def run(self, from_queue):
        for self._event_spec in self._event_specs:
            if not from_queue and self._is_ttl_event():
                self._queue_event()
            else:
                self._fire_event()

    def _is_ttl_event(self):
        return self._event_spec.consolidated \
            and not TriggerUtil.is_index_trigger(self._trigger.name)

    def _queue_event(self):
        logger.debug("Queuing event '%s'", self._event_spec.name)
        self._analyzer.ttl_queue.append(
            self._event_spec.name, self._trigger, self._event_spec.ttl)

    def _fire_event(self):
        if self._trigger.state:
            self._increase_severity_counter()
        self._analyzer.events_spec.set_event(self._event_spec)
        self._take_actions()
        self._reset_triggers_if_required()

    def _increase_severity_counter(self):
        if self._event_spec.current_severity == Severity.CLEAR:
            self._event_spec.current_severity = self._event_spec.start_level
        self._event_spec.counter += 1
        self._escalate_event_if_required()

    def _escalate_event_if_required(self):
        if self._is_escalation_required():
            self._event_spec.current_severity = Severity(
                self._event_spec.current_severity + 1)
            self._event_spec.counter = 0

    def _is_escalation_required(self):
        return self._event_spec.counter >= self._event_spec.escalation_freq \
            and Severity.get_max_severity() > self._event_spec.current_severity

    def _reset_triggers_if_required(self):
        for trigger_name in self._event_spec.reset_triggers:
            del self._analyzer.triggers[trigger_name]


class _ClearEventDispatcher(_ActionDispatcher):

    def run(self):
        for self._event_spec in self._event_specs:
            self._clear_event_spec()
            self._analyzer.events_spec.set_event(self._event_spec)
            self._take_actions()

    def _clear_event_spec(self):
        self._event_spec.current_severity = Severity.CLEAR
        self._event_spec.counter = 0


class Event:

    def __init__(self, event_spec, severity, triggers):
        self._event_spec = deepcopy(event_spec)
        self._severity = severity
        self._triggers = deepcopy(triggers)

    @property
    def name(self):
        return self._event_spec.name

    @property
    def severity(self):
        return self._severity

    @property
    def event_spec(self):
        return deepcopy(self._event_spec)

    @property
    def triggers(self):
        return self._triggers

    @property
    def condition(self):
        return self._event_spec.condition


class _EventProcessor:

    def __init__(self, events_spec, triggers, trigger_name):
        self._events_spec = events_spec
        self._triggers = triggers
        self._trigger_name = trigger_name

    @property
    def events_to_fire(self):
        return self._events_to_fire

    @property
    def events_to_clear(self):
        return self._events_to_clear

    def process(self):
        self._events_to_fire, self._events_to_clear = [], []
        for self._event_spec in self._events_spec.get_events().values():
            if self._should_fire_event():
                self._events_to_fire.append(self._event_spec)
            elif self._should_clear_event():
                self._events_to_clear.append(self._event_spec)

    def _should_fire_event(self):
        if TriggerUtil.is_index_trigger(self._trigger_name):
            return self._should_fire_event_for_index_trigger()
        else:
            return self._should_fire_event_for_trigger(self._trigger_name)

        return False

    def _should_fire_event_for_index_trigger(self):
        if not self._event_spec.consolidated:
            primary_trigger_name = \
                TriggerUtil.remove_suffix_from_trigger_name(
                    self._trigger_name)
            return self._should_fire_event_for_trigger(primary_trigger_name)

        return False

    def _create_trigger_state_dict(self):
        d = {}
        for trigger_name, trigger in self._triggers.items():
            d[trigger_name] = trigger.state
        return d

    def _should_fire_event_for_trigger(self, primary_trigger_name):
        evaluator = _EventEvaluator(self._event_spec.condition)
        if primary_trigger_name in evaluator.get_referenced_names():
            evaluator.substitute(self._create_trigger_state_dict())
            return evaluator.evaluate()
        return False

    def _should_clear_event(self):
        if not TriggerUtil.is_index_trigger(self._trigger_name) and \
                self._should_clear_event_for_trigger(self._trigger_name):
            current_severity = self._event_spec.current_severity
            return current_severity != Severity.CLEAR \
                and (current_severity != self._event_spec.start_level
                     or self._event_spec.counter > 0)
        return False

    def _should_clear_event_for_trigger(self, trigger_name):
        evaluator = _EventEvaluator(self._event_spec.condition)
        if trigger_name in evaluator.get_referenced_names():
            evaluator.substitute(self._create_trigger_state_dict())
            return not evaluator.evaluate()
        return False


class _EventEvaluator:

    def __init__(self, condition):
        self._condition = condition

    def substitute(self, values_dict):
        logger.debug("Before substitution: '%s'", self._condition)
        for name in self.get_referenced_names():
            self._condition = re.sub(r"\b{}\b".format(name),
                                     str(values_dict.get(name, False)),
                                     self._condition)

    def get_referenced_names(self):
        pattern = re.compile(r"\b\w+\b")
        names = set(pattern.findall(self._condition))
        for operator in "and", "or", "not", "True", "False":
            names.discard(operator)
        return names

    def evaluate(self):
        logger.debug("Event evaluator: '%s' => '%s'",
                     self._condition,
                     eval(self._condition))
        return eval(self._condition)


class _TTLEventQueue:
    TARGET_TIME, TRIGGER = "target_time", "trigger"

    def __init__(self):
        self._events = {}

    @property
    def event_names(self):
        return list(self._events.keys())

    def append(self, event_name, trigger, ttl_secs):
        if event_name not in self._events:
            expiry = time.time() + ttl_secs
            self._events[event_name] = {
                _TTLEventQueue.TRIGGER: trigger,
                _TTLEventQueue.TARGET_TIME: expiry
            }
            logger.debug("Added event '%s' into queue with expiry '%s'",
                         event_name, expiry)

    def extract_due_events(self):
        now = time.time()
        due_event_names = [
            event_name for event_name, details in self._events.items()
            if details[_TTLEventQueue.TARGET_TIME] <= now
        ]

        logger.debug("TTL due events: %s", ','.join(due_event_names))
        due_events = {}
        for event_name in due_event_names:
            due_events[event_name] = self._events[event_name][
                _TTLEventQueue.TRIGGER]

        self._remove_due_events(due_event_names)
        return due_events

    def _remove_due_events(self, due_event_names):
        for event_name in due_event_names:
            del self._events[event_name]
