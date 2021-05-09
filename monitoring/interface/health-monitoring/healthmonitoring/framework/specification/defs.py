'''
Created on 12-Dec-2019

@author: nageb
'''
from builtins import property
import copy
from enum import IntEnum, Enum
import re

from healthmonitoring.framework import config
from healthmonitoring.framework.specification.util import ConflictDetector
from healthmonitoring.framework.util import collection_util

from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class SpecificationCollection:

    def __init__(self, items_spec, triggers_spec, events_spec):
        self._items_spec = items_spec
        self._triggers_spec = triggers_spec
        self._events_spec = events_spec

    @property
    def items_spec(self):
        return self._items_spec

    @property
    def triggers_spec(self):
        return self._triggers_spec

    @property
    def events_spec(self):
        return self._events_spec

    def conflicts_with(self, specification_collection):
        conflict_detector = ConflictDetector(specification_collection)
        return conflict_detector.are_items_conflicting(self._items_spec) or \
            conflict_detector.are_triggers_conflicting(self._triggers_spec) or\
            conflict_detector.are_events_conflicting(self._events_spec)

    def merge(self, specification_collection):
        self._items_spec.merge(specification_collection.items_spec)
        self._triggers_spec.merge(specification_collection.triggers_spec)
        self._events_spec.merge(specification_collection.events_spec)


class Specification:

    def merge(self, spec):
        self._get_specification_value().update(spec._get_specification_value())

    def conflicts_with(self, spec):
        return bool(set(self._get_keys()) & set(spec._get_keys()))

    def _get_specification_value(self):
        pass

    def _get_keys(self):
        pass


class HostSpecification:

    def __init__(self, spec=None):
        self._host_groups = {}
        if spec:
            self.set_specification(spec)

    def set_specification(self, spec):
        self._host_groups = {}

        for category in spec.keys():
            self._process_host_groups(spec.get(category, {}), category)

    def get_hosts(self, name):
        return self._host_groups[name]

    def get_host(self, name):
        hosts = list(self.get_hosts(name))
        if hosts:
            return hosts[0]
        else:
            raise ValueError("No host available")

    def _process_host_groups(self, host_groups_spec, category):
        for group_name, host_details in host_groups_spec.items():
            self._host_groups[group_name] = self._create_host_group(
                category, group_name, host_details)

    def _create_host_group(self, category, group_name, host_details):
        host_group = HostGroup(category)
        if "hosts" in host_details:
            for address in host_details.get("hosts"):
                service = Service(group_name, address,
                                  self._get_fields_dict(host_details),
                                  self._get_list_fields_dict(host_details),
                                  host_details.get("ha_status", None))
                host_group.add_service(service)
        else:
            service = Service(group_name, "",
                              self._get_fields_dict(host_details),
                              self._get_list_fields_dict(host_details),
                              host_details.get("ha_status", None))
            host_group.add_service(service)
        return host_group

    def _get_fields_dict(self, host_details):
        fields = {}
        for field in host_details.get('fields', []):
            fields[field] = host_details[field]
        return fields

    def _get_list_fields_dict(self, host_details):
        list_fields = {}
        for list_field in host_details.get('list_fields', []):
            list_fields[list_field] = host_details[list_field]
        return list_fields


class Service:

    def __init__(self, name, address, fields, list_fields, ha=None):
        self._name = name
        self._address = address
        self._fields = fields
        self._list_fields = list_fields
        self._ha = bool(ha)

    @property
    def name(self):
        return self._name

    @property
    def address(self):
        return self._address

    @property
    def fields(self):
        return self._fields

    @property
    def list_fields(self):
        return self._list_fields

    @property
    def ha(self):
        return self._ha


class HostGroup:

    def __init__(self, category):
        self._services = []
        self._category = category

    @property
    def category(self):
        return self._category

    @category.setter
    def category(self, value):
        self._category = value

    def add_service(self, service):
        self._services.append(service)

    def __len__(self):
        return len(self._services)

    def __iter__(self):
        self.__iterator_index = 0
        return self

    def __next__(self):
        if self.__iterator_index == len(self):
            raise StopIteration
        else:
            host = self._services[self.__iterator_index]
            self.__iterator_index += 1
            return host


class ItemsSpecification(Specification):

    def __init__(self, spec=None):
        self._applications = {}
        if spec:
            self.set_specification(spec)

    @property
    def applications(self):
        return self._applications

    def set_specification(self, spec):
        self._applications = {}
        for name, details in spec.items():
            self._applications[name] = self._create_application(name, details)

    def get_all_item_names(self):
        return (item.name for application in self._applications.values()
                for item in application.get_items())

    def get_item(self, name):
        for application in self._applications.values():
            item = application.get_item(name)
            if item is not None:
                return item

        return None

    def get_all_enabled_item_names(self):
        return tuple(item.name for application in self._applications.values()
                     for item in application.get_items() if item.enabled)

    def get_all_csv_writing_enabled_item_names(self):
        return tuple(
            item.name for application in self._applications.values()
            for item in application.get_items()
            if item.csv_details.csv_header and item.csv_details.csv_filename)

    def create_item(self, item_spec, app):
        item_name, item_details = list(item_spec.items())[0]
        schedule = item_details["schedule"]
        host = item_details.get("host", "localhost")
        enabled = item_details.get("enabled", True)
        csv_details = self._create_csv_details(item_details)
        db_category = item_details.get("db_category")
        commands = collection_util.convert_list_dict_to_dict(
            item_details.get("commands", []))
        item = ItemSpecification(item_name, host, commands, schedule, app,
                                 csv_details, db_category, enabled)
        self._add_custom_fields(item, item_details)
        return item

    def _get_specification_value(self):
        return self._applications

    def _get_keys(self):
        return self._applications.keys()

    def _create_application(self, name, details):
        application = Application(name)
        for item_spec in details:
            item = self.create_item(item_spec, application)
            application.add_item(item)

        return application

    def _create_csv_details(self, item_details):
        csv_details = CSVDetails(item_details.get("csv_header", None),
                                 item_details.get("csv_filename", None),
                                 item_details.get("csv_header_always", False),
                                 item_details.get("csv_directory", None))
        return csv_details

    def _add_custom_fields(self, item, item_details):
        for key, value in item_details.items():
            STANDARD_FIELDS = ("schedule", "host", "commands")
            if key not in STANDARD_FIELDS:
                item.add_field(key, value)


class CSVDetails():

    def __init__(self,
                 header=None,
                 filename=None,
                 header_always=False,
                 directory=None):
        self._csv_header = header
        self._csv_filename = filename
        self._csv_header_always = header_always
        self._csv_directory = directory

    @property
    def csv_header(self):
        return self._csv_header

    @property
    def csv_filename(self):
        return self._csv_filename

    @property
    def csv_header_always(self):
        return self._csv_header_always

    @property
    def csv_directory(self):
        return self._csv_directory


class ItemSpecification:

    def __init__(self,
                 name,
                 host,
                 commands,
                 schedule,
                 app,
                 csv_details=CSVDetails(),
                 db_category=None,
                 enabled=True):
        self._name = name
        self._schedule = schedule
        self._host = host
        self._enabled = enabled
        self._commands = commands
        self._csv_details = csv_details
        self._db_category = db_category
        self._app = app
        self._fields = {}

    def __str__(self):
        return self.name

    @property
    def name(self):
        if self._app:
            return "{}.{}".format(self._app.name, self._name)
        else:
            return self._name

    @property
    def schedule(self):
        return self._schedule

    @property
    def enabled(self):
        return self._enabled

    @schedule.setter
    def schedule(self, interval):
        self._schedule = interval

    @property
    def app(self):
        return self._app

    @property
    def fields(self):
        return self._fields

    @property
    def host(self):
        return self._host

    @property
    def csv_details(self):
        return self._csv_details

    @property
    def db_category(self):
        return self._db_category

    @property
    def commands(self):
        return self._commands

    def add_field(self, key, value):
        self._fields[key] = value


class Application:

    def __init__(self, name):
        self._name = name
        self._item_specs = {}

    @property
    def name(self):
        return self._name

    def add_item(self, item_spec):
        self._item_specs[item_spec.name] = item_spec

    def get_items(self):
        return tuple(self._item_specs.values())

    def get_item(self, name):
        return self._item_specs.get(name, None)


class TriggersSpecification(Specification):

    def __init__(self, spec=None):
        self.set_specification(spec)

    def set_specification(self, spec):
        self._triggers = {}
        if spec:
            self._populate_triggers(spec[0])

    def get_all_trigger_names(self):
        return self._triggers.keys()

    def get_trigger(self, trigger_name):
        return self._triggers[trigger_name]

    def _get_specification_value(self):
        return self._triggers

    def _get_keys(self):
        return self._triggers.keys()

    def _populate_triggers(self, triggers_spec):
        for trigger_name, trigger_spec in triggers_spec.items():
            aliases = trigger_spec['items']
            aliases[SpecInjector.CONFIG_ITEM_NAME] = \
                SpecInjector().get_config_item_name()
            trigger_condition = trigger_spec['condition']
            trigger_variables = self._get_trigger_variables(
                trigger_spec['variables'], aliases)
            self._triggers[trigger_name] = TriggerSpecification(
                trigger_name, trigger_variables, trigger_condition)

    def _get_trigger_variables(self, variables, aliases):
        updated_variables = {}
        for key, value in variables.items():
            updated_variables[key] = re.sub(
                r"\${(.*?)([\[}])",
                lambda m: "${{{}{}".format(aliases[m.group(1)], m.group(2)),
                value)
        return updated_variables


class TriggerSpecification:

    def __init__(self, name, variables, condition):
        self._name = name
        self._variables = variables
        self._condition = condition

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def variables(self):
        return self._variables

    @variables.setter
    def variables(self, variables):
        self._variables = variables

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, value):
        self._condition = value


class EventsSpecification(Specification):

    def __init__(self, spec=None):
        self._events = {}
        if spec:
            self.set_specification(spec)

    def __eq__(self, other):
        return isinstance(other, EventsSpecification) \
            and self._events == other._events

    def __repr__(self):
        return str(self._events)

    def set_specification(self, spec):
        self._events = {}
        for event_name, event_spec in spec.items():
            self.add_event_spec(event_name, event_spec)

    def get_events(self):
        return copy.deepcopy(self._events)

    def get_event(self, name):
        return copy.deepcopy(self._events[name])

    def set_event(self, event_spec):
        self._events[event_spec.name] = copy.deepcopy(event_spec)

    def add_event_spec(self, event_name, event_spec):
        event = EventSpecification(event_name)
        event.start_level = event_spec['start_level']
        event.escalation_freq = int(event_spec['escalation_freq'])
        event.condition = event_spec['condition']
        self._process_severities(event, event_spec)
        self._process_notifications(event, event_spec)
        self._process_reset_triggers(event, event_spec)
        event.consolidated = event_spec.get('consolidated', False)
        event.ttl = event_spec.get('ttl_secs', 0)
        self._events[event_name] = event

    def _get_specification_value(self):
        return self._events

    def _get_keys(self):
        return self._events.keys()

    def _process_severities(self, event, event_spec):
        for severity in "clear", "minor", "major", "critical":
            for action in event_spec.get(severity, []):
                event.add_severity_action(severity, action)

    def _process_notifications(self, event, event_spec):
        for key, value in event_spec.items():
            if key.endswith("_notification"):
                event.add_notification(key, value)

    def _process_reset_triggers(self, event, event_spec):
        for trigger_name in event_spec.get('reset_triggers', []):
            event.add_reset_trigger(trigger_name)


class EventSpecification:

    def __init__(self, name):
        self._name = name
        self._start_level = Severity.MINOR
        self._escalation_freq = 1
        self._severities = {}
        self._counter = 0
        self._current_severity = self._start_level
        self._notifications = {}
        self._reset_triggers = []
        self._consolidated = False

    def __eq__(self, other):
        return isinstance(other, EventSpecification) \
            and self.name == other.name \
            and self.start_level == other.start_level \
            and self.escalation_freq == other.escalation_freq \
            and self.get_severities() == other.get_severities() \
            and self.counter == other.counter \
            and self.current_severity == other.current_severity \
            and self._notifications == other._notifications \
            and self.reset_triggers == other.reset_triggers \
            and self.consolidated == other.consolidated

    def __repr__(self):
        return str(dict(
            name=self.name,
            start_level=self.start_level,
            escalation_freq=self.escalation_freq,
            severities=self.get_severities(),
            counter=self.counter,
            current_severity=self.current_severity,
            notifications=self._notifications,
            reset_triggers=self.reset_triggers,
            consolidated=self._consolidated
            ))

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name):
        self._name = new_name

    @property
    def start_level(self):
        return self._start_level

    @start_level.setter
    def start_level(self, value):
        self._start_level = Severity.create(value)
        self._current_severity = self._start_level
        self._counter = 0

    @property
    def escalation_freq(self):
        return self._escalation_freq

    @escalation_freq.setter
    def escalation_freq(self, value):
        self._escalation_freq = value

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, condition):
        self._condition = condition

    @property
    def counter(self):
        return self._counter

    @counter.setter
    def counter(self, counter_value):
        self._counter = counter_value

    @property
    def current_severity(self):
        return self._current_severity

    @current_severity.setter
    def current_severity(self, severity):
        self._current_severity = severity

    @property
    def reset_triggers(self):
        return self._reset_triggers

    @property
    def consolidated(self):
        return self._consolidated

    @consolidated.setter
    def consolidated(self, consolidated):
        self._consolidated = consolidated

    @property
    def ttl(self):
        return self._ttl

    @ttl.setter
    def ttl(self, ttl):
        self._ttl = ttl

    def add_severity_action(self, severity, action):
        if severity in self._severities:
            self._severities[severity].append(action)
        else:
            self._severities[severity] = [action]

    def get_severities(self):
        return tuple(str(x) for x in self._severities.keys())

    def get_severity_actions(self, severity):
        return tuple(str(x) for x in self._severities.get(severity, []))

    def add_notification(self, name, content):
        self._notifications[name] = content

    def get_notification(self, name):
        return self._notifications.get(name, {})

    def add_reset_trigger(self, trigger_name):
        self._reset_triggers.append(trigger_name)


class SpecInjector:

    BUILTIN_APP_NAME = "__builtin"
    TTL_ITEM_NAME = "__ttl_item"
    TTL_TRIGGER = "__ttl_trigger"
    CONFIG_ITEM_NAME = "config"

    def __init__(self, specs=None):
        self._specs = specs if specs else (config.items_spec,
                                           config.triggers_spec,
                                           config.events_spec)

    @property
    def items_spec(self):
        return self._specs[0]

    @property
    def events_spec(self):
        return self._specs[2]

    def inject_ttl_item(self, ttl_mins):
        self._create_and_get_special_item_spec(
            SpecInjector.TTL_ITEM_NAME,
            "every {:d} minute".format(ttl_mins))

    def get_ttl_item(self):
        return self.items_spec.get_item(self.get_ttl_item_name())

    def get_ttl_item_name(self):
        return '.'.join((SpecInjector.BUILTIN_APP_NAME,
                         SpecInjector.TTL_ITEM_NAME))

    def inject_audit_item(self, name, schedule):
        self._create_and_get_special_item_spec(
            f"__{name}_html_audit_item", schedule)

    def get_audit_item(self, name):
        return self.items_spec.get_item(self.get_audit_item_name(name))

    def get_audit_item_name(self, name):
        return '.'.join((SpecInjector.BUILTIN_APP_NAME,
                         f"__{name}_html_audit_item"))

    def get_config_item_spec(self):
        return self._create_and_get_special_item_spec(
            SpecInjector.CONFIG_ITEM_NAME, "0 0 1 */6 *")

    def get_config_item_name(self):
        return "{}.{}".format(
            SpecInjector.BUILTIN_APP_NAME,
            SpecInjector.CONFIG_ITEM_NAME)

    def add_ttl_trigger_spec(self, store):
        store.add_trigger_spec(
            TriggerSpecification(
                SpecInjector.TTL_TRIGGER,
                {"ttl_item_value": "${__builtin.__ttl_item['value']}"},
                "{ttl_item_value} == '1'")
            )

    def add_audit_triggers_spec(self, store):
        for name in config.properties['HTMLAuditReportSection']:
            trigger_name = self._get_audit_trigger_name(name)
            trigger_variables = {
                "html_audit_value":
                "${{__builtin.__{}_html_audit_item['value']}}".format(name)
                }
            trigger_condition = \
                "{html_audit_value} == '1'"
            store.add_trigger_spec(
                TriggerSpecification(
                    trigger_name, trigger_variables, trigger_condition))

    def inject_html_audit_report_events(self):
        for report_name in config.properties['HTMLAuditReportSection']:
            self._inject_html_audit_report_event(report_name)

    def _get_audit_trigger_name(self, name):
        return "__{}_html_audit_trigger".format(name)

    def _inject_html_audit_report_event(self, report_name):
        event_name = f"__{report_name}_html_audit_event"
        event_spec = {
            'start_level': 'critical',
            'escalation_freq': 99999,
            'condition': self._get_audit_trigger_name(report_name),
            'critical': ['audit_html_report'],
            'audit_html_report_notification': {
                'report': report_name,
                'subject': "Audit Report",
                'body': "{report}"
                }
            }
        self.events_spec.add_event_spec(event_name, event_spec)

    def _create_and_get_special_item_spec(self, name, schedule):
        app = self._get_or_create_app(SpecInjector.BUILTIN_APP_NAME)

        class SpecialItem:

            def items(self):
                return [(name, {
                    "schedule": schedule,
                    "commands": [{
                        "value": '''python3.7 -c "print(1)"'''
                    }]
                })]

        item_spec = self.items_spec.create_item(SpecialItem(), app)
        app.add_item(item_spec)
        return item_spec

    def _get_or_create_app(self, app_name):
        if app_name not in self.items_spec.applications:
            self.items_spec.applications[app_name] = Application(app_name)
        return self.items_spec.applications[app_name]


class Severity(IntEnum):
    CLEAR, MINOR, MAJOR, CRITICAL = range(4)

    def __str__(self):
        Severity._create_strings()
        return Severity.strings[self]

    @staticmethod
    def get_max_severity():
        return Severity.CRITICAL

    @staticmethod
    def create(value):
        if isinstance(value, Severity):
            return value

        value_uppercase = value.upper()
        Severity._create_strings()
        for k, v in Severity.strings.items():
            if v.upper() == value_uppercase:
                return k
        raise ValueError("Invalid severity: '{}'".format(value))

    @staticmethod
    def _create_strings():
        if getattr(Severity, "strings", None) is None:
            Severity.strings = {
                Severity.CLEAR: "clear",
                Severity.MINOR: "minor",
                Severity.MAJOR: "major",
                Severity.CRITICAL: "critical"
            }


class Action(IntEnum):
    TRAP, CLEAR, EMAIL, RESTART = range(4)

    def __str__(self):
        Action._create_strings()
        return Action.strings[self]

    @staticmethod
    def create(value):
        if isinstance(value, Action):
            return value

        value_uppercase = value.upper()
        Action._create_strings()
        for k, v in Action.strings.items():
            if v.upper() == value_uppercase:
                return k
        raise ValueError("Invalid action: '{}'".format(value))

    @staticmethod
    def _create_strings():
        if getattr(Action, "strings", None) is None:
            Action.strings = {
                Action.TRAP: "trap_action",
                Action.CLEAR: "clear_action",
                Action.EMAIL: "email_action",
                Action.RESTART: "restart_action"
            }


class DBType(Enum):
    POSTGRES_HIVE, POSTGRES_SDK, MARIADB = \
        ("postgres_hive", "postgres_sdk", "mariadb")

    def __str__(self):
        return self.value

    @staticmethod
    def create(value):
        if isinstance(value, DBType):
            return value

        for db_type in DBType:
            if db_type.value.upper() == value.upper():
                return db_type
        raise ValueError("Invalid db type: '{}'".format(value))
