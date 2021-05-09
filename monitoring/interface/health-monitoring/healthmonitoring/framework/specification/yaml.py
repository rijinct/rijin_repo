'''
Created on 28-Apr-2020

@author: nageb
'''

import os

from healthmonitoring.framework import config
import yaml

from healthmonitoring.framework.specification.defs import HostSpecification, \
    ItemsSpecification, TriggersSpecification, EventsSpecification, \
    SpecificationCollection
from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class YamlMonitoringSpecificationReader:

    @staticmethod
    def read(pathname):
        with open(pathname) as file:
            spec = yaml.full_load(file)
            return spec['config']


class YamlHostSpecificationReader:

    @staticmethod
    def read(pathname):
        with open(pathname) as file:
            spec = yaml.full_load(file)
            return HostSpecification(spec['hostgroups'])


class YamlItemsSpecificationReader:

    @staticmethod
    def read(pathname):
        with open(pathname) as file:
            spec = yaml.full_load(file)
            return ItemsSpecification(spec['items'])


class YamlTriggersSpecificationReader:

    @staticmethod
    def read(pathname):
        with open(pathname) as file:
            spec = yaml.full_load(file)
            return TriggersSpecification(spec['triggers'])


class YamlEventsSpecificationReader:

    @staticmethod
    def read(pathname):
        with open(pathname) as file:
            spec = yaml.full_load(file)
            return EventsSpecification(spec['events'][0])


class YamlEventsSpecificationWriter:

    @staticmethod
    def write(events_spec, pathname):
        ds = YamlEventsSpecificationWriter._build_ds(events_spec)
        yaml_content = yaml.safe_dump(
            {'events': [ds]}, default_flow_style=False, sort_keys=False)
        with open(pathname, "w") as file:
            file.write(yaml_content)

    @staticmethod
    def _build_ds(events_spec):
        ds = {}
        for event_name, event_details in events_spec.get_events().items():
            ds[event_name] = YamlEventsSpecificationWriter.\
                _convert_event_details(event_details)

        return ds

    @staticmethod
    def _convert_event_details(event_details):
        ds = {}
        ds['start_level'] = str(event_details.start_level)
        ds['escalation_freq'] = event_details.escalation_freq
        ds['condition'] = event_details.condition

        actions = YamlEventsSpecificationWriter._populate_severity_actions(
            event_details, ds)
        YamlEventsSpecificationWriter._populate_notification_contents(
            event_details, ds, actions)

        if event_details.reset_triggers:
            ds['reset_triggers'] = event_details.reset_triggers

        ds['consolidated'] = event_details.consolidated
        ds['ttl_secs'] = event_details.ttl

        return ds

    @staticmethod
    def _populate_severity_actions(event_details, ds):
        actions = set()
        for severity in event_details.get_severities():
            severity_actions = event_details.get_severity_actions(severity)
            actions |= set(severity_actions)
            ds[str(severity)] = severity_actions

        return actions

    @staticmethod
    def _populate_notification_contents(event_details, ds, actions):
        for action in actions:
            notification = '{}_notification'.format(action)
            notification_content = event_details.get_notification(notification)
            if notification_content:
                ds[notification] = notification_content


class YamlSpecificationLoader:

    def __init__(self):
        self._specification_collection = None
        self._specification_collection_dict = {}

    def load_monitoring_specification(self, directory):
        monitoring_spec_pathname = os.path.abspath(
            os.path.join(directory, "monitoring.yaml"))
        logger.debug("Loading monitoring specification from '%s'...",
                     monitoring_spec_pathname)
        config.monitoring_spec = YamlMonitoringSpecificationReader.read(
            monitoring_spec_pathname)

    def load_hosts_specification(self, directory):
        hosts_spec_pathname = os.path.abspath(
            os.path.join(directory, "hosts.yaml"))
        logger.debug("Loading host specification from '%s'...",
                     hosts_spec_pathname)
        config.hosts_spec = YamlHostSpecificationReader.read(
            hosts_spec_pathname)

    def load(self, priority_directories, custom_directories,
             standard_directories):
        self._priority_directories = priority_directories
        self._custom_directories = custom_directories
        self._standard_directories = standard_directories
        self._specification_collection = None
        self._specification_collection_dict = {}

        self._load_specification_collection()
        self._assign_specifications()

    def _load_specification_collection(self):
        custom_directories_to_load = self._remove_priority_directories_from(
            self._custom_directories)
        standard_directories_to_load = self._remove_priority_directories_from(
            self._standard_directories)
        self._load_spec_from_directories(self._priority_directories)
        self._load_spec_from_directories(custom_directories_to_load)
        self._load_spec_from_directories(standard_directories_to_load)

    def _remove_priority_directories_from(self, directories):
        return set(directories) - set(self._priority_directories)

    def _load_spec_from_directories(self, directories):
        for directory in directories:
            logger.info(
                "Loading specification files from folder %s", directory)
            self._update_specification_collection(directory)

    def _update_specification_collection(self, current_directory):
        new_collection = YamlSpecificationCollectionLoader.load(
            current_directory)
        conflicting_directory = self._get_conflicting_directory(new_collection)
        if conflicting_directory:
            logger.info("Skipped %s because of conflict with %s",
                        current_directory, conflicting_directory)
        else:
            self._specification_collection_dict[current_directory] = \
                new_collection
        self._merge_specification_collection(new_collection)

    def _get_conflicting_directory(self, new_collection):
        for directory, existing_collection in \
                self._specification_collection_dict.items():
            if new_collection.conflicts_with(existing_collection):
                return directory
        return None

    def _merge_specification_collection(self, new_collection):
        if self._specification_collection:
            self._specification_collection.merge(new_collection)
        else:
            self._specification_collection = new_collection

    def _assign_specifications(self):
        config.items_spec = self._specification_collection.items_spec
        config.triggers_spec = self._specification_collection.triggers_spec
        config.events_spec = self._specification_collection.events_spec


class YamlSpecificationCollectionLoader:

    def __init__(self, directory):
        self._items_spec = self._load_items_spec(directory)
        self._triggers_spec = self._load_triggers_spec(directory)
        self._events_spec = self._load_events_spec(directory)

    @staticmethod
    def load(directory):
        loader = YamlSpecificationCollectionLoader(directory)
        return SpecificationCollection(
            loader._items_spec, loader._triggers_spec, loader._events_spec)

    def _load_items_spec(self, directory):
        items_spec_pathname = os.path.join(directory, "items.yaml")
        logger.debug("Loading item specification...")
        return YamlItemsSpecificationReader.read(items_spec_pathname)

    def _load_triggers_spec(self, directory):
        triggers_spec_pathname = os.path.join(directory, "triggers.yaml")
        logger.debug("Loading trigger specification...")
        return YamlTriggersSpecificationReader.read(triggers_spec_pathname)

    def _load_events_spec(self, directory):
        events_spec_pathname = os.path.join(directory, "events.yaml")
        logger.debug("Loading event specification...")
        return YamlEventsSpecificationReader.read(events_spec_pathname)
