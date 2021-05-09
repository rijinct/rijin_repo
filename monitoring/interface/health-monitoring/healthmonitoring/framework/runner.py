'''
Created on 17-Mar-2020

@author: nageb
'''
import os
from pathlib import Path

from healthmonitoring.framework import config
from healthmonitoring.framework.actor import Actor
from healthmonitoring.framework.analyzer import Analyzer
from healthmonitoring.framework.audit.input.yaml import YamlReader
from healthmonitoring.framework.audit.report import ReportStore
from healthmonitoring.framework.db.connection_factory import ConnectionFactory
from healthmonitoring.framework.monitor import Monitor, CommandExecutor
import healthmonitoring.framework.scheduler
from healthmonitoring.framework.specification.defs import DBType, SpecInjector
from healthmonitoring.framework.specification.yaml import \
    YamlSpecificationLoader
from healthmonitoring.framework.store import Store, Item
from healthmonitoring.framework.util.file_util import get_resource_path
from healthmonitoring.framework.util.loader import SpecificationLoader
from logger import Logger


logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


def load_config(path):
    path = os.path.abspath(path)
    logger.info("Loading configuration from '%s'", path)
    config.properties.read(path)
    CommandExecutor.populate_commands_dictionary()


def load_all_specs(path, base_dir="."):
    if not isinstance(path, Path):
        path = Path(path)
    if not isinstance(base_dir, Path):
        base_dir = Path(base_dir)
    standard_directories = _get_sub_directories(
        path / base_dir / config.properties.get(
            'DirectorySection', 'Standard'))
    custom_directories = _get_sub_directories(
        path / base_dir / config.properties.get('DirectorySection', 'Custom'))
    priority_directories = _get_priority_directories(path, base_dir)
    loader = YamlSpecificationLoader()
    SpecificationLoader.load_if_required()
    loader.load(priority_directories, custom_directories, standard_directories)
    SpecInjector().inject_ttl_item(
        int(config.properties.get("Consolidation", "ttl")))
    create_audit_reports(path)


def launch_workflow():
    analyzer = _create_analyzer()
    store = _create_store(analyzer)
    monitor = _create_monitor(store)
    load_db_driver()
    monitor.run()


def load_db_driver():
    db_type = os.environ.get('DB_TYPE')
    if db_type:
        return ConnectionFactory.instantiate_connection(
            DBType.create(db_type))


def create_audit_reports(base_dir):
    report_store = ReportStore()
    directory = (base_dir / Path(config.properties.get(
        'DirectorySection', 'AuditConfigDirectory'))).resolve()
    spec_injector = SpecInjector()
    for name, schedule in config.properties['HTMLAuditReportSection'].items():
        pathname = get_resource_path(directory / ("%s.yaml" % name))
        report_store.add(YamlReader(pathname).report)
        spec_injector.inject_audit_item(name, schedule)
    spec_injector.inject_html_audit_report_events()
    config.report_store = report_store


def _get_sub_directories(directory):
    sub_directories_path = []
    for sub_directory in os.listdir(directory):
        if os.path.isdir(os.path.join(directory, sub_directory)):
            sub_directories_path.append(os.path.join(directory, sub_directory))
    return sub_directories_path


def _get_priority_directories(path, base_dir):
    priority_directory = config.properties.get(
        'DirectorySection', 'PriorityDirectories').strip()
    priority_directories = []
    if priority_directory:
        for dir_relative_path in priority_directory.split(','):
            priority_directories.append(str(
                (path / base_dir / dir_relative_path).resolve()))
    return priority_directories


def _create_analyzer():
    actor = Actor()
    return Analyzer(actor, config.events_spec)


def _create_store(analyzer):
    store = Store()
    _load_config_in_store(store)
    _add_trigger_specs_to_store(store)
    store.analyzer = analyzer
    return store


def _load_config_in_store(store):
    config_item_spec = SpecInjector().get_config_item_spec()
    config_item = Item(config_item_spec, **config.monitoring_spec)
    store.add_item(config_item)


def _add_trigger_specs_to_store(store):
    for trigger_name in config.triggers_spec.get_all_trigger_names():
        trigger_spec = config.triggers_spec.get_trigger(trigger_name)
        store.add_trigger_spec(trigger_spec)

    spec_injector = SpecInjector()
    spec_injector.add_ttl_trigger_spec(store)
    spec_injector.add_audit_triggers_spec(store)


def _create_monitor(store):
    scheduler_name = config.properties["SchedulerSection"]["scheduler"]
    logger.debug("Using scheduler: '%s'", scheduler_name)
    scheduler = getattr(
        healthmonitoring.framework.scheduler, scheduler_name)(store)
    return Monitor(store, scheduler)
