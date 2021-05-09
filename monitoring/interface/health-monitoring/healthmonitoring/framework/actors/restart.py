'''
Created on 07-Feb-2020

@author: nageb
'''
import subprocess
import importlib

from logger import Logger

from healthmonitoring.framework import config
from healthmonitoring.framework.util.specification import SpecificationUtil

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class RestartActor:

    def act(self, notification_content):
        logger.info("Initiating RestartAction execution...")
        self._notification_content = notification_content
        self._service_name = notification_content['service_name']
        self.service_restarter = self._get_restarter()
        self.service_restarter.restart()

    @property
    def service_name(self):
        return self._service_name

    def _get_restarter(self):
        restart_service = self._get_restarter_by_name(self.service_name) or\
            self._get_restarter_by_name(self._get_type_of_service())
        logger.info("Using %s to restart %s",
                    restart_service.__class__.__name__, self.service_name)
        return restart_service

    def _get_restarter_by_name(self, name):
        restarter_class_name = "_{}ServiceRestarter".\
                format(name.capitalize())
        try:
            module = importlib.import_module(
                "healthmonitoring.framework.actors.restarters.{}"
                .format(name.lower()))
            restarter_class = getattr(module, restarter_class_name)
            return restarter_class(self._notification_content)
        except (ModuleNotFoundError, AttributeError):
            logger.debug(
                'Unable to fetch %s', restarter_class_name)

    def _get_type_of_service(self):
        return SpecificationUtil.get_hosts(self.service_name).category


class _ServiceRestarter:

    def __init__(self, notification_content):
        self._service_name = notification_content["service_name"]

    def restart(self):
        self._prepare_command()
        self._run_command()

    def _prepare_command(self):
        self.command_to_run = 'ssh {} "{} --service {} --level {}"'.format(
            self.get_node(), self.script_path, self._service_name.lower(),
            self.get_type_of_operation().lower())

    def get_type_of_operation(self):
        if SpecificationUtil.get_host(self._service_name).ha:
            return "stop"
        else:
            return "restart"

    def _run_command(self):
        p = subprocess.Popen(self.command_to_run,
                             stdout=subprocess.PIPE,
                             shell=True)
        (output, error) = p.communicate()
        return error

    def get_node(self):
        return SpecificationUtil.get_host(
            self._service_name).address
