'''
Created on 11-Dec-2019

@author: nageb
'''
from pathlib import Path
import subprocess
import shutil
import tempfile

from healthmonitoring.framework import config
from healthmonitoring.framework.store import Item
from healthmonitoring.framework.util import collection_util
from healthmonitoring.framework.util.specification import SpecificationUtil

from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class Monitor:

    def __init__(self, store, scheduler):
        self._store = store
        self._scheduler = scheduler

    def run(self):
        logger.debug("Adding jobs to scheduler...")
        self._add_jobs_to_scheduler()
        logger.debug("Running the scheduler...")
        self._scheduler.run()

    def _add_jobs_to_scheduler(self):
        for item_name in config.items_spec.get_all_enabled_item_names():
            item_spec = config.items_spec.get_item(item_name)
            self._scheduler.add_job(item_spec, CommandExecutor.execute_item)


class CommandExecutor:
    command_dictionary = {}

    @staticmethod
    def populate_commands_dictionary():
        for cmd_key, cmd in config.properties['Commands'].items():
            CommandExecutor.command_dictionary[cmd_key] = cmd

    @staticmethod
    def execute_item(item_spec):
        hosts = CommandExecutor._get_hosts(item_spec)
        dict_items = {}
        for host in hosts:
            var_dict = _HostCommandExecutor(
                host.address).execute_item(item_spec)
            dict_items[host.address] = CommandExecutor._convert_dict_to_item(
                item_spec, **var_dict)
        return dict_items

    @staticmethod
    def _get_hosts(item_spec):
        if item_spec.host == "localhost":
            hosts = [CommandExecutor._create_localhost()]
        elif '/' in item_spec.host:
            host = item_spec.host[0:item_spec.host.index("/")]
            hosts = [SpecificationUtil.get_host(host)]
        else:
            hosts = SpecificationUtil.get_hosts(item_spec.host)
        return hosts

    @staticmethod
    def _convert_dict_to_item(item_spec, **var_dict):
        return Item(item_spec, **var_dict)

    @staticmethod
    def _create_localhost():

        class Host:
            pass

        host = Host()
        host.address = "localhost"
        return host


class _HostCommandExecutor:

    def __init__(self, host_address):
        self._host_address = host_address
        self._var_dict = {}

    def execute_item(self, item_spec):
        self._var_dict = collection_util.convert_list_dict_to_dict(
            item_spec.fields.get('vars', {}))
        directory = _HostCommandExecutor._create_or_get_temp_directory(
            item_spec.name)
        for var_name, command in item_spec.commands.items():
            self._execute_command_on_host(var_name, command, directory)

        self._remove_file_fields()
        _HostCommandExecutor._remove_temp_directory(item_spec.name)
        return self._var_dict

    def _execute_command_on_host(self, var_name, command, directory):
        command_with_substitutions = _CommandSubstituter(
            var_name, command, self._host_address,
            self._var_dict).get_substituted_command()

        if _HostCommandExecutor.is_file_field(var_name):
            command_with_substitutions = _FileFieldHostCommandExecutor(
                self._var_dict, var_name, directory,
                command_with_substitutions).execute()
        else:
            _NonFileHostCommandExecutor(self._var_dict, var_name,
                                        command_with_substitutions,
                                        directory).execute()

        logger.debug("Command: %s", command_with_substitutions)
        logger.debug("Output: %s", self._var_dict[var_name])

    @staticmethod
    def is_file_field(field):
        return field.endswith("_file")

    def _remove_file_fields(self):
        keys = list(self._var_dict.keys())
        for key in keys:
            if _HostCommandExecutor.is_file_field(key):
                self._var_dict.pop(key)

    @staticmethod
    def _create_or_get_temp_directory(directory_name):
        directory = _HostCommandExecutor._get_temp_directory(directory_name)

        if not directory.exists():
            logger.debug("Creating temporary directory '%s'", str(directory))
            directory.mkdir()

        return directory

    @staticmethod
    def _get_temp_directory(directory_name):
        return Path(tempfile.gettempdir()) / ("dir_item_" + directory_name)

    @staticmethod
    def _remove_temp_directory(directory_name):
        directory = _HostCommandExecutor._get_temp_directory(directory_name)

        if directory.exists():
            logger.debug("Removing temporary directory '%s'", str(directory))
            shutil.rmtree(directory.resolve())


class _FileFieldHostCommandExecutor:

    def __init__(self, var_dict, var_name, directory, command):
        temp_filename = str(Path(directory / var_name).resolve())
        var_dict[var_name] = temp_filename
        self._command = "{} > {}".format(command, temp_filename)

    def execute(self):
        subprocess.getoutput(self._command)
        return self._command


class _NonFileHostCommandExecutor:

    def __init__(self, var_dict, var_name, command, directory):
        self._var_dict = var_dict
        self._var_name = var_name
        self._command = command
        self._directory = directory

    def execute(self):
        output = subprocess.getoutput(self._command)
        if _NonFileHostCommandExecutor._is_datastructure(output):
            self._var_dict[self._var_name] = eval(output, globals(), locals())
        else:
            self._var_dict[self._var_name] = output

    @staticmethod
    def _is_datastructure(output):
        return output.startswith("[") and output.endswith("]")


class _CommandSubstituter:

    def __init__(self, var_name, command, host_address, var_dict):
        self._var_name = var_name
        self._command = command
        self._host_address = host_address
        self._var_dict = var_dict

    def get_substituted_command(self):
        command = self._get_external_command()
        substituted_command = command.format(**self._var_dict).replace(
            "'", r"\'")

        if not self._should_be_locally_executed(self._var_name):
            substituted_command = r"""ssh {ssh_node} $'{command}'"""\
                .format(ssh_node=self._host_address,
                        command=substituted_command)

        return substituted_command

    def _get_external_command(self):
        if self._command.startswith('^'):
            return CommandExecutor.command_dictionary[self._command[1:]]
        return self._command

    def _should_be_locally_executed(self, var_name):
        return self._host_address == "localhost" \
            or _CommandSubstituter._is_local_command(var_name)

    @staticmethod
    def _is_local_command(var_name):
        return var_name.endswith("_localhost") or var_name.endswith(
            "_localhost_file")
