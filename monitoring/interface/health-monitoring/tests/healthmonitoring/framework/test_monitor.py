import os
from pathlib import Path
import tempfile
import unittest

from healthmonitoring.framework import config, runner
from healthmonitoring.framework.monitor import CommandExecutor, \
    _CommandSubstituter, _HostCommandExecutor
from healthmonitoring.framework.specification.yaml import \
    YamlItemsSpecificationReader, YamlHostSpecificationReader
from healthmonitoring.framework.store import Item
from healthmonitoring.framework.util import file_util
from mock import patch

from tests.healthmonitoring.framework.utils import TestUtil


def _execute_command(command):
    temp_dir = Path(tempfile.gettempdir()).resolve()
    dict_command = {
        r"""ssh h1 $'cmd1'""":
        "h1_cmd1_value1",
        r"""ssh h2 $'cmd1'""":
        "h2_cmd1_value1",
        r"""ssh h3 $'cmd1'""":
        "h3_cmd1_value1",
        r"""ssh h1 $'ss -nlp | grep 8091 | tr -s \' \' | gawk -F"," \'{print $2}\' | gawk -F"=" \'{print $2}\''""":  # noqa:E501
        "h1_cmd2_value2",
        r"""ssh h2 $'ss -nlp | grep 8091 | tr -s \' \' | gawk -F"," \'{print $2}\' | gawk -F"=" \'{print $2}\''""":  # noqa:E501
        "h2_cmd2_value2",
        r"""ssh h3 $'ss -nlp | grep 8091 | tr -s \' \' | gawk -F"," \'{print $2}\' | gawk -F"=" \'{print $2}\''""":  # noqa:E501
        "h3_cmd2_value2",
        r"""ssh h1 $'echo h1_cmd2_value2'""":
        "h1_cmd3_value3",
        r"""ssh h2 $'echo h2_cmd2_value2'""":
        "h2_cmd3_value3",
        r"""ssh h3 $'echo h3_cmd2_value2'""":
        "h3_cmd3_value3",
        r"""ssh h1 $'date +"%Y-%m-%s %H:%M:%S"'""":
        "h1_cmd4_value4",
        r"""ssh h2 $'date +"%Y-%m-%s %H:%M:%S"'""":
        "h2_cmd4_value4",
        r"""ssh h3 $'date +"%Y-%m-%s %H:%M:%S"'""":
        "h3_cmd4_value4",
        r"""ssh h4 $'cmd3'""":
        "h4_cmd3_value3",
        r"""ssh h4 $'cmd4'""":
        "h4_cmd4_value4",
        r"""ssh h5 $'cmd3'""":
        "h5_cmd3_value3",
        r"""ssh h5 $'cmd4'""":
        "h5_cmd4_value4",
        r"""ssh h4 $'wc -l {}{sep}dir_item_application3.item7{sep}var1_file'""".format(temp_dir, sep=os.path.sep):  # noqa: E501
        "36",
        r"""cal 2000 > {}{sep}dir_item_application3.item11{sep}var1_localhost_file""".format(temp_dir, sep=os.path.sep):  # noqa: E501
        "<file>",
        r"""wc -l {}{sep}dir_item_application3.item11{sep}var1_localhost_file""".format(temp_dir, sep=os.path.sep):  # noqa: E501
        "36",
        r"""ssh h4 $'cmd2'""":
        "h4_cmd2_value2",
        r"""ssh h5 $'cmd2'""":
        "h5_cmd2_value2",
        r"""cmd1""":
        "localhost_cmd1_value1",
        r"""cmd2""":
        "localhost_cmd2_value2",
        r"""cmd3""":
        "[{'name': 'COMMON_DIMENSION', 'dataUsed': 53.0}]",
    }

    return dict_command.get(command, "default_value")


@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
    return_value="./../../resources")
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts')
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
    return_value=0)
@patch('subprocess.getoutput', new=_execute_command)
class TestCommandExecutor(unittest.TestCase):

    TEMP_DIR = Path(tempfile.gettempdir()).resolve()

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()
        CommandExecutor.populate_commands_dictionary()

    def _setUp(self):
        file_util.under_test = True
        pathname = file_util.get_resource_path("hosts.yaml")
        self._host_spec = YamlHostSpecificationReader.read(pathname)
        pathname = file_util.get_resource_path(
            "standard/healthmonitoring/items.yaml")
        config.items_spec = YamlItemsSpecificationReader.read(pathname)
        runner.load_config(
            file_util.get_resource_path('monitoring.properties'))
        runner.load_all_specs(file_util.get_resource_path('.'))

    def test_execute_item_with_simple_cmd(self, mock_get_config_dir_path,
                                          mock_repopulate_hosts,
                                          mock_get_config_map_time_stamp):
        ITEM = 'application1.item1'
        expected = {
            "h1":
            Item(config.items_spec.get_item(ITEM),
                 port=8091,
                 var1='h1_cmd1_value1',
                 var2='h1_cmd2_value2',
                 var3='h1_cmd3_value3',
                 var4="h1_cmd4_value4")
        }
        actual = CommandExecutor.execute_item(config.items_spec.get_item(ITEM))
        for host in ['h1']:
            self.assertEqual(actual[host].params, expected[host].params)

    def test_execute_item_with_quotes_in_cmd(self, mock_get_config_dir_path,
                                             mock_repopulate_hosts,
                                             mock_get_config_map_time_stamp):
        ITEM = 'application1.item1'
        expected = {
            "h2":
            Item(config.items_spec.get_item(ITEM),
                 port=8091,
                 var1='h2_cmd1_value1',
                 var2='h2_cmd2_value2',
                 var3='h2_cmd3_value3',
                 var4="h2_cmd4_value4")
        }
        actual = CommandExecutor.execute_item(config.items_spec.get_item(ITEM))
        for host in ['h2']:
            self.assertEqual(actual[host].params, expected[host].params)

    def test_execute_item_with_variables_in_cmd(
            self, mock_get_config_dir_path, mock_repopulate_hosts,
            mock_get_config_map_time_stamp):
        ITEM = 'application1.item1'
        expected = {
            "h3":
            Item(config.items_spec.get_item(ITEM),
                 port=8091,
                 var1='h3_cmd1_value1',
                 var2='h3_cmd2_value2',
                 var3='h3_cmd3_value3',
                 var4="h3_cmd4_value4")
        }
        actual = CommandExecutor.execute_item(config.items_spec.get_item(ITEM))
        for host in ['h3']:
            self.assertEqual(actual[host].params, expected[host].params)

    def test_get_external_command(self, mock_get_config_dir_path,
                                  mock_repopulate_hosts,
                                  mock_get_config_map_time_stamp):
        command = '''^date'''
        expected = '''date +"%Y-%m-%s %H:%M:%S"'''
        actual = _CommandSubstituter(var_name=None,
                                     command=command,
                                     host_address=None,
                                     var_dict=None)._get_external_command()
        self.assertEqual(actual, expected)

    def test_execute_item_without_variables(self, mock_get_config_dir_path,
                                            mock_repopulate_hosts,
                                            mock_get_config_map_time_stamp):
        ITEM = 'application1.item1'
        expected = {
            "h4":
            Item(config.items_spec.get_item(ITEM),
                 var3='h4_cmd3_value3',
                 var4='h4_cmd4_value4'),
            "h5":
            Item(config.items_spec.get_item(ITEM),
                 var3='h5_cmd3_value3',
                 var4='h5_cmd4_value4')
        }
        actual = CommandExecutor.execute_item(
            config.items_spec.get_item('application1.item2'))
        for host in ['h4', 'h5']:
            self.assertEqual(actual[host].params, expected[host].params)

    def test_execute_item_with_file_field(self, mock_get_config_dir_path,
                                          mock_repopulate_hosts,
                                          mock_get_config_map_time_stamp):
        ITEM = 'application3.item7'
        expected = {"h4": Item(config.items_spec.get_item(ITEM), var2="36")}
        actual = CommandExecutor.execute_item(config.items_spec.get_item(ITEM))
        for host in ['h4']:
            self.assertEqual(actual[host].params, expected[host].params)

    def test_execute_item_on_localhost(self, mock_get_config_dir_path,
                                       mock_repopulate_hosts,
                                       mock_get_config_map_time_stamp):
        ITEM = 'application3.item6'
        expected = {
            "localhost":
            Item(config.items_spec.get_item(ITEM),
                 var1="localhost_cmd1_value1",
                 var2="localhost_cmd2_value2")
        }
        actual = CommandExecutor.execute_item(config.items_spec.get_item(ITEM))
        for host in ['localhost']:
            self.assertEqual(actual[host].params, expected[host].params)

    def test_execute_item_on_localhost_for_single_command(
            self, mock_get_config_dir_path, mock_repopulate_hosts,
            mock_get_config_map_time_stamp):
        ITEM = 'application3.item10'
        expected = {
            "h4":
            Item(config.items_spec.get_item(ITEM),
                 var1_localhost="localhost_cmd1_value1",
                 var2="h4_cmd2_value2"),
            "h5":
            Item(config.items_spec.get_item(ITEM),
                 var1_localhost="localhost_cmd1_value1",
                 var2="h5_cmd2_value2")
        }
        actual = CommandExecutor.execute_item(config.items_spec.get_item(ITEM))
        for host in ['h4', 'h5']:
            self.assertEqual(actual[host].params, expected[host].params)

    @patch.object(_HostCommandExecutor, '_remove_file_fields')
    def test_execute_item_with_file_field_on_localhost_for_single_command(
            self, mock_remove_file_fields, mock_get_config_dir_path,
            mock_repopulate_hosts, mock_get_config_map_time_stamp):
        ITEM = 'application3.item11'
        expected = {
            "h4":
            Item(
                config.items_spec.get_item(ITEM),
                var1_localhost_file=  # noqa: E251
                "{}{sep}dir_item_application3.item11{sep}var1_localhost_file".
                format(TestCommandExecutor.TEMP_DIR, sep=os.path.sep),
                var2_localhost="36")
        }
        actual = CommandExecutor.execute_item(config.items_spec.get_item(ITEM))
        actual['h4'].params['var1_localhost_file'] = actual['h4'].params[
            'var1_localhost_file']
        for host in ['h4']:
            self.assertEqual(actual[host].params, expected[host].params)

    def test_execute_item_with_dict_output(self, mock_get_config_dir_path,
                                           mock_repopulate_hosts,
                                           mock_get_config_map_time_stamp):
        ITEM = 'application3.item8'
        actual = CommandExecutor.execute_item(config.items_spec.get_item(ITEM))
        for host in ['localhost']:
            self.assertEqual(actual[host].params['var1'], [{
                'name': 'COMMON_DIMENSION',
                'dataUsed': 53.0
            }])

    def test_execute_item_with_any_host(self, mock_get_config_dir_path,
                                        mock_repopulate_hosts,
                                        mock_get_config_map_time_stamp):
        ITEM = 'application3.item9'
        expected = {
            "h1": Item(config.items_spec.get_item(ITEM), var1="h1_cmd1_value1")
        }
        actual = CommandExecutor.execute_item(config.items_spec.get_item(ITEM))
        for host in ['h1']:
            self.assertEqual(actual[host].params, expected[host].params)


if __name__ == "__main__":
    unittest.main()
