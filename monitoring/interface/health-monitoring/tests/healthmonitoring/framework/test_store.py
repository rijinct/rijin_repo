'''
Created on 12-Dec-2019

@author: nageb
'''
import os
import shutil
import unittest

from healthmonitoring.framework import config
from healthmonitoring.framework.db.connection import DBConnection
from healthmonitoring.framework.db.connection_factory import ConnectionFactory
from healthmonitoring.framework.specification.defs import (
    ItemSpecification, Application, TriggerSpecification, CSVDetails,
    SpecInjector)
from healthmonitoring.framework.store import (
    Store, Item, Trigger, _CSVWriter, _TriggerGenerator, _JsonConvertor)
from healthmonitoring.framework.util import file_util
from mock import patch
from mock.mock import Mock

import healthmonitoring
from tests.healthmonitoring.framework.utils import TestUtil


class TestItem(unittest.TestCase):

    PARAMS = {"k1": "v1"}

    def setUp(self):
        self.item_spec = ItemSpecification("item1", "host", {"var1": "cmd1"},
                                           "* * * * *", Application("app1"))
        self.item = Item(self.item_spec, **TestItem.PARAMS)

    def test_get_item(self):
        self.assertEqual(self.item.item, self.item_spec)

    def test_get_name(self):
        self.assertEqual(self.item.name, "app1.item1")

    def test_get_params(self):
        self.assertEqual(self.item.params, TestItem.PARAMS)


class DummyAnalyzer():

    def __init__(self):
        self.called = False

    def inject_trigger(self, trigger):
        self.called = True


class TestTrigger(unittest.TestCase):
    TRIGGER_NAME = "trigger1"
    APPLICATION_NAME = "app1"
    generated_triggers = []

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def test_trigger_creation_and_get_name(self):
        trigger_spec = TriggerSpecification(TestTrigger.TRIGGER_NAME,
                                            {'var1': '${app1.item1}'},
                                            "{var1} > 100")
        trigger = Trigger(trigger_spec)
        self.assertEqual(trigger.name, TestTrigger.TRIGGER_NAME)

    def test_trigger_set_and_get_item(self):
        item = Item(ItemSpecification("item", "host", {}, "", "app"))
        trigger_spec = TriggerSpecification(TestTrigger.TRIGGER_NAME,
                                            {'var1': '${app1.item1}'},
                                            "{var1} > 100")
        trigger = Trigger(trigger_spec)
        trigger.item = item
        self.assertEqual(trigger.item, item)

    def _create_trigger_without_params(self, value):
        TRIGGER_SPEC = "{var1} > 100"
        trigger_variables = {
            'var1': "${app1.item1['value']}"
            }
        trigger_spec = TriggerSpecification(
            TestTrigger.TRIGGER_NAME, trigger_variables, TRIGGER_SPEC)
        app = Application(TestTrigger.APPLICATION_NAME)
        item_spec = ItemSpecification("item1", "host1", {
            'var1': 'cmd1',
            'var2': 'cmd2'
        }, "", app)
        app.add_item(item_spec)
        item = Item(item_spec, value=value)
        self.analyzer = DummyAnalyzer()
        self.store = Store()
        self.store.analyzer = self.analyzer
        self.store.add_trigger_spec(trigger_spec)
        self.store.add_item(item)

    def test_trigger_evaluate_when_no_trigger_is_expected(self):
        self._create_trigger_without_params(60)
        self.assertFalse(
            self.store.get_trigger(TestTrigger.TRIGGER_NAME).state)
        self.assertFalse(self.analyzer.called)

    def test_trigger_evaluate_when_trigger_is_expected(self):
        self._create_trigger_without_params(160)
        self.assertTrue(self.store.get_trigger(TestTrigger.TRIGGER_NAME).state)
        self.assertTrue(self.analyzer.called)

    def _test_sequence(self, value, expected_state, expected_called):
        self.store.add_item(Item(self.item_spec, value=value))
        trigger = self.store.get_trigger(TestTrigger.TRIGGER_NAME)
        self.assertEqual(trigger.state, expected_state)
        self.assertEqual(self.analyzer.called, expected_called)
        self.analyzer.called = False

    def test_trigger_generation_when_items_fired_in_sequence(self):
        TRIGGER_SPEC = "{var1} > 100"
        trigger_variables = {
            'var1': "${app1.item1['value']}"
            }
        trigger_spec = TriggerSpecification(
            TestTrigger.TRIGGER_NAME, trigger_variables, TRIGGER_SPEC)
        app = Application(TestTrigger.APPLICATION_NAME)
        self.item_spec = ItemSpecification("item1", "host1", {
            'var1': 'cmd1',
            'var2': 'cmd2'
        }, "", app)
        self.analyzer = DummyAnalyzer()
        self.store = Store()
        self.store.analyzer = self.analyzer
        self.store.add_trigger_spec(trigger_spec)

        # 00 = no calls
        self._test_sequence(40, False, False)

        # 01 = call
        self._test_sequence(140, True, True)

        # 11 = call
        self._test_sequence(240, True, True)

        # 10 = call
        self._test_sequence(40, False, True)

        # 00 = no call
        self._test_sequence(40, False, False)

    def _create_trigger_with_params(self, params):
        TRIGGER_SPEC = "{var1} > 0 and ({var2} == 1 or {var2} == 2 and {var3} == 11)"  # noqa: 501
        trigger_variables = {
            "var1": "${app1.item2['count']}",
            "var2": "${app1.item2['type']}",
            "var3": "${app1.item2['status']['execution']['duration']}"
            }

        trigger_spec = TriggerSpecification(
            TestTrigger.TRIGGER_NAME, trigger_variables, TRIGGER_SPEC)
        app = Application(TestTrigger.APPLICATION_NAME)
        item_spec = ItemSpecification("item2", "host2", {
            'var3': 'cmd3',
            'var4': 'cmd4'
        }, "", app)
        app.add_item(item_spec)
        item = Item(item_spec, **params)
        self.analyzer = DummyAnalyzer()
        self.store = Store()
        self.store.analyzer = self.analyzer
        self.store.add_trigger_spec(trigger_spec)
        self.store.add_item(item)

    def _create_trigger_with_params_across_2_items(self, params):
        TRIGGER_SPEC = "{var1} > 0 and ({var2} == 1 or {var3} == 2)"  # noqa: 501
        trigger_variables = {
            "var1": "${app1.item2['count']}",
            "var2": "${app1.item2['type']}",
            "var3": "${app1.item2['type']}"
            }
        trigger_spec = TriggerSpecification(
            TestTrigger.TRIGGER_NAME, trigger_variables, TRIGGER_SPEC)
        app = Application(TestTrigger.APPLICATION_NAME)
        item_spec = ItemSpecification("item1", "host2", {
            'var3': 'cmd3',
            'var4': 'cmd4'
        }, "", app)
        app.add_item(item_spec)
        item_spec = ItemSpecification("item2", "host2", {
            'var3': 'cmd3',
            'var4': 'cmd4'
        }, "", app)
        app.add_item(item_spec)
        item = Item(item_spec, **params)
        self.analyzer = DummyAnalyzer()
        self.store = Store()
        self.store.analyzer = self.analyzer
        self.store.add_trigger_spec(trigger_spec)
        self.store.add_item(item)

    def test_trigger_evaluate_with_params_when_no_trigger_is_expected(self):
        self._create_trigger_with_params({
            "count": 10,
            "type": 3,
            "status": {
                "execution": {
                    "duration": 11}
                }
            })
        self.assertFalse(
            self.store.get_trigger(TestTrigger.TRIGGER_NAME).state)
        self.assertFalse(self.analyzer.called)

    def test_trigger_evaluate_with_params_when_trigger_is_expected(self):
        self._create_trigger_with_params({
            "count": 10,
            "type": 1,
            "status": {
                "execution": {
                    "duration": 11}
                }
            })
        self.assertTrue(self.store.get_trigger(TestTrigger.TRIGGER_NAME).state)
        self.assertTrue(self.analyzer.called)

    def test_trigger_evaluate_with_params_when_value_is_0(self):
        self._create_trigger_with_params({
            "count": 0,
            "type": 1,
            "status": {
                "execution": {
                    "duration": 11}
                }
            })
        self.assertFalse(
            self.store.get_trigger(TestTrigger.TRIGGER_NAME).state)
        self.assertFalse(self.analyzer.called)

    def test_trigger_evaluate_with_multiple_items(self):
        self._create_trigger_with_params_across_2_items({
            "count": 10,
            "type": 1
        })
        self.assertTrue(self.store.get_trigger(TestTrigger.TRIGGER_NAME).state)
        self.assertTrue(self.analyzer.called)

    def test_trigger_evaluate_with_dict(self):
        params = {
            "var1": [{
                'name': 'DIMENSION1',
                'dataUsed': 53
            }, {
                'name': 'DIMENSION2',
                'dataUsed': 83
            }, {
                'name': 'DIMENSION3',
                'dataUsed': ''
            }, {
                'name': 'DIMENSION3',
                'dataUsed': 83,
                'depth1': [{
                    "depth2": 90
                    }]
            }]
        }
        trigger_spec = TriggerSpecification(
            TestTrigger.TRIGGER_NAME,
            {"data_used": "${app1.item8['var1']['dataUsed']}"},
            "int({data_used} or 0) > 50")
        app = Application(TestTrigger.APPLICATION_NAME)
        item_spec = ItemSpecification("item8", "localhost", {'var1': ""}, "",
                                      app)
        app.add_item(item_spec)
        item = Item(item_spec, **params)
        self.store = Store()
        self.analyzer = DummyAnalyzer()
        self.store.analyzer = self.analyzer
        self.store.add_trigger_spec(trigger_spec)
        self.store.add_item(item)
        self.trigger = Trigger(trigger_spec)

        value = self.trigger._get_replacement_value_for_parameterized_item(
            self.store, "app1.item8['var1'][0]['dataUsed']")
        self.assertEqual(value, 53)

        value = self.trigger._get_replacement_value_for_parameterized_item(
            self.store, "app1.item8['var1'][1]['dataUsed']")
        self.assertEqual(value, 83)

        value = self.trigger._get_replacement_value_for_parameterized_item(
            self.store, "app1.item8['var1'][2]['dataUsed']")
        self.assertEqual(value, "''")

        value = self.trigger._get_replacement_value_for_parameterized_item(
            self.store, "app1.item8['var1'][3]['depth1'][0]['depth2']")
        self.assertEqual(value, 90)

    def test_trigger_generation_with_loading_values_from_config(self):
        TRIGGER_SPEC = "{var1} > {threshold}"
        trigger_variables = {
            'var1': "${app1.item1['value']}",
            'threshold': "${__builtin.config['SUMMARYTHRESHOLDS']['WeekJob']['value']}"  # noqa: 501
            }
        trigger_spec = TriggerSpecification(
            TestTrigger.TRIGGER_NAME, trigger_variables, TRIGGER_SPEC)
        app = Application(TestTrigger.APPLICATION_NAME)
        self.item_spec = ItemSpecification("item1", "host1", {
            'var1': 'cmd1',
            'var2': 'cmd2'
        }, "", app)
        self.analyzer = DummyAnalyzer()
        self.store = Store()
        self._load_config_item_to_store()
        self.store.analyzer = self.analyzer
        self.store.add_trigger_spec(trigger_spec)
        self._test_sequence(140, True, True)

    def test_trigger_generation_for_key_having_braces(self):
        TRIGGER_SPEC = "{var1} > 0 and ({var2} == 1 or {var2} == 2 and {var3} == 11)"  # noqa: 501
        trigger_variables = {
            "var1": "${app1.item2['count(/mnt/)']}",
            "var2": "${app1.item2['type']}",
            "var3": "${app1.item2['status']['execution']['duration']}"
            }
        params = {
            "count(/mnt/)": 10,
            "type": 1,
            "status": {
                "execution": {
                    "duration": 11}
                }
            }

        trigger_spec = TriggerSpecification(
            TestTrigger.TRIGGER_NAME, trigger_variables, TRIGGER_SPEC)
        app = Application(TestTrigger.APPLICATION_NAME)
        item_spec = ItemSpecification("item2", "host2", {
            'var3': 'cmd3',
            'var4': 'cmd4'
        }, "", app)
        app.add_item(item_spec)
        item = Item(item_spec, **params)
        self.analyzer = DummyAnalyzer()
        self.store = Store()
        self.store.analyzer = self.analyzer
        self.store.add_trigger_spec(trigger_spec)
        self.store.add_item(item)
        self.assertTrue(self.store.get_trigger(TestTrigger.TRIGGER_NAME).state)
        self.assertTrue(self.analyzer.called)

    @staticmethod
    def _create_item_rows(key, values):
        return [{
            'name': 'COMMON_DIMENSION1',
            key: values[0]
        }, {
            'name': 'COMMON_DIMENSION2',
            key: values[1]
        }, {
            'name': 'COMMON_DIMENSION3',
            key: values[2]
        }]

    def _setup_param_test(self, key, numbers):
        params = {"hour_agg_dict_ds": self._create_item_rows(key, numbers)}
        trigger_variables = {
            "data_used":
            "${job_monitoring.hour_aggregations['hour_agg_dict_ds']['dataUsed']}"  # noqa: 501
        }
        trigger_spec = TriggerSpecification(
            TestTrigger.TRIGGER_NAME,
            trigger_variables, "{data_used} > 50")
        self._initiate_test(params, trigger_spec)

    def _setup_param_test_dict_inside_dict(self, key, dict):
        params = {"hour_agg_dict_ds": self._create_item_rows(key, dict)}
        trigger_variables = {
            "executionduration":
            "${job_monitoring.hour_aggregations['hour_agg_dict_ds']['status']['executionduration']}"  # noqa: 501
        }
        trigger_spec = TriggerSpecification(
            TestTrigger.TRIGGER_NAME,
            trigger_variables, "{executionduration} > 50")
        self._initiate_test(params, trigger_spec)

    def _initiate_test(self, params, trigger_spec):
        app = Application("job_monitoring")
        item_spec = ItemSpecification("hour_aggregations", "localhost",
                                      {'hour_agg_dict_ds': ""}, "", app)
        app.add_item(item_spec)
        item = Item(item_spec, **params)
        self.store = Store()
        self.analyzer = DummyAnalyzer()
        self.store.analyzer = self.analyzer
        self.store.add_trigger_spec(trigger_spec)

        def _generate_trigger(trigger, item):
            self.store._trigger_generator._trigger_generated = True
            TestTrigger.generated_triggers.append((trigger, item))

        TestTrigger.generated_triggers = []
        self.store._trigger_generator._generate_trigger = _generate_trigger

        self.store.add_item(item)

    def _assert_trigger_name_present(self, name):
        return self.assertTrue(
            name in
            [trigger[0].name for trigger in TestTrigger.generated_triggers])

    def _get_trigger(self, trigger_name):
        for trigger, item in TestTrigger.generated_triggers:
            if trigger.name == trigger_name:
                return trigger

    def test_trigger_evaluate_with_array_and_single_trigger(self):
        self._setup_param_test("dataUsed", (53, 40, 3))
        self.assertEqual(len(TestTrigger.generated_triggers), 2)
        self._assert_trigger_name_present("trigger1_0")

    def test_trigger_evaluate_with_array_and_multiple_trigger(self):
        self._setup_param_test("dataUsed", (53, 40, 300))
        self.assertEqual(len(TestTrigger.generated_triggers), 3)
        self._assert_trigger_name_present("trigger1_0")
        self._assert_trigger_name_present("trigger1_2")

    def test_trigger_consolidation_of_2_triggers(self):
        self._setup_param_test("dataUsed", (53, 40, 300))
        self.assertEqual(len(TestTrigger.generated_triggers), 3)
        self._assert_trigger_name_present("trigger1")
        self.assertTrue(self._get_trigger("trigger1").state)

    def test_trigger_evaluate_with_array_and_multiple_depths(self):
        self._setup_param_test_dict_inside_dict(
            "status", ({'executionduration': 54},
                       {'executionduration': 21},
                       {'executionduration': 78}))
        self.assertEqual(len(TestTrigger.generated_triggers), 3)
        self._assert_trigger_name_present("trigger1_0")
        self._assert_trigger_name_present("trigger1_2")

    @patch('healthmonitoring.framework.analyzer.Analyzer')
    def test_generation_of_ttl_trigger(self, MockAnalyzer):
        spec_injector = SpecInjector()
        spec_injector.inject_ttl_item(0)
        store = Store()
        spec_injector.add_ttl_trigger_spec(store)

        item = Item(spec_injector.get_ttl_item(), **{'value': '1'})
        trigger_name = SpecInjector.TTL_TRIGGER
        trigger_variables = {
            "ttl_item_value": "${__builtin.__ttl_item['value']}"
            }
        trigger_condition = "{ttl_item_value} == '1'"
        self._assert_generates_trigger(
            store, item, trigger_name, trigger_variables,
            trigger_condition, MockAnalyzer)

    @patch('healthmonitoring.framework.analyzer.Analyzer')
    def test_generation_of_audit_trigger(self, MockAnalyzer):
        name = 'standard'
        spec_injector = SpecInjector()
        spec_injector.inject_audit_item(name, 'every minute')
        store = Store()
        spec_injector.add_audit_triggers_spec(store)

        item = Item(spec_injector.get_audit_item(name), **{'value': '1'})
        trigger_name = "__{}_html_audit_trigger".format(name)
        trigger_variables = {
            "html_audit_value":
            "${{__builtin.__{}_html_audit_item['value']}}".format(name)
           }
        trigger_condition = "{html_audit_value} == '1'"
        self._assert_generates_trigger(
            store, item, trigger_name, trigger_variables,
            trigger_condition, MockAnalyzer)

    def _assert_generates_trigger(
            self, store, item, trigger_name, trigger_variables,
            trigger_condition, MockAnalyzer):
        store.analyzer = MockAnalyzer
        store.add_item(item)
        expected_trigger = Trigger(TriggerSpecification(
            trigger_name, trigger_variables, trigger_condition))
        expected_trigger.state = True
        MockAnalyzer.inject_trigger.assert_called_with(expected_trigger)

    def _prepare_real_world_scenario1(self, trigger_spec):
        file_util.under_test = True
        output = [{
            'status': 'E',
            'start_time': '2020-04-01 16:13:43',
            'time_frame': '15Hour===16Hour',
            'end_time': '',
            'job_name': 'Perf_DATA_CP_NE_1_1_HOUR_AggregateJob',
            'executionduration': ''
        }, {
            'status': 'R',
            'start_time': '2020-04-01 16:14:54',
            'time_frame': '15Hour===16Hour',
            'end_time': '',
            'job_name': 'Perf_CEI2_SMS_1_HOUR_AggregateJob',
            'executionduration': 20
        }, {
            'status': 'R',
            'start_time': '2020-04-01 16:16:40',
            'time_frame': '15Hour===16Hour',
            'end_time': '',
            'job_name': 'Perf_SMS_DP_ROLLUP_CITY_1_HOUR_AggregateJob',
            'executionduration': 60
        }]
        params = {"hour_jobs_status": output}
        app = Application("AggregationJobMonitoring")
        header = "time_frame,job_name,start_time,end_time,executionduration,status"  # noqa: 501
        directory = "aggregations"
        filename = "hourAggregations_DATE.csv"
        csv_details = CSVDetails(header, filename, directory=directory)
        item_spec = ItemSpecification("hour_aggregations", "localhost",
                                      {'hour_jobs_status': ""}, "", app,
                                      csv_details)
        app.add_item(item_spec)
        item = Item(item_spec, **params)
        self.store = Store()
        self.analyzer = DummyAnalyzer()
        self.store.analyzer = self.analyzer
        self.store.add_trigger_spec(trigger_spec)

        def _generate_trigger(trigger, item):
            self.store._trigger_generator._trigger_generated = True
            TestTrigger.generated_triggers.append((trigger, item))

        TestTrigger.generated_triggers = []
        self.store._trigger_generator._generate_trigger = _generate_trigger

        self.store.add_item(item)
        shutil.rmtree(file_util.get_resource_path(directory))

    def test_real_world_scenario1_test1(self):

        trigger_variables = {
            "execution_duration":
            "${aggregation_jobs_monitoring.hour_aggregations['hour_jobs_status']['executionduration']}",  # noqa: 501
            "execution_duration_int":
            "${int(AggregationJobMonitoring.hour_aggregations['hour_jobs_status']['executionduration'] or 0)}"  # noqa: 501
            }

        trigger_condition = "{execution_duration_int} != '' and {executionduration} > 30"  # noqa: 501

        trigger_spec = TriggerSpecification(
            "hour_execution_duration_breached_trigger",
            trigger_variables,
            trigger_condition
        )
        self._prepare_real_world_scenario1(trigger_spec)

    def test_real_world_scenario1_test2(self):
        TRIGGER_NAME = "hour_aggregation_failed_trigger"
        trigger_variables = {
            'job_status':
            "${AggregationJobMonitoring.hour_aggregations['hour_jobs_status']['status']}"  # noqa: 501
            }
        trigger_spec = TriggerSpecification(
            TRIGGER_NAME,
            trigger_variables,
            "{job_status} == 'E'"
        )
        self._prepare_real_world_scenario1(trigger_spec)
        self._assert_trigger_name_present(TRIGGER_NAME)

    def _load_config_item_to_store(self):
        item_spec = SpecInjector().get_config_item_spec()
        config_item = Item(item_spec, **config.monitoring_spec)
        self.store.add_item(config_item)


class TestStore(unittest.TestCase):
    header = "Date, Service Name, Last Restart Time, Uptime"
    filename = "item.csv"
    csv_details = CSVDetails(header, filename)

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def setUp(self):
        self.store = Store()

    def test_add_and_get_item(self):
        item1 = Item(ItemSpecification("item1", "* * * * *", "host1", {
            'var1': 'cmd1',
            'var2': 'cmd2'
        }, Application("app1")),
                     value=60)
        item2 = Item(ItemSpecification("item2", "* * * * *", "host2", {
            'var3': 'cmd3',
            'var4': 'cmd4'
        }, Application("app1")),
                     value=60)
        self.store.add_item(item1)
        self.store.add_item(item2)
        self.assertEqual(len(self.store._items), 2)
        self.assertEqual(self.store.get_item("app1.item1"), item1)
        self.assertEqual(self.store.get_item("app1.item2"), item2)

    def test_get_list_param_name(self):
        item1 = Item(ItemSpecification("item1", "* * * * *", "host1", {},
                                       Application("app1")),
                     ds=[{
                         "key": "value"
                     }])
        self.store.add_item(item1)
        trigger_variables = {
            "ds_key": "${app1.item1['ds']['key']}"
            }
        actual = self.store._trigger_generator._get_list_param_names(
            trigger_variables)
        self.assertEqual(actual, ["['ds']"])

    def test_get_length_of_param_ds(self):
        numbers = (43, 40, 300)
        params = {"abc_ds": TestTrigger._create_item_rows("dataUsed", numbers)}
        item_spec = ItemSpecification("item1", "localhost", {'abc_ds': ""}, "",
                                      Application("app1"))
        item = Item(item_spec, **params)
        length = _TriggerGenerator._get_length_of_param_ds(item, "['abc_ds']")
        self.assertEqual(length, 3)

    @patch('healthmonitoring.framework.store.ConnectionFactory')
    @patch('healthmonitoring.framework.store._CSVWriter')
    @patch('healthmonitoring.framework.store._JsonConvertor')
    def test_add_item(
            self, MockJsonConvertor,
            MockCsvWriter, MockConnectionFactory):
        mock_connection = Mock()
        MockConnectionFactory.get_connection.return_value = mock_connection
        healthmonitoring.framework.store._JsonConvertor = MockJsonConvertor
        item = Item(ItemSpecification("item", "* * * * *", "host1", {
            'var1': 'cmd1',
            'var2': 'cmd2'
        }, Application("app1"), TestStore.csv_details, db_category="test"),
                    value=60)
        store = Store()
        store.add_item(item)
        MockCsvWriter().write.assert_called_once()
        MockJsonConvertor.convert_csv_to_jsons.assert_called_once()

    @patch('healthmonitoring.framework.store._CSVWriter')
    @patch('healthmonitoring.framework.store._JsonConvertor')
    def test_add_item_with_exception(self, MockJsonConvertor, MockCsvWriter):
        DBConnection._instance = None
        healthmonitoring.framework.db.postgres.hosts_spec = \
            config.hosts_spec
        item = Item(ItemSpecification("item", "* * * * *", "host1", {
            'var1': 'cmd1',
            'var2': 'cmd2'
        }, Application("app1"), TestStore.csv_details),
                    value=60)
        store = Store()
        store.add_item(item)
        MockCsvWriter().write.assert_called_once()
        MockJsonConvertor.convert_csv_to_jsons.assert_not_called()
        self.assertEqual(ConnectionFactory.get_connection(), None)

    def test_is_list_param_when_not_list(self):
        item = Item(ItemSpecification("item1", "* * * * *", "host1", {
            'var1': 'cmd1',
            'var2': 'cmd2'
        }, Application("app1")),
                    var1=60)
        self.store.add_item(item)
        self.assertFalse(
            self.store._trigger_generator._is_list_param(
                "app1.item1", "['var1']"))

    def test_is_list_param_when_list_present(self):
        params = {"ds": TestTrigger._create_item_rows("dataUsed", (1, 2, 3))}
        item = Item(
            ItemSpecification("item1", "every min", "localhost", {'ds': ""},
                              Application("app1")), **params)
        self.store.add_item(item)
        self.assertTrue(
            self.store._trigger_generator._is_list_param(
                "app1.item1", "['ds']"))


class TestJsonConvertor(unittest.TestCase):

    def setUp(self):
        file_util.under_test = True

    def test_convert_csv_output_to_json(self):
        config.properties.read(
            file_util.get_resource_path('monitoring.properties'))
        filename = "serviceStability_DATE.csv"
        header = TestCSVWriter.HEADER.rstrip("\n")
        csv_details = CSVDetails(header,
                                 filename,
                                 header_always=True,
                                 directory=None)
        item_spec = ItemSpecification("item1", "host", {"var1": "cmd1"}, "",
                                      Application("ServiceStabilityTest"),
                                      csv_details)
        output = [{
            "Date": "today",
            "Host": "myhost",
            "Service": "myservice",
            "Last_Restart_Time": "now"
        }]
        params = {'var1': output}
        item = Item(item_spec, **params)
        csv_writer = _CSVWriter(item)
        csv_writer.write()
        directory = file_util.get_resource_path("ServiceStability")
        expected = [
            '{"Date": "today", "Host": "myhost", "Service": "myservice", "Last_Restart_Time": "now"}'  # noqa: 501
        ]
        self._pathname = os.path.join(directory, filename)
        self._json_convertor = _JsonConvertor.convert_csv_to_jsons(
            self._pathname)
        self.assertEqual(self._json_convertor, expected)
        shutil.rmtree(directory)


class TestCSVWriter(unittest.TestCase):
    HEADER = "Date,Host,Service,Last_Restart_Time"
    LINE = "today,myhost,myservice,now"
    HEADER_AND_LINE = "{}\n{}\n".format(HEADER, LINE)
    FILENAME = "serviceStability_DATE.csv"
    DIRECTORY = "ServiceStability"
    HEADER_ALWAYS = True
    CSV_DETAILS = CSVDetails(header=HEADER,
                             filename=FILENAME,
                             header_always=HEADER_ALWAYS,
                             directory=DIRECTORY)

    def setUp(self):
        file_util.under_test = True
        config.properties.read(
            file_util.get_resource_path('monitoring.properties'))
        item_spec = ItemSpecification("item1", "host", {"var1": "cmd1"}, "",
                                      Application("ServiceStability"),
                                      TestCSVWriter.CSV_DETAILS)
        DATE = "today"
        HOST = "myhost"
        SERVICE = "myservice"
        LAST_RESTART_TIME = "now"
        output = [{
            "Date": DATE,
            "Host": HOST,
            "Service": SERVICE,
            "Last_Restart_Time": LAST_RESTART_TIME
        }]
        params = {'var1': output}
        item = Item(item_spec, **params)
        self._csv_writer = _CSVWriter(item)
        self._directory = file_util.get_resource_path("ServiceStability")
        self._filename = "serviceStability_DATE.csv"
        self._pathname = os.path.join(self._directory, self._filename)

    def _create_directory_if_needed(self):
        if not os.path.exists(self._directory):
            os.makedirs(self._directory)

    def _delete_directory_if_exists(self):
        if os.path.exists(self._directory):
            shutil.rmtree(self._directory)

    def _get_file_contents(self):
        with open(self._pathname) as file:
            return file.read()

    def _write_to_file(self, content):
        with open(self._pathname, "a") as file:
            file.write(content)

    def _delete_file_if_exists(self):
        try:
            os.unlink(self._pathname)
        except (IsADirectoryError, FileNotFoundError):
            pass

    def test_create_or_get_directory_(self):
        self._delete_directory_if_exists()

        directory = self._csv_writer._create_or_get_directory()

        self.assertTrue(os.path.exists(self._directory))
        self.assertEqual(directory, self._directory)
        self._delete_directory_if_exists()

    def test_write_header_always_on_missing_file(self):
        self._create_directory_if_needed()
        self._delete_file_if_exists()

        self._csv_writer.write()
        self.assertEqual(
            self._get_file_contents(),
            TestCSVWriter.HEADER_AND_LINE)
        self._delete_directory_if_exists()

    def test_write_header_always_on_existing_file(self):
        EXISTING_CONTENT = "line1\nline2\nline3\n"
        self._create_directory_if_needed()
        self._delete_file_if_exists()
        self._write_to_file(EXISTING_CONTENT)

        self._csv_writer.write()
        self.assertEqual(
            self._get_file_contents(),
            EXISTING_CONTENT + TestCSVWriter.HEADER_AND_LINE)
        self._delete_directory_if_exists()

    def test_write_header_if_required_on_missing_file(self):
        self._create_directory_if_needed()
        self._delete_file_if_exists()

        self._csv_writer.write()
        self.assertEqual(
            self._get_file_contents(), TestCSVWriter.HEADER_AND_LINE)
        self._delete_directory_if_exists()

    def test_write_header_if_required_on_existing_file(self):
        EXISTING_CONTENT = "line1\nline2\nline3"
        self._create_directory_if_needed()
        self._delete_file_if_exists()
        self._write_to_file(EXISTING_CONTENT)

        self._csv_writer.write()
        self.assertEqual(
            self._get_file_contents(),
            EXISTING_CONTENT + TestCSVWriter.HEADER_AND_LINE)
        self._delete_directory_if_exists()

    def test_write(self):
        self._create_directory_if_needed()
        self._delete_file_if_exists()

        self._csv_writer.write()
        self.assertEqual(
            self._get_file_contents(),
            TestCSVWriter.HEADER_AND_LINE)
        self._delete_directory_if_exists()


if __name__ == "__main__":
    unittest.main()
