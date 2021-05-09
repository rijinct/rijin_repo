'''
Created on 12-Dec-2019

@author: nageb
'''
from pathlib import Path
import unittest

from healthmonitoring.framework import config
from healthmonitoring.framework.specification.defs import \
    Service, HostGroup, ItemSpecification, \
    Application, Severity, Action, EventSpecification, \
    ItemsSpecification, ConflictDetector, CSVDetails, SpecInjector
from healthmonitoring.framework.specification.yaml import \
    YamlHostSpecificationReader, YamlItemsSpecificationReader, \
    YamlTriggersSpecificationReader, YamlEventsSpecificationReader, \
    YamlSpecificationLoader, YamlSpecificationCollectionLoader, \
    YamlEventsSpecificationWriter, YamlMonitoringSpecificationReader
from healthmonitoring.framework.util import file_util

from tests.healthmonitoring.framework.utils import TestUtil


class TestMonitoringSpecification(unittest.TestCase):

    def setUp(self):
        file_util.under_test = True
        self.spec = YamlMonitoringSpecificationReader.read(
            file_util.get_resource_path('monitoring.yaml'))

    def test_multiple_keys_exist(self):
        self.assertTrue(len(self.spec.keys()) > 1)

    def test_read_a_property(self):
        expected_value = {
            'property': {
                'autocorrection': True
            },
        }
        actual_value = self.spec.get('BucketInformation')
        self.assertEqual(actual_value, expected_value)


class TestHostSpecification(unittest.TestCase):
    SAMPLE_APPLICATION_GROUP_NAME = "couchbase"
    SAMPLE_PLATFORM_GROUP_NAME = "hdfs"

    def setUp(self):
        file_util.under_test = True
        pathname = "hosts.yaml"
        self.spec = YamlHostSpecificationReader.read(
            file_util.get_resource_path(pathname))

    def test_get_host_address_by_name_for_application_host_group(self):
        self._verify_hosts(TestHostSpecification.SAMPLE_APPLICATION_GROUP_NAME,
                           ["h1", "h2", "h3"])

    def test_get_host_address_by_name_for_platform_host_group(self):
        self._verify_hosts(TestHostSpecification.SAMPLE_PLATFORM_GROUP_NAME,
                           ["h6"])

    def _verify_hosts(self, name, addresses):
        hosts = self.spec.get_hosts(name)
        host_names = map(lambda x: x.address, hosts)
        self.assertEqual(list(host_names), addresses)

    def test_get_host_for_application_host_group(self):
        self.assertEqual(
            self.spec.get_host(
                TestHostSpecification.SAMPLE_APPLICATION_GROUP_NAME).address,
            "h1")

    def test_get_host_for_platform_host_group(self):
        self.assertEqual(
            self.spec.get_host(
                TestHostSpecification.SAMPLE_PLATFORM_GROUP_NAME).address,
            "h6")

    def test_get_category(self):
        self.assertEqual(
            self.spec.get_hosts(
                TestHostSpecification.SAMPLE_APPLICATION_GROUP_NAME).category,
            "application")
        self.assertEqual(
            self.spec.get_hosts(
                TestHostSpecification.SAMPLE_PLATFORM_GROUP_NAME).category,
            "platform")


class TestService(unittest.TestCase):
    NAME = "host1"
    ADDRESS = "address1"

    def setUp(self):
        self.host = Service(TestService.NAME, TestService.ADDRESS, {}, {})

    def test_get_name(self):
        self.assertEqual(self.host.name, TestService.NAME)

    def test_get_address(self):
        self.assertEqual(self.host.address, TestService.ADDRESS)

    def test_get_ha(self):
        self.host = Service(TestService.NAME, TestService.ADDRESS, {}, {},
                            True)
        self.assertEqual(self.host.ha, True)
        self.host = Service(TestService.NAME, TestService.ADDRESS, {}, {})
        self.assertEqual(self.host.ha, False)
        self.host = Service(TestService.NAME, TestService.ADDRESS, {}, {},
                            False)
        self.assertEqual(self.host.ha, False)
        self.host = Service(TestService.NAME, TestService.ADDRESS, {}, {},
                            None)
        self.assertEqual(self.host.ha, False)


class TestHostGroup(unittest.TestCase):
    CATEGORY = "category1"
    HOST_NAME1, HOST_NAME2, HOST_NAME3 = "host1", "host2", "host3"
    HOST_ADDRESS1, HOST_ADDRESS2, HOST_ADDRESS3 = \
        "address1", "address2", "address3"
    HA_STATUS1, HA_STATUS2, HA_STATUS3 = False, True, None
    HOST1 = Service(HOST_NAME1, HOST_ADDRESS1, {}, {}, HA_STATUS1)
    HOST2 = Service(HOST_NAME2, HOST_ADDRESS2, {}, {}, HA_STATUS2)
    HOST3 = Service(HOST_NAME3, HOST_ADDRESS3, {}, {}, HA_STATUS3)

    def setUp(self):
        self.host_group = HostGroup(TestHostGroup.CATEGORY)
        self.host_group.add_service(TestHostGroup.HOST1)
        self.host_group.add_service(TestHostGroup.HOST2)
        self.host_group.add_service(TestHostGroup.HOST3)

    def test_get_category(self):
        self.assertEqual(self.host_group.category, TestHostGroup.CATEGORY)

    def test_len(self):
        self.assertEqual(len(self.host_group), 3)

    def test_hosts_within_host_group(self):
        host_names = [
            TestHostGroup.HOST_NAME1, TestHostGroup.HOST_NAME2,
            TestHostGroup.HOST_NAME3
        ]
        host_addresses = [
            TestHostGroup.HOST_ADDRESS1, TestHostGroup.HOST_ADDRESS2,
            TestHostGroup.HOST_ADDRESS3
        ]
        host_ha_status = [False, True, False]
        count = 0

        for host in self.host_group:
            self.assertEqual(host.name, host_names[count])
            self.assertEqual(host.address, host_addresses[count])
            self.assertEqual(host.ha, host_ha_status[count])
            count += 1


class TestItemsSpecification(unittest.TestCase):

    def setUp(self):
        file_util.under_test = True
        pathname = "standard/healthmonitoring/items.yaml"
        self.spec = YamlItemsSpecificationReader.read(
            file_util.get_resource_path(pathname))

    def test_get_applications(self):
        applications = self.spec.applications
        expected = ("application1", "application2", "application3",
                    "container", "application4")
        self.assertTrue(set(expected).issubset(set(applications)))

    def test_get_all_item_names(self):
        item_names = list(self.spec.get_all_item_names())
        expected = "application1.item1", "application1.item2", \
            "application2.item3", "application2.item4", \
            "application3.item5", "application3.item6", \
            "application3.item7", "container.item1", \
            "application3.item8", "application3.item9", \
            "application3.item10", "application3.item11", \
            "application4.item12", "application4.item13"
        self.assertTrue(set(expected).issubset(set(item_names)))

    def test_get_item(self):
        self.assertEqual(
            self.spec.get_item("application1.item2").name,
            "application1.item2")

    def test_get_item_when_item_not_found(self):
        self.assertIsNone(self.spec.get_item("item99"))

    def test_get_custom_fields(self):
        self.assertEqual(
            self.spec.get_item("application1.item1").fields["hm_type"],
            "backlog")

    def test_get_host(self):
        self.assertEqual(
            self.spec.get_item("application1.item1").host, "couchbase")

    def test_get_commands(self):
        self.assertEqual(
            self.spec.get_item("application1.item1").commands,
            {
                'var1': 'cmd1',
                'var2':
                'ss -nlp | grep {port} | tr -s \' \' | gawk -F"," \'{{print $2}}\' | gawk -F"=" \'{{print $2}}\'',  # noqa:E501
                'var3': 'echo {var2}',
                'var4': '^date'
            })

    def test_get_item_when_enabled_not_present(self):
        item_name = "application1.item2"
        self.assertTrue(self.spec.get_item(item_name).enabled)

    def test_get_item_when_enabled_is_true(self):
        item_name = "application1.item1"
        self.assertTrue(self.spec.get_item(item_name).enabled)

    def test_get_item_when_enabled_is_false(self):
        item_name = "application2.item3"
        self.assertFalse(self.spec.get_item(item_name).enabled)

    def test_get_all_enabled_items(self):
        item_names = list(self.spec.get_all_enabled_item_names())
        enabled_items = [
            "application1.item1", "application2.item4", "application3.item6",
            "application3.item7", "application1.item2", "container.item1",
            "application3.item8", "application3.item9", "application3.item10",
            "application3.item11", "application4.item12", "application4.item13"
        ]
        self.assertTrue(set(enabled_items).issubset(set(item_names)))

    def test_get_all_csv_writing_enabled_items(self):
        expected = ["application4.item12"]
        item_names = list(self.spec.get_all_csv_writing_enabled_item_names())
        self.assertEqual(item_names, expected)

    def test_get_csv_header_from_item_name(self):
        expected = "Date, Service Name, Last Restart Time, Uptime"
        item_name = "application4.item12"
        actual_csv_header = (
            self.spec.get_item(item_name)).csv_details.csv_header
        self.assertEqual(actual_csv_header, expected)

    def test_get_csv_filename_from_item_name(self):
        expected = "serviceStability_DATE.csv"
        item_name = "application4.item12"
        actual_file_name = self.spec.get_item(
            item_name).csv_details.csv_filename
        self.assertEqual(actual_file_name, expected)

    def get_csv_header_always(self, item_name):
        return self.spec.get_item(item_name).csv_details.csv_header_always

    def test_get_csv_header_always_true_value(self):
        self.assertTrue(self.get_csv_header_always("application4.item12"))

    def test_get_csv_header_always_false_value(self):
        self.assertFalse(self.get_csv_header_always("application4.item13"))

    def test_get_default_csv_header_always(self):
        self.assertFalse(self.get_csv_header_always("application3.item11"))

    def get_csv_directory(self, item_name):
        return self.spec.get_item(item_name).csv_details.csv_directory

    def test_get_csv_directory_when_present(self):
        expected = "ServiceStability"
        self.assertEqual(self.get_csv_directory("application4.item12"),
                         expected)

    def test_get_csv_directory_when_not_present(self):
        self.assertIsNone(self.get_csv_directory("application4.item13"))

    def get_db_category(self, item_name):
        return self.spec.get_item(item_name).db_category

    def test_get_db_category_for_default_value(self):
        self.assertIsNone(self.get_db_category("application3.item11"))

    def test_get_db_category_for_non_none_value(self):
        self.assertEqual(
            self.get_db_category("application4.item12"), "serviceStability")


class TestItemSpecification(unittest.TestCase):
    NAME = "item1"
    SCHEDULE = "0 10 * * *"
    HOST = "host1"
    COMMANDS = {'var1': 'cmd1', 'var2': 'cmd2'}
    APP = Application("app")
    CSV_HEADER = "Date, Service Name, Last Restart Time, Uptime"
    CSV_DETAILS = CSVDetails(CSV_HEADER)

    def setUp(self):
        self.item_spec = ItemSpecification(TestItemSpecification.NAME,
                                           TestItemSpecification.HOST,
                                           TestItemSpecification.COMMANDS,
                                           TestItemSpecification.SCHEDULE,
                                           TestItemSpecification.APP,
                                           TestItemSpecification.CSV_DETAILS)

    def test_get_name(self):
        self.assertEqual(
            self.item_spec.name, "{}.{}".format(TestItemSpecification.APP.name,
                                                TestItemSpecification.NAME))

    def test_get_schedule(self):
        self.assertEqual(self.item_spec.schedule,
                         TestItemSpecification.SCHEDULE)

    def test_get_csv_header(self):
        self.assertEqual(self.item_spec.csv_details._csv_header,
                         TestItemSpecification.CSV_HEADER)

    def test_add_and_get_fields(self):
        KEY = "key1"
        VALUE = "value1"
        self.item_spec.add_field(KEY, VALUE)
        self.assertEqual(self.item_spec.fields[KEY], VALUE)

    def test_get_host(self):
        self.assertEqual(self.item_spec.host, "host1")

    def test_get_commands(self):
        self.assertEqual(self.item_spec.commands, {
            'var1': 'cmd1',
            'var2': 'cmd2'
        })


class TestApplication(unittest.TestCase):
    APPLICATION_NAME = "app1"
    ITEM1 = ItemSpecification("item1", "host1", {
        'var1': 'cmd1',
        'var2': 'cmd2'
    }, "* * * * *", Application("app"), "True")
    ITEM2 = ItemSpecification("item2", "host2", {
        'var3': 'cmd3',
        'var4': 'cmd4'
    }, "* * * * *", Application("app"), "False")
    ITEM3 = ItemSpecification("item3", "host1", {
        'var1': 'cmd1',
        'var4': 'cmd4'
    }, "* * * * *", Application("app"), "True")

    def setUp(self):
        self.application = Application(TestApplication.APPLICATION_NAME)
        self.application.add_item(TestApplication.ITEM1)
        self.application.add_item(TestApplication.ITEM2)
        self.application.add_item(TestApplication.ITEM3)

    def test_get_application_name(self):
        self.assertEqual(self.application.name,
                         TestApplication.APPLICATION_NAME)

    def test_get_items(self):
        expected = [
            TestApplication.ITEM1, TestApplication.ITEM2, TestApplication.ITEM3
        ]
        actual = set(self.application.get_items())
        self.assertEqual(len(actual), len(expected))
        for item in actual:
            self.assertTrue(item in expected)

    def test_get_item(self):
        self.assertEqual(self.application.get_item("app.item2"),
                         TestApplication.ITEM2)

    def test_get_item_when_item_not_present(self):
        self.assertIsNone(self.application.get_item("item99"))


class TestTriggersSpecification(unittest.TestCase):

    def setUp(self):
        file_util.under_test = True
        pathname = "standard/healthmonitoring/triggers.yaml"
        self.spec = YamlTriggersSpecificationReader.read(
            file_util.get_resource_path(pathname))

    def test_get_all_trigger_names(self):
        trigger_names = tuple(self.spec.get_all_trigger_names())
        self.assertTrue("trigger2" in trigger_names)

    def test_get_trigger(self):
        self.assertEqual(self.spec.get_trigger("trigger1").name, "trigger1")
        self.assertEqual(
            self.spec.get_trigger("trigger1").condition,
            "{value} > 100")

    def test_get_trigger_condition_for_one_alias(self):
        self.assertEqual(
            self.spec.get_trigger("trigger3").condition,
            "{hour_agg} != ''"
        )

    def test_get_trigger_condition_for_multiple_alias(self):
        self.assertEqual(
            self.spec.get_trigger("trigger4").condition,
            "{hour_agg} != '' and {fifmin_agg} != ''"
        )

    def test_get_trigger_with_config_variable(self):
        trigger = self.spec.get_trigger("trigger5")
        self.assertEqual(trigger.condition,
                         "{value} > {threshold}")
        self.assertEqual(
            trigger.variables['threshold'],
            '${__builtin.config["AGGREGATIONTHRESHOLD"]["Week"]["value"]}')


class TestEventsSpecification(unittest.TestCase):

    def setUp(self):
        file_util.under_test = True
        pathname = "standard/healthmonitoring/events.yaml"
        self.spec = YamlEventsSpecificationReader.read(
            file_util.get_resource_path(pathname))

    def test_get_event_names(self):
        expected_event_names = {
            "event1", "event2", "event3", "event4", "event5", "event6",
            "event7"
        }
        actual_event_names = self.spec.get_events().keys()
        for event_name in expected_event_names:
            self.assertTrue(event_name in actual_event_names)

    def test_get_event(self):
        for event_name in "event1", "event2":
            self.assertEqual(self.spec.get_event(event_name).name, event_name)

    def test_get_start_level(self):
        self.assertEqual(
            self.spec.get_event("event1").start_level, Severity.MINOR)
        self.assertEqual(
            self.spec.get_event("event2").start_level, Severity.MINOR)

    def test_get_escalation_freq(self):
        self.assertEqual(self.spec.get_event("event1").escalation_freq, 3)
        self.assertEqual(self.spec.get_event("event2").escalation_freq, 10)

    def test_get_severity_actions(self):
        self.assertEqual(
            self.spec.get_event("event1").get_severities(),
            ("minor", "major", "critical"))
        self.assertEqual(
            self.spec.get_event("event2").get_severities(),
            ("clear", "minor", "major", "critical"))

    def test_get_triggers(self):
        self.assertEqual(
            self.spec.get_event("event3").condition,
            "(trigger1 or trigger2) and (trigger3 or trigger4)")

    def test_get_notification(self):
        self.assertEqual(
            self.spec.get_event("event1").get_notification(
                "email_notification"), {
                    'subject': 'None',
                    'body': 'None'
                })

    def test_get_reset_triggers(self):
        self.assertEqual(
            self.spec.get_event("event3").reset_triggers, ["trigger3"])

    def test_get_consolidation(self):
        self.assertEqual(self.spec.get_event("event6").consolidated, False)
        self.assertEqual(self.spec.get_event("event7").consolidated, True)

    def test_get_ttl(self):
        self.assertEqual(self.spec.get_event("event6").ttl, 0)
        self.assertEqual(self.spec.get_event("event7").ttl, 60)


class TestEventSpecification(unittest.TestCase):
    EVENT_NAME = "event1"

    def test_creation_and_get_name(self):
        event_spec = EventSpecification(TestEventSpecification.EVENT_NAME)
        self.assertEqual(event_spec.name, TestEventSpecification.EVENT_NAME)

    def test_get_start_level(self):
        event_spec = EventSpecification(TestEventSpecification.EVENT_NAME)
        severity = Severity.MAJOR
        event_spec.start_level = severity
        self.assertEqual(event_spec.start_level, severity)

    def test_setting_start_level_in_mixed_case(self):
        event_spec = EventSpecification(TestEventSpecification.EVENT_NAME)
        event_spec.start_level = "major"
        self.assertEqual(event_spec.start_level, Severity.MAJOR)

    def test_setting_invalid_start_level(self):
        event_spec = EventSpecification(TestEventSpecification.EVENT_NAME)
        with self.assertRaises(ValueError):
            event_spec.start_level = "blah"

    def test_set_and_get_escalation_freq(self):
        event_spec = EventSpecification(TestEventSpecification.EVENT_NAME)
        ESCALATION_FREQ = 3
        event_spec.escalation_freq = ESCALATION_FREQ
        self.assertEqual(event_spec.escalation_freq, ESCALATION_FREQ)

    def test_add_and_get_severity_actions(self):
        event_spec = EventSpecification(TestEventSpecification.EVENT_NAME)
        event_spec.add_severity_action(Severity.create("minor"),
                                       Action.create("email_action"))
        event_spec.add_severity_action(Severity.create("minor"),
                                       Action.create("restart_action"))
        self.assertEqual(event_spec.get_severities(), ("minor",))
        self.assertEqual(
            event_spec.get_severity_actions(Severity.create("MINOR")),
            ("email_action", "restart_action"))

    def test_add_and_get_notifications(self):
        event_spec = EventSpecification(TestEventSpecification.EVENT_NAME)
        content = {'subject': 'Subject', 'body': 'Body'}
        event_spec.add_notification("email_notification", content)
        self.assertEqual(event_spec.get_notification("email_notification"),
                         content)

    def test_add_and_get_reset_trigger(self):
        event_spec = EventSpecification(TestEventSpecification.EVENT_NAME)
        event_spec.add_reset_trigger("trigger1")
        event_spec.add_reset_trigger("trigger2")
        self.assertEqual(event_spec.reset_triggers, ["trigger1", "trigger2"])

    def test_create_and_get_consolidation(self):
        event_spec = EventSpecification(TestEventSpecification.EVENT_NAME)
        self.assertFalse(event_spec.consolidated)

    def test_set_and_get_consolidation(self):
        event_spec = EventSpecification(TestEventSpecification.EVENT_NAME)
        consolidated = True
        event_spec.consolidated = consolidated
        self.assertEqual(event_spec.consolidated, consolidated)


class TestSpecificationLoading(unittest.TestCase):

    def test_load_all_specification(self):
        TestUtil.load_specification()
        self.assertIsNotNone(config.hosts_spec)
        self.assertIsNotNone(config.items_spec)
        self.assertIsNotNone(config.events_spec)
        self.assertIsNotNone(config.triggers_spec)

    def test_conflicting_directory(self):
        file_util.under_test = True
        portal_spec_directory = file_util.get_resource_path("standard/portal")
        webservice_spec_directory = file_util.get_resource_path(
            "standard/webservice")

        conflicting_directories = self._get_conflicting_directories(
            portal_spec_directory, webservice_spec_directory)
        self.assertTrue(conflicting_directories)

    def test_non_conflicting_directory(self):
        healthmonitoring_spec_directory = file_util.get_resource_path(
            "standard/healthmonitoring")

        portal_spec_directory = file_util.get_resource_path("standard/portal")

        conflicting_directories = self._get_conflicting_directories(
            healthmonitoring_spec_directory, portal_spec_directory)
        self.assertFalse(conflicting_directories)

    def _get_conflicting_directories(self, directory1, directory2):
        loader = YamlSpecificationLoader()
        loader.load([], [], [directory1])
        specs = YamlSpecificationCollectionLoader.load(directory2)
        return loader._get_conflicting_directory(specs)

    def get_spec_collection(self, directory, priority_directories,
                            specification_collection_dict):
        directories_to_scan = set(TestUtil.get_sub_directories(
            directory)) - set(priority_directories)
        return self.get_spec_collection_from_directories(
            directories_to_scan, specification_collection_dict)

    def get_spec_collection_from_directories(self, directories,
                                             specification_collection_dict):
        loader = YamlSpecificationLoader()
        loader._specification_collection_dict = specification_collection_dict
        loader._load_spec_from_directories(directories)
        return loader._specification_collection

    def test_merge_of_priority_custom_standard_directories(self):
        config.properties.read(
            file_util.get_resource_path('monitoring.properties'))
        custom_directory = config.properties._sections['DirectorySection'][
            'custom']
        standard_directory = config.properties._sections['DirectorySection'][
            'standard']
        priority_directories = TestUtil.get_priority_directories()
        specification_collection_dict = {}

        custom_dir_spec_collection = self.get_spec_collection(
            custom_directory, priority_directories,
            specification_collection_dict)
        expected_specification_collection = self.get_spec_collection(
            standard_directory, priority_directories,
            specification_collection_dict)

        loader = YamlSpecificationLoader()
        loader.load(priority_directories,
                    TestUtil.get_sub_directories(custom_directory),
                    TestUtil.get_sub_directories(standard_directory))

        specification_collection = loader._specification_collection

        if custom_dir_spec_collection is not None:
            expected_specification_collection.merge(custom_dir_spec_collection)

        self.assertEqual(
            expected_specification_collection.items_spec._get_keys(),
            specification_collection.items_spec._get_keys())
        self.assertEqual(
            expected_specification_collection.triggers_spec._get_keys(),
            specification_collection.triggers_spec._get_keys())
        self.assertEqual(
            expected_specification_collection.events_spec._get_keys(),
            specification_collection.events_spec._get_keys())


class TestYamlEventsSpecificationWriter(unittest.TestCase):

    def setUp(self):
        file_util.under_test = True

    def test_write(self):
        in_pathname = file_util.get_resource_path(
            "standard/healthmonitoring/events.yaml")
        out_pathname = file_util.get_resource_path("output-events.yaml")

        spec = YamlEventsSpecificationReader.read(in_pathname)
        YamlEventsSpecificationWriter.write(spec, out_pathname)

        test_instance = TestEventsSpecification()
        test_instance.spec = YamlEventsSpecificationReader.read(out_pathname)
        self._run_tests(test_instance)

        Path(out_pathname).unlink()

    def _run_tests(self, test_instance):
        for attr in dir(test_instance):
            if attr.startswith("test_"):
                getattr(test_instance, attr)()


class TestSpecificationCollection(unittest.TestCase):

    def test_conflicting_items(self):
        portal_spec_collection = YamlSpecificationCollectionLoader.load(
            file_util.get_resource_path("standard/portal"))
        webservice_spec_collection = YamlSpecificationCollectionLoader.load(
            file_util.get_resource_path("standard/webservice"))
        are_items_conflicting = ConflictDetector(
            portal_spec_collection).are_items_conflicting(
                webservice_spec_collection._items_spec)
        self.assertTrue(are_items_conflicting)

    def test_conflicting_triggers(self):
        portal_spec_collection = YamlSpecificationCollectionLoader.load(
            file_util.get_resource_path("standard/portal"))
        webservice_spec_collection = YamlSpecificationCollectionLoader.load(
            file_util.get_resource_path("standard/webservice"))
        are_triggers_conflicting = ConflictDetector(
            portal_spec_collection).are_triggers_conflicting(
                webservice_spec_collection._triggers_spec)
        self.assertTrue(are_triggers_conflicting)

    def test_conflicting_events(self):
        portal_spec_collection = YamlSpecificationCollectionLoader.load(
            file_util.get_resource_path("standard/portal"))
        webservice_spec_collection = YamlSpecificationCollectionLoader.load(
            file_util.get_resource_path("standard/webservice"))
        are_events_conflicting = ConflictDetector(
            portal_spec_collection).are_events_conflicting(
                webservice_spec_collection._events_spec)
        self.assertTrue(are_events_conflicting)

    def test_non_conflicting_items(self):
        portal_spec_collection = YamlSpecificationCollectionLoader.load(
            file_util.get_resource_path("standard/portal"))
        healthmonitoring_spec_collection = YamlSpecificationCollectionLoader.\
            load(file_util.get_resource_path("standard/healthmonitoring"))
        are_items_conflicting = ConflictDetector(
            healthmonitoring_spec_collection).are_items_conflicting(
                portal_spec_collection._items_spec)
        self.assertFalse(are_items_conflicting)

    def test_non_conflicting_triggers(self):
        portal_spec_collection = YamlSpecificationCollectionLoader.load(
            file_util.get_resource_path("standard/portal"))
        healthmonitoring_spec_collection = YamlSpecificationCollectionLoader.\
            load(file_util.get_resource_path("standard/healthmonitoring"))
        are_triggers_conflicting = ConflictDetector(
            healthmonitoring_spec_collection).are_triggers_conflicting(
                portal_spec_collection._triggers_spec)
        self.assertFalse(are_triggers_conflicting)

    def test_non_conflicting_events(self):
        portal_spec_collection = YamlSpecificationCollectionLoader.load(
            file_util.get_resource_path("standard/portal"))
        healthmonitoring_spec_collection = YamlSpecificationCollectionLoader.\
            load(file_util.get_resource_path("standard/healthmonitoring"))
        are_events_conflicting = ConflictDetector(
            healthmonitoring_spec_collection).are_events_conflicting(
                portal_spec_collection._events_spec)
        self.assertFalse(are_events_conflicting)


class TestSpecInjector(unittest.TestCase):

    def test_ttl_item_creation(self):
        TTL = 1
        expected = "every {} minute".format(TTL)

        items_spec = ItemsSpecification()
        spec_injector = SpecInjector((items_spec, None, None))
        spec_injector.inject_ttl_item(TTL)
        actual = spec_injector.get_ttl_item().schedule

        self.assertEqual(actual, expected)

    def test_audit_item_creation(self):
        name = "standard"
        schedule = "every 5 minutes"

        items_spec = ItemsSpecification()
        spec_injector = SpecInjector((items_spec, None, None))
        spec_injector.inject_audit_item(name, schedule)
        actual = spec_injector.get_audit_item(name).schedule

        self.assertEqual(actual, schedule)

    def test_inject_html_audit_report_events(self):
        TestUtil.load_specification()
        spec_injector = SpecInjector()
        spec_injector.inject_html_audit_report_events()
        self._assert_audit_event('standard')
        self._assert_audit_event('summary')

    def _assert_audit_event(self, report_name):
        actual = config.events_spec.get_event(
            f'__{report_name}_html_audit_event')
        self.assertEqual(actual.name, f'__{report_name}_html_audit_event')
        self.assertEqual(actual.start_level, Severity.CRITICAL)
        self.assertEqual(actual.escalation_freq, 99999)
        self.assertEqual(actual.get_severity_actions('critical'),
                         ('audit_html_report',))
        self.assertEqual(
            actual.get_notification('audit_html_report_notification'),
            {'report': report_name,
             'subject': 'Audit Report',
             'body': '{report}'})


if __name__ == "__main__":
    unittest.main()
