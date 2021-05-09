'''
Created on 11-Dec-2019

@author: nageb
'''
from copy import deepcopy
import csv
import json
import os
import re
import threading

from healthmonitoring.framework import config
from healthmonitoring.framework.db.connection_factory import ConnectionFactory
from healthmonitoring.framework.util import file_util
from healthmonitoring.framework.util.collection_util import get_lone_value
from healthmonitoring.framework.util.trigger_util import TriggerUtil

from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class Item:

    def __init__(self, item_spec, **params):
        self._item = item_spec
        self._params = params

    def __str__(self):
        return "{} ({})".format(self.name, str(self.params))

    @property
    def item(self):
        return self._item

    @item.setter
    def item(self, value):
        self._item = value

    @property
    def name(self):
        return self.item.name

    @property
    def csv_details(self):
        return self._item.csv_details

    @property
    def params(self):
        return self._params


class Trigger:

    def __init__(self, trigger_spec):
        self._spec = trigger_spec
        self._item = None
        self._state = False

    def __eq__(self, other):
        if not isinstance(other, Trigger):
            return NotImplemented

        return self.name == other.name and self.condition == other.condition \
            and self.state == other.state

    @property
    def name(self):
        return self._spec.name

    @name.setter
    def name(self, name):
        self._spec.name = name

    @property
    def variables(self):
        return self._spec.variables

    @variables.setter
    def variables(self, variables):
        self._spec.variables = variables

    @property
    def condition(self):
        return self._spec.condition

    @condition.setter
    def condition(self, condition):
        self._spec.condition = condition

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state

    @property
    def item(self):
        return self._item

    @item.setter
    def item(self, item):
        self._item = item

    def evaluate(self, store):
        logger.debug("Condition before substitution: %s", self.condition)
        self._substitute_spec(store)
        logger.debug("Condition after substitution: %s",
                     self._substituted_condition)
        self._state = self._evaluate_condition()
        logger.debug("Condition evaluation: %s", str(self._state))
        return self.state

    def _substitute_spec(self, store):
        values = {}
        for key, value in self.variables.items():
            item_expr = self._extract_item_expr(value)
            values[key] = self._get_replacement_value(store, item_expr)
        self._substituted_condition = self.condition.format(**values)

    def _extract_item_expr(self, item_identifier):
        return item_identifier[2:-1]

    def _get_replacement_value(self, store, item_expr):
        val = self._get_replacement_value_for_parameterized_item(
            store, item_expr)
        if isinstance(val, str) and not val:
            val = "''"
        return val

    def _get_replacement_value_for_parameterized_item(self, store, item_expr):
        index = item_expr.index("[")
        item_name_with_truncated_param = item_expr[:index]
        param_names = TriggerUtil.get_param_name_list(item_expr[index:])
        if store.contains_item(item_name_with_truncated_param):
            item_params = store.get_item(item_name_with_truncated_param).params
            try:
                return self._as_suitable_type(
                    TriggerUtil.evaluate_dict(item_params, param_names))
            except Exception:
                logger.exception("Error while evaluating item '%s' ",
                                 item_expr)
        return "0"

    def _as_suitable_type(self, x):
        if isinstance(x, str):
            return "'{}'".format(x)
        else:
            return x

    def _evaluate_condition(self):
        try:
            return eval(self._substituted_condition)
        except Exception:
            logger.error("Unable to evaluate condition: '%s'",
                         self._substituted_condition)
            raise


class _JsonConvertor:

    @staticmethod
    def convert_csv_to_jsons(filename):
        with open(filename) as csv_reader:
            dict_reader = csv.DictReader(csv_reader)
            return [json.dumps(row) for row in dict_reader]


class _CSVWriter:

    def __init__(self, item):
        self._item = item

    @property
    def pathname(self):
        return self._pathname

    def write(self):
        self._pathname = os.path.join(
            self._create_or_get_directory(),
            self._item.csv_details.csv_filename.format(**self._item.params))
        with open(self._pathname, "a") as file:
            self._file = file
            self._header_required = self._item.csv_details.csv_header_always
            self._header = self._item.csv_details.csv_header
            self._write_header_if_required()
            self._write_to_file()

    def _create_or_get_directory(self):
        if self._item.csv_details.csv_directory:
            directory = file_util.get_resource_path(
                self._item.csv_details.csv_directory)
        else:
            directory = file_util.get_resource_path(
                config.properties['CSVDirectorySection'][self._get_app_name()])
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def _get_app_name(self):
        return self._item.item.app.name

    def _write_header_if_required(self):
        if self._header_required or self._is_file_empty():
            self._write_header()

    def _write_header(self):
        self._file.write(self._header + '\n')

    def _is_file_empty(self):
        return os.path.getsize(self._pathname) == 0

    def _write_to_file(self):
        place_holders = re.findall(r"{(.*?)}",
                                   self._item.csv_details.csv_filename)
        content = get_lone_value(self._item.params, excludes=place_holders)
        logger.debug("Content has %s", content)
        logger.debug("Header has %s", self._header.split(","))
        writer = csv.DictWriter(self._file,
                                fieldnames=self._header.split(","),
                                lineterminator='\n')
        for each_dict in content:
            writer.writerow(each_dict)
        logger.info("CSV Writing has been Completed")


class Store:

    def __init__(self):
        self._items = {}
        self._triggers = {}
        self._trigger_generator = _TriggerGenerator(self)
        self._lock = threading.Lock()

    @property
    def analyzer(self):
        return self._analyzer

    @analyzer.setter
    def analyzer(self, analyzer):
        self._analyzer = analyzer

    @property
    def items(self):
        return self._items

    @property
    def triggers(self):
        return self._triggers

    def add_item(self, item):
        with self._lock:
            self._items[item.name] = item
            self._trigger_generator.identify_and_generate(item)
            if self._should_generate_csv(item):
                csv_file_path = self._write_to_csv(item)
                self._write_to_db(item, csv_file_path)

    def contains_item(self, item_name):
        return item_name in self._items

    def get_item(self, item_name):
        return self._items[item_name]

    def add_trigger_spec(self, trigger_spec):
        self._triggers[trigger_spec.name] = Trigger(trigger_spec)

    def get_trigger(self, name):
        return self._triggers[name]

    def _should_generate_csv(self, item):
        return item.csv_details.csv_header and item.csv_details.csv_filename

    def _write_to_csv(self, item):
        csv_writer = _CSVWriter(item)
        csv_writer.write()
        return csv_writer.pathname

    def _write_to_db(self, item, csv_file_path):
        utility_type = item.item.db_category
        if utility_type:
            self._insert_into_db(utility_type, csv_file_path)

    def _insert_into_db(self, utility_type, csv_file_path):
        json_list = _JsonConvertor.convert_csv_to_jsons(csv_file_path)
        if json_list:
            try:
                ConnectionFactory.get_connection().\
                    insert_values(utility_type, json_list)
            except ConnectionError as e:
                logger.exception('Error occurred while inserting values to db',
                                 e)


class _TriggerGenerator:

    def __init__(self, store):
        self._store = store
        self._trigger_generated = False

    def identify_and_generate(self, item):
        self._item = item
        pattern = re.compile(r"\$\{" + self._item.name + r"[}\[]")
        for trigger in self._get_all_triggers():
            if self._is_trigger_to_be_evaluated(trigger, pattern):
                self._evaluate_and_generate_trigger(self._item, trigger)

    def _is_trigger_to_be_evaluated(self, trigger, pattern):
        for key, value in trigger.variables.items():
            if pattern.search(value):
                return True

    def _get_all_triggers(self):
        return self._store.triggers.values()

    def _evaluate_and_generate_trigger(self, item, trigger):
        if self._references_multiple_rows(trigger.variables):
            self._evaluate_and_generate_multiple_triggers(item, trigger)
        else:
            self._evaluate_and_generate_single_trigger(item, trigger)

    def _references_multiple_rows(self, variables):
        return bool(self._get_list_param_names(variables))

    def _get_list_param_names(self, variables):
        list_params = []
        for key, value in variables.items():
            for (item_name,
                 param) in re.findall(r"\$\{([a-zA-Z0-9_.]*)(\[.*?)\}", value):
                param_name = param[:param.index("]") + 1]
                if self._is_list_param(item_name, param_name):
                    list_params.append(param_name)
        return list(set(list_params))

    def _is_list_param(self, item_name, param_name):
        params = TriggerUtil.evaluate_dict(
            self._store.items.get(item_name).params,
            TriggerUtil.get_param_name_list(param_name))
        return isinstance(params, list)

    def _evaluate_and_generate_multiple_triggers(self, item, trigger):
        self._trigger_generated = False
        self._generate_index_triggers_as_required(item, trigger)
        if self._trigger_generated:
            trigger.state = True
            self._generate_trigger(trigger, item)

    def _generate_index_triggers_as_required(self, item, trigger):
        length = self._get_list_param_size(item, trigger)
        condition = trigger.condition
        for index in range(length):
            self._evaluate_and_generate_trigger_for_index(item, trigger, index)
            trigger.condition = condition

    def _get_list_param_size(self, item, trigger):
        param_name = self._get_list_param_names(trigger.variables)[0]
        return _TriggerGenerator._get_length_of_param_ds(item, param_name)

    @staticmethod
    def _get_length_of_param_ds(item, param_name):
        return len(
            TriggerUtil.evaluate_dict(
                item.params, TriggerUtil.get_param_name_list(param_name)))

    def _evaluate_and_generate_trigger_for_index(self, item, trigger, index):
        index_trigger = deepcopy(trigger)
        self._insert_index(index_trigger, index)
        index_trigger.name += "_{}".format(index)
        self._evaluate_and_generate_single_trigger(item, index_trigger)

    def _insert_index(self, trigger, index):
        for param_name in self._get_list_param_names(trigger.variables):
            self._update_index_in_variable(trigger, param_name, index)

    def _update_index_in_variable(self, trigger, param_name, index):
        for key, value in trigger.variables.items():
            trigger.variables[key] = value.replace(param_name,
                                                   f'{param_name}[{index}]')

    def _evaluate_and_generate_single_trigger(self, item, trigger):
        previous_state = trigger.state
        trigger.evaluate(self._store)
        if previous_state != trigger.state or previous_state:
            self._generate_trigger(trigger, item)

    def _generate_trigger(self, trigger, item):
        trigger.item = item
        logger.info("Generated trigger: %s", trigger.name)
        self._store.analyzer.inject_trigger(trigger)
        self._trigger_generated = True
