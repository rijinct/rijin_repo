'''
Created on 22-May-2020

@author: nageb
'''
import re

from healthmonitoring.framework.util import string_util


class TriggerUtil:

    @staticmethod
    def is_index_trigger(trigger_name):
        return bool(re.search(r'_\d+$', trigger_name))

    @staticmethod
    def get_index_of_trigger(trigger_name):
        return int(trigger_name.split('_')[-1])

    @staticmethod
    def remove_suffix_from_trigger_name(trigger_name):
        return re.sub(r'_\d+$', '', trigger_name)

    @staticmethod
    def get_specific_trigger_list(specific_trigger_name, trigger_name_list):
        return list(
            filter(
                lambda trigger_name: TriggerUtil.
                remove_suffix_from_trigger_name(
                    trigger_name) == specific_trigger_name, trigger_name_list))

    @staticmethod
    def get_index_triggers(trigger_name, trigger_name_list):
        return [name for name in TriggerUtil.get_specific_trigger_list(
            trigger_name, trigger_name_list)
            if name != trigger_name]

    @staticmethod
    def get_param_name_list(param):
        param_names = []
        for param_name in param.split("[")[1:]:
            TriggerUtil._update_param_names(
                param_names, param_name[:-1])
        return param_names

    @staticmethod
    def evaluate_dict(item_params, param_names):
        value = item_params
        for param_name in param_names:
            value = value[param_name]
        return value

    @staticmethod
    def get_referenced_triggers(condition):
        unique_triggers = set()
        for word in re.split(r'\W+', condition):
            if word.lower() not in ('and', 'or', 'not'):
                unique_triggers.add(word)
        return unique_triggers

    @staticmethod
    def get_failures_list(event):
        trigger_names = event.triggers.keys()
        ref_trigger_names = list(TriggerUtil.get_referenced_triggers(
            event.condition))

        ref_trigger = event.triggers[ref_trigger_names[0]]  # noqa: E501 TODO: Make it work for multiple triggers

        failures = []
        item = ref_trigger.item
        for index_trigger in TriggerUtil.get_index_triggers(
                ref_trigger.name, trigger_names):
            row_num = TriggerUtil.get_index_of_trigger(index_trigger)
            failures.append(item.params['table'][row_num])

        return failures

    @staticmethod
    def _update_param_names(param_names, param_name):
        if string_util.is_quoted(param_name):
            param_names.append(
                string_util.remove_quotes(param_name))
        else:
            param_names.append(int(param_name))
