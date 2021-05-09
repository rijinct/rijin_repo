'''
Created on 27-Mar-2020

@author: nageb
'''
from copy import deepcopy


def convert_list_dict_to_dict(list_dict):
    dictionary = {}
    for _dict in list_dict:
        for key, value in _dict.items():
            dictionary[key] = value
    return dictionary


def get_lone_value(dictionary, excludes=[]):
    param_dict = deepcopy(dictionary)
    for key in excludes:
        param_dict.pop(key)
    keys = list(param_dict.keys())
    if len(keys) == 1:
        return param_dict[keys[0]]
    raise ValueError('Zero or more than one keys exist')


def str2dict(string):
    d = {}
    if string:
        for pair in string.split(","):
            key, value = pair.split(':')
            d[key] = value
    return d


class DictObject:

    def __init__(self, dictionary):
        self._dict = dictionary

    def __bool__(self):
        return bool(self.dict)

    def __len__(self):
        return len(self.dict)

    def __iter__(self):
        return iter(self.dict)

    def __eq__(self, other):
        if isinstance(other, DictObject):
            return self.dict == other.dict
        elif isinstance(other, dict):
            return self.dict == other
        else:
            return False

    def __getattr__(self, attr):
        value = self.dict.get(attr)
        if isinstance(value, list):
            return ListObject(value)
        elif isinstance(value, dict):
            return DictObject(value)
        return value

    @property
    def dict(self):
        return self._dict

    def items(self):
        return self.dict.items()

    def keys(self):
        return self.dict.keys()

    def values(self):
        return self.dict.values()


class ListObject:

    def __init__(self, a_list):
        self._list = a_list

    def __bool__(self):
        return bool(self.list)

    def __len__(self):
        return len(self.list)

    def __iter__(self):
        return iter(self.list)

    def __eq__(self, other):
        if isinstance(other, ListObject):
            return self.list == other.list
        elif isinstance(other, list):
            return self.list == other
        else:
            return False

    def __getitem__(self, index):
        value = self.list[index]
        if isinstance(value, list):
            return ListObject(value)
        elif isinstance(value, dict):
            return DictObject(value)
        return value

    @property
    def list(self):
        return self._list
