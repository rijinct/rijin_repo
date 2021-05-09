"""
Copyright (C) 2018 Nokia. All rights reserved.
"""
import json
from .missing_mandatory_filed import MissingMandatoryField


class MessageImpl(object):
    """
    This is the internal implementation of log, alarm, and counter messages, not intended
    to be used by application users directly.
    """
    def __init__(self):
        '''
        Constructor of MessageImpl
        '''
        self._metadata = dict()
        self._delimiters = '.'

    @property
    def metadata(self):
        '''
        The message content
        :return: return a dict type of the message content
        '''
        return self._metadata

    def set(self, key, value):
        """
        Set the value of a leaf key
        :type key: str
        :param key: the key of a leaf, which is delimited by dots. e.g. 'alarm.severity'
        :param value: he value of the leaf key
        :return: None
        """
        if not isinstance(key, str):
            raise TypeError('key must be a str')
        if value is None:
            raise TypeError('the value of %s can not be None' % key)
        self._set(key.split(self._delimiters), value)

    def _set(self, subkeys, value):
        if not isinstance(subkeys, list):
            raise TypeError('subkeys must be a list')
        cursor = self._metadata
        for i in range(0, len(subkeys)-1):
            subkey = subkeys[i]
            if subkey not in cursor:
                cursor[subkey] = dict()
            cursor = cursor.get(subkey)
        subkey = subkeys[len(subkeys)-1]
        cursor[subkey] = value

    def contains_key_dotted(self, key):
        """
        Indicate whether the message has a special key
        :param key: the key of a leaf, which is delimited by dots. e.g. 'alarm.severity'
        :return: return True if the message has a special key, else False
        :raise TypeError: if the parameter key is not a str
        """
        if not isinstance(key, str):
            raise TypeError('key must be a str')
        subkeys = key.split(self._delimiters)
        cursor = self._metadata
        for i in range(0, len(subkeys)-1):
            subkey = subkeys[i]
            if subkey not in cursor:
                return False
            cursor = cursor.get(subkey)
        subkey = subkeys[len(subkeys)-1]
        return subkey in cursor

    def validate(self, mandatory_fields):
        '''
        Check if the message is lack of any mandatory fields, just before being sent out
        :type mandatory_fields: list
        :param mandatory_fields: the list of mandatory fields
        :return: None
        :raise TypeError: if the parameter mandatory_fields is not a list
        :raise MissingMandatoryField: if any of the mandatory fields given in the parameter is missing
        '''
        if not isinstance(mandatory_fields, list):
            raise TypeError('mandatory_fields must be a list')
        for key in mandatory_fields:
            if not self.contains_key_dotted(key):
                raise MissingMandatoryField(key)

    def tojson(self):
        """
        Convert the message into a JSON string
        :return: the JSON string
        """
        return json.dumps(self._metadata, ensure_ascii=False)
