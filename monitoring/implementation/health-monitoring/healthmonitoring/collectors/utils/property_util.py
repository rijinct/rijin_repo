'''
Created on 27-Apr-2020

@author: deerakum
'''
import configparser
import os


class PropertyUtil:
    def __init__(self, monitoring_type, section):
        self.monitoring_type = monitoring_type
        self.section = section
        self.configLoad = configparser.RawConfigParser()
        self.configLoad.read(
            os.path.join(os.path.abspath(os.path.dirname(__file__)),
                         'monitoring.properties'))

    def get_value_for_key(self):
        return self.configLoad.get(self.section, self.monitoring_type)
