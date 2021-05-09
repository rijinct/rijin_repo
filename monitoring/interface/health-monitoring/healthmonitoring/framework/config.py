'''
Created on 06-Feb-2020

@author: nageb
'''

import logging
from configparser import RawConfigParser

log_level = logging.DEBUG

properties = RawConfigParser()
monitoring_spec, hosts_spec, items_spec, triggers_spec, events_spec = \
    (None,) * 5
report_store = None
