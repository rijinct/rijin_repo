'''
Created on 28-Apr-2020

@author: nageb
'''
from logger import Logger

from healthmonitoring.framework import config

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class ConflictDetector:

    def __init__(self, specification_collection):
        self._specification_collection = specification_collection

    def are_items_conflicting(self, items_spec):
        if items_spec.conflicts_with(
                self._specification_collection.items_spec):
            logger.info('items.yaml conflicted')
            return True
        return False

    def are_triggers_conflicting(self, triggers_spec):
        if triggers_spec.conflicts_with(
                self._specification_collection.triggers_spec):
            logger.info('triggers.yaml conflicted')
            return True
        return False

    def are_events_conflicting(self, events_spec):
        if events_spec.conflicts_with(
                self._specification_collection.events_spec):
            logger.info('events.yaml conflicted')
            return True
        return False
