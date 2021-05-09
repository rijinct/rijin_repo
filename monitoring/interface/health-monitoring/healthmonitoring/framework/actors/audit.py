'''
Created on 21-Apr-2020

@author: nageb
'''
from healthmonitoring.framework import config
from healthmonitoring.framework.specification.defs import Severity
from healthmonitoring.framework.store import TriggerUtil

from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class AuditActor:

    def act(self, notification_content):
        self._notification_content = notification_content
        self._fetch_report()
        self._fetch_category()
        self._fetch_checklist_item()
        self._change_results()

    def _fetch_report(self):
        report_name = self._notification_content['report']
        self._report = config.report_store.get(report_name)

    def _fetch_category(self):
        category_name = self._notification_content['category']
        self._category = self._report.get_category(category_name)

    def _fetch_checklist_item(self):
        item_name = self._notification_content['name']
        self._checklist_item = self._category.get_checklist_item(item_name)

    def _change_results(self):
        event = self._notification_content['event']
        state = event.severity == Severity.CLEAR
        logger.debug("Setting audit state of {}:{} to {}".format(
            self._category.name, self._checklist_item.name, state))
        self._checklist_item.result = state
        self._checklist_item.failures = TriggerUtil.get_failures_list(
            event)
        self._category.recompute()
