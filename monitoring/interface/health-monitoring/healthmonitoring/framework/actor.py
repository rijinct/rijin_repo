'''
Created on 11-Dec-2019

@author: nageb
'''
import re

from logger import Logger

from healthmonitoring.framework import config

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class Actor:

    def take_actions(self, event):
        actions = event.event_spec.get_severity_actions(str(event.severity))
        for action in actions:
            self._take_action(event, action)

    def _take_action(self, event, action):
        actor_name = action
        try:
            actor = Actor._get_actor_instance(actor_name)
            notification_content = Actor._get_notification_content(
                actor_name, event.event_spec)
            notification_content = {**notification_content, **{"event": event}}
            actor.act(notification_content)
        except ImportError as e:
            logger.error("Unable to locate actor: '%s'", e.name)

    @staticmethod
    def _get_actor_instance(actor_name):
        module_name, class_name = Actor._get_module_and_class_names(actor_name)
        command = "from {} import {}".format(module_name, class_name)
        logger.debug("Import command: '%s'", command)
        exec(command, globals(),
             locals())
        return eval("{}()".format(class_name))

    @staticmethod
    def _get_module_and_class_names(actor_name):
        module_name = "healthmonitoring.framework.actors.{}".format(actor_name)
        class_name = actor_name.capitalize() + 'Actor'
        class_name = re.sub(
            r'_(\w)', lambda match: r'{0}'.format(match.group(1).upper()),
            class_name)
        return module_name, class_name

    @staticmethod
    def _get_notification_content(actor_name, event_spec):
        return event_spec.get_notification(
            "{}_notification".format(actor_name))
