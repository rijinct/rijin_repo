'''
Created on 14-Feb-2020

@author: a4yadav
'''
import sys

from logger import Logger

from healthmonitoring.framework import config

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class RestartContainerActor:

    def act(self, notification_content):
        logger.info("Initiating RestartContainerAction execution...")
        sys.exit(1)
