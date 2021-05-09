import subprocess

from logger import Logger

from healthmonitoring.framework import config
from healthmonitoring.framework.config import hosts_spec

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class TrapActor:

    @staticmethod
    def _get_trap_action_instance(module_name, class_name):
        exec("from {} import {}".format(module_name, class_name), globals(),
             locals())
        return eval("{}()".format(class_name), globals(), locals())

    @staticmethod
    def _get_class_name(module_name):
        class_name = ""
        for character in module_name.split('_'):
            class_name += character.capitalize()

        return class_name

    def act(self, notification_content):
        logger.info("Initiating TrapAction execution...")
        module_name = notification_content['module_name']
        class_name = TrapActor._get_class_name(module_name)
        action = TrapActor._get_trap_action_instance(
                "healthmonitoring.framework.actors.traps.{}".
                format(module_name), class_name)
        action.notification_content = notification_content
        action.execute()


class TrapMedia:

    def __init__(self):
        snmp_details = hosts_spec.get_host('snmp')
        self._setup(snmp_details.fields['cemod_snmp_ip'],
                    snmp_details.fields['cemod_snmp_port'],
                    snmp_details.fields['cemod_snmp_community'])

    def _setup(self, snmp_ip, snmp_port, snmp_community):
        self._snmp_ip = snmp_ip
        self._snmp_port = snmp_port
        self._snmp_community = snmp_community

    @property
    def snmp_ip(self):
        return self._snmp_ip

    @property
    def snmp_port(self):
        return self._snmp_port

    @property
    def snmp_community(self):
        return self._snmp_community


class TrapAction:
    _media = None

    def __init__(self):
        self._notification_content = None
        if TrapAction._media is None:
            TrapAction._media = TrapMedia()

    @property
    def notification_content(self):
        return self._notification_content

    @notification_content.setter
    def notification_content(self, notification_content):
        self._notification_content = notification_content

    def get_action_specific_conf(self):
        pass

    def _analyze_return_code(self, return_code):
        if return_code == 0:
            logger.info('SNMP traps sent successfully.')
        else:
            logger.info('Error sending SNMP trap.')

    def _get_return_code(self, action_specific_configuration):
        return subprocess.Popen(['/usr/bin/snmptrap', '-v', '2c', '-c',
                                 '{0} {1}:{2}'.format(
                                     TrapAction._media.snmp_community,
                                     TrapAction._media.snmp_ip,
                                     TrapAction._media.snmp_port),
                                 action_specific_configuration
                                 ], stdout=subprocess.PIPE).returncode

    def execute(self):
        action_specific_configuration = self.get_action_specific_conf()
        return_code = self._get_return_code(action_specific_configuration)
        self._analyze_return_code(return_code)
