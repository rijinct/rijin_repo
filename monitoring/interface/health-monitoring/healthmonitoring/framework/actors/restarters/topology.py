import subprocess

from logger import Logger

from healthmonitoring.framework import config
from healthmonitoring.framework.util import string_util
from healthmonitoring.framework.util.trigger_util import TriggerUtil
from healthmonitoring.framework.util.specification import SpecificationUtil

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class _TopologyServiceRestarter:

    def __init__(self, notification_content):
        self._triggers = notification_content['event'].triggers

    def restart(self):
        for specific_trigger_name in self._triggers:
            failed_topology_name = string_util.substring_before(
                "_", self._get_item_value(
                    specific_trigger_name))
            self._stop_topology(failed_topology_name)
            adaptation_name = self._get_adaptation_name(failed_topology_name)
            self._start_topology(adaptation_name, failed_topology_name)

    def _get_item_value(self, specific_trigger_name):
        trigger = self._triggers[specific_trigger_name]
        index = TriggerUtil.get_index_of_trigger(trigger.name)
        return trigger.item.params['topologies'][index]['Connector_Type']

    def _get_adaptation_name(self, topology_name):
        cemod_postgres_sdk_fip_active_host = SpecificationUtil.get_host(
            "postgres_sdk").fields["cemod_postgres_sdk_fip_active_host"]
        return self._run_command(
            f'''ssh {cemod_postgres_sdk_fip_active_host} '/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c "select adaptationid from adapt_cata where id in (( select adaptationid from adap_entity_rel where usagespecid in ((select id from usage_spec where specid in (\'{topology_name}\')))));"' | egrep -v "rows|row|--|adaptationid"|head -1|awk '$1=$1' '''  # noqa: 501
        )

    def _start_topology(self, adaptation_name, topology_name):
        self._run_command(
            f'/opt/nsn/ngdb/etl/app/bin/etl-management-service.sh -adaptation {adaptation_name} -version 1 -topologyname {topology_name}'  # noqa: 501
        )

    def _stop_topology(self, topology_name):
        self._run_command(
            f'/opt/nsn/ngdb/etl/app/bin/etl-management-service.sh -kill {topology_name}'  # noqa: 501
        )

    def _run_command(self, command):
        return subprocess.Popen(command,
                                stdout=subprocess.PIPE,
                                shell=True).communicate()[0]
