import os
import subprocess
import json
from datetime import datetime
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm
from enum_util import AlarmKeys
from htmlUtil import HtmlUtil
from dbUtils import DBUtil
from healthmonitoring.collectors.utils.queries import Queries, QueryExecutor
from healthmonitoring.framework.specification.defs import DBType

from logger_util import \
    create_logger, Logger, get_default_log_level, \
    get_logging_instance

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))


def main():
    print(_execute())


def _execute():
    raw_data = Collector().collect()
    return Presenter(raw_data).present()


class Collector:
    def collect(self):
        return Collector._get_actual_vs_configured_meph()

    @staticmethod
    def _get_actual_vs_configured_meph():
        usage_adap_mapping = Collector._get_usage_adap_mapping()
        configured = Collector._get_configured_meph()
        final_usage = Collector._get_counts_from_usage(
            configured, usage_adap_mapping)  # noqa
        config_vs_actual = Collector._get_final(configured,
                                                final_usage)  # noqa
        return config_vs_actual

    @staticmethod
    def _get_usage_adap_mapping():
        query = Queries.USAGE_ADAP_MAPPING
        output_list = QueryExecutor.execute(DBType.POSTGRES_SDK, query)
        output = ""
        for item in output_list:
            output = output + '\n' + ''.join(item)
        output = output.splitlines()
        usage_adap_mapping = {}

        for line in output:
            if line:
                row = line.split("=")
                usage_adap_mapping[row[0]] = row[1]
        return usage_adap_mapping

    @staticmethod
    def _get_configured_meph():
        namespace = os.environ['RELEASE_NAMESPACE']
        rest_url = """sudo wget --no-check-certificate -O- -q etlconfigservice.{0}.svc.cluster.local:8080/api/topologies/v1/etl/deployment?topology=all""".format(  # noqa
            namespace)  # noqa
        json_response = Collector._get_json_response(rest_url)
        configured = {}
        for item in json_response:
            try:
                configured[item["adaptation"]] = configured[
                    item["adaptation"]] + item["meph"]  # noqa
            except KeyError:
                configured[item["adaptation"]] = item["meph"]
        return configured

    @staticmethod
    def _get_counts_from_usage(configured, usage_adap_mapping):
        today = datetime.today().strftime('%Y%m%d')
        file_path = '/opt/nsn/ngdb/monitoring/output/tableCount_Usage/'
        file = file_path + 'total_count_usage_' + str(today) + '.json'  # noqa

        if os.path.exists(file):
            f = open(file, 'r')
            actual_from_file = json.loads(f.read())
            usage = {}
            for item in actual_from_file:
                if item['Hour'] == '24':
                    usage = item
        final_usage = {}
        for key in configured.keys():
            for key_m in usage_adap_mapping.keys():
                if usage_adap_mapping[key_m] == key and key_m in usage.keys():
                    try:
                        final_usage[key] = int(
                            round((int(final_usage[key]) + int(usage[key_m])) /
                                  (1000000 * 10), 0))
                    except KeyError:
                        final_usage[key] = int(
                            round(int(usage[key_m]) / (1000000 * 10),
                                  0))  # noqa
        return final_usage

    @staticmethod
    def _get_final(configured, final_usage):
        config_vs_actual = []
        for key in configured.keys():
            if key in final_usage.keys():
                config_vs_actual.append({
                    'adaptation':
                    key,
                    'configured_meph':
                    configured[key],
                    'actual_meph':
                    final_usage[key],
                    'Difference':
                    abs(configured[key] - final_usage[key]),
                    'Date':
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })  # noqa
        return config_vs_actual

    @staticmethod
    def _get_json_response(rest_url):
        rest_response = subprocess.getoutput(rest_url)
        if "server returned error" in rest_response:
            return ""
        else:
            return json.loads(rest_response)


class Presenter:
    identifier = "configure_vs_actual_meph"
    ALARM_KEY = AlarmKeys.MEPH_ALARM.value
    MEPH_THRESHOLD = 50

    def __init__(self, raw_data):
        self._data = raw_data
        self._breached_data = []

    def present(self):
        output = []
        breached_data = []
        for data in self._data:
            dict_data = dict(data)
            output.append(dict_data)
        Presenter.push_data_to_maria_db(output)
        breached_data = self._get_breached_info(output)
        Presenter.send_alert(breached_data)
        return output

    def _get_breached_info(self, output):
        for item in output:
            if int(item['Difference']) > Presenter.MEPH_THRESHOLD:
                self._breached_data.append(
                    {item['adaptation']: str(item['Difference'])})  # noqa
        return self._breached_data

    @staticmethod
    def send_alert(breached_data):
        if breached_data:
            severity = SnmpAlarm.get_severity_for_email(Presenter.ALARM_KEY)
            html = str(HtmlUtil().generateHtmlFromDictList(
                "MEPH configuration is not done properly for " +
                str(Presenter.identifier), breached_data))  # noqa
            EmailUtils().frameEmailAndSend(
                "[{0} ALERT]: MEPH(tuning) is not done properly for adaptations and can see more data in usage than tuned: at {1}"  # noqa: 501
                .format(  # noqa: 501
                    severity,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                html,
                Presenter.ALARM_KEY)  # noqa
            SnmpAlarm.send_alarm(Presenter.ALARM_KEY, breached_data)

    @staticmethod
    def push_data_to_maria_db(stats):
        for stat in stats:
            json = ('[%s]' % stat).replace("'", '"')
            DBUtil().jsonPushToMariaDB(json, Presenter.identifier)


if __name__ == '__main__':
    main()
