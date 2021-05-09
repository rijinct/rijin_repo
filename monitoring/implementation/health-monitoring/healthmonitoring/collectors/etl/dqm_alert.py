import os
import signal
from datetime import datetime
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm
from enum_util import AlarmKeys
from subprocess import Popen, PIPE, TimeoutExpired
from dbUtils import DBUtil
from htmlUtil import HtmlUtil

from monitoring_utils import XmlParserUtils
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

    identifier = "dqmBacklog"
    DQM_PATH = '/mnt/staging/dqm/'
    ALARM_KEY = AlarmKeys.DQM_FILECOUNT_ALARM.value
    TIMEOUT = 180
    DQM_THRESHOLD = 3000

    def __init__(self):
        self._topologies_data = Collector.get_list_of_topologies()
        self._dqm_data = []
        self._counter = []

    def collect(self):
        return self._get_dqm_count()

    @staticmethod
    def get_list_of_topologies():
        logger.debug("Getting Topologies Info")
        topologies = []
        data = XmlParserUtils.get_topologies_from_xml()
        for name, info in data.items():
            topologies.append({
                'name':
                name,
                'dqm_dir':
                '%s%s' % (Collector.DQM_PATH, info['Input'])
            })
        return topologies

    def _get_dqm_count(self):
        dqm_dict = {}
        for item in self._topologies_data:
            for keys in item.keys():
                if keys == "name":
                    topology = item[keys]
                if keys == "dqm_dir":
                    dqm_path = item[keys]
                    command = "ls " + str(dqm_path) + " | wc -l "
                    with Popen(command,
                               shell=True,
                               stdout=PIPE,
                               preexec_fn=os.setsid) as process:
                        try:
                            out = process.communicate(
                                timeout=Collector.TIMEOUT)[0]
                            dqm_dict[topology] = str(int(out))
                        except TimeoutExpired:
                            dqm_dict[topology] = '-1'
                            os.killpg(process.pid, signal.SIGKILL)
                            process.communicate()[0]
                            pass
        for key in dqm_dict.keys():
            self._counter.append({
                'topology':
                key,
                'filecount':
                dqm_dict[key],
                'Date':
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        return self._counter


class Presenter:

    def __init__(self, raw_data):
        self._data = raw_data
        self._breached_data = []

    def present(self):
        output = []
        for data in self._data:
            dict_data = dict(data)
            output.append(dict_data)
        Presenter.push_data_to_maria_db(output)
        breached_data = self._get_breached_info(output)
        Presenter.send_alert(breached_data)
        return output

    def _get_breached_info(self, output):
        for item in output:
            if int(item['filecount']) > Collector.DQM_THRESHOLD or int(
                    item['filecount']) == -1:
                self._breached_data.append(
                    {item['topology']: item['filecount']})
        return self._breached_data

    @staticmethod
    def send_alert(breached_data):
        if breached_data:
            severity = SnmpAlarm.get_severity_for_email(Collector.ALARM_KEY)
            html = str(HtmlUtil().generateHtmlFromDictList(
                "No of files in dqm is more for " + str(Collector.identifier),
                breached_data))
            EmailUtils().frameEmailAndSend(
                "[{0} ALERT]:No of files in DQM is exceeded threshold for {1} at {2}"  # noqa: 501
                .format(
                    severity, type,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                html,
                Collector.ALARM_KEY)
            SnmpAlarm.send_alarm(Collector.ALARM_KEY, breached_data)

    @staticmethod
    def push_data_to_maria_db(stats):
        for stat in stats:
            json = ('[%s]' % stat).replace("'", '"')
            DBUtil().jsonPushToMariaDB(json, Collector.identifier)


if __name__ == '__main__':
    main()
