from datetime import datetime
from os import path
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm
from enum_util import AlarmKeys
from dbUtils import DBUtil
from htmlUtil import HtmlUtil

from logger_util import \
    create_logger, Logger, get_default_log_level, \
    get_logging_instance  # noqa

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
        return self._get_exception_status()

    def _get_exception_status(self):
        imsi_exception_status = []
        file = '/mnt/staging/healthmonitoring/kubectl/etl_imsi_id.txt'
        if path.exists(file):
            with open(file) as f:
                for line in f:
                    words = line.strip().split(":")
                    imsi_exception_status.append({'topology': words[0], 'exception_status': words[1]})  # noqa
            return imsi_exception_status


class Presenter:

    identifier = "generate_imsi_id_exceptions"
    ALARM_KEY = AlarmKeys.IMSI_ID_GENERATE_EXCEPTION.value

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
            if int(item['exception_status']) == 0:
                self._breached_data.append(
                    {item['topology']: "1"})
        return self._breached_data

    def send_alert(breached_data):
        print(breached_data)
        if breached_data:
            severity = SnmpAlarm.get_severity_for_email(Presenter.ALARM_KEY)
            html = str(HtmlUtil().generateHtmlFromDictList(
                "IMSI_ID Exceptions are seens for :  " + str(Presenter.identifier), breached_data))  # noqa
            EmailUtils().frameEmailAndSend("[{0} ALERT]: IMSI_ID generation exceptiosn are seen: at {1}".format(# noqa: 501
                severity, datetime.now().strftime("%Y-%m-%d %H:%M:%S")), html, Presenter.ALARM_KEY)  # noqa
            SnmpAlarm.send_alarm(Presenter.ALARM_KEY, breached_data)

    @staticmethod
    def push_data_to_maria_db(stats):
        print(stats)
        for stat in stats:
            json = ('[%s]' % stat).replace("'", '"')
            DBUtil().jsonPushToMariaDB(json, Presenter.identifier)


if __name__ == '__main__':
    main()
