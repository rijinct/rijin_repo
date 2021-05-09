import json, sys, requests, time
import multiprocessing
from lxml.html import fromstring

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from csvUtil import CsvUtil
from dateTimeUtil import DateTimeUtil
from dbConnectionManager import DbConnection
from dbUtils import DBUtil
from enum_util import AlarmKeys, TypeOfUtils
from htmlUtil import HtmlUtil
from logger_util import *
from monitoring_utils import XmlParserUtils, MonitoringUtils
from multiprocessing_util import multiprocess
from propertyFileUtil import PropertyFileUtil
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm
from yarn_queue_stats import get_vcore_output

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

user_name = 'admin'
user_password = 'admin'
port = os.environ['CDLK_CLUSTERPORT']
cloudera_ip = os.environ['CDLK_CLUSTERHOST']
cluster_name = os.environ['CDLK_CLUSTERNAME']
spark_services = ['spark2_thrift1', 'spark2_thrift2', 'spark2_thrift3']


def main():
    child_tag = XmlParserUtils.extract_child_tags('SparkSelfHeal', 'property')
    enabled = child_tag[0].attributes['enabled'].value
    if enabled.lower() == 'yes':
        _execute()
    else:
        logger.info("Spark Self Heal is disabled in Config Map")


def _execute():
    raw_data = Collector().collect()
    logger.debug("Raw Data is %s ", raw_data)
    processed_data = Processor(raw_data).process()
    Presenter(processed_data).present()


class Collector():
    def collect(self):
        api_url = "https://{}:{}/api/{}/clusters/{}/services/".format(
            cloudera_ip, port, ClouderaApiVersion.get_api_version(),
            cluster_name)
        services_info = requests.get(api_url,
                                     verify=False,
                                     auth=(user_name, user_password))
        return json.loads(services_info.content)


class ClouderaApiVersion:
    @staticmethod
    def get_api_version():
        api_version_url = "https://{}:{}/api/version".format(cloudera_ip, port)
        version_info = requests.get(api_version_url,
                                    verify=False,
                                    auth=(user_name, user_password))
        return version_info.content.decode("utf-8")


class Processor:

    results = multiprocessing.Manager().list()

    def __init__(self, raw_data):
        self.__data = raw_data
        self.util_name = TypeOfUtils.SERVICE_DOWN_UTIL.value
        self.services_stopped = []
        self.services_down_after_restart = []

    def process(self):
        DBUtil().jsonPushToMariaDB(json.dumps([self.__data]), self.util_name)
        logger.info('Services stats data pushed to maria db')
        up_services = self.__get_service_list(self.__data)
        services_down_maintain_false, services_down_maintain_true = SelfHeal(
            self.services_down_after_restart).get_services_stopped_names(
                self.services_stopped)
        up_services_with_maintain_true = self.__get_services_uplist_with_maintain_false(
            up_services, True)
        logger.info(
            "Services which are up: %s , Services Down with Maintain false %s , Services Down with Maintain True %s ",
            up_services, services_down_maintain_false,
            services_down_maintain_true)
        if up_services:
            up_services_with_maintain_false = self.__get_services_uplist_with_maintain_false(
                up_services, False)
            up_services_with_maintain_true = self.__get_services_uplist_with_maintain_false(
                up_services, True)
            logger.info("up_services_with_maintain_false is %s ",
                        up_services_with_maintain_false)
            if up_services_with_maintain_false:
                Processor.__get_query_response(up_services_with_maintain_false)
                self_heal_services, self.services_down_after_restart, services_stopped_maintain_true = SelfHeal(
                    self.services_down_after_restart
                ).process_self_heal_services(Processor.results,
                                             self.services_stopped,
                                             up_services_with_maintain_true)
            else:
                logger.info("All Services are in maintenance mode!!!!")
                exit(0)
            return services_stopped_maintain_true, self_heal_services, self.services_down_after_restart
        elif services_down_maintain_false:
            self_heal_services, self.services_down_after_restart, services_stopped_maintain_true = SelfHeal(
                self.services_down_after_restart).process_self_heal_services(
                    Processor.results, self.services_stopped,
                    up_services_with_maintain_true)
            return services_stopped_maintain_true, self_heal_services, self.services_down_after_restart
        else:
            return services_down_maintain_true, None, None

    def __get_services_uplist_with_maintain_false(self, up_services, state):
        return [
            service for service in up_services
            if service['Maintenance Mode'] == state
        ]

    def __get_service_list(self, services):
        result = []
        for serviceName in spark_services:
            data = self.__check_status(services, serviceName)
            if data: result.append(data)
        return result

    def __check_status(self, services, serviceName):
        for service in services['items']:
            if service['name'] == serviceName:
                if service['serviceState'] == 'STARTED':
                    return self.__format_data(service)
                else:
                    self.services_stopped.append(self.__format_data(service))

    def __format_data(self, service):
        data = {}
        data['Date'] = DateTimeUtil.now().strftime(DateTimeUtil.DATE_FORMAT)
        data['Name'] = service['name']
        data['Service State'] = service['serviceState']
        data['Maintenance Mode'] = service['maintenanceMode']
        return data

    @staticmethod
    @multiprocess(timeout=360)
    def __get_query_response(service_name):
        spark_types_dict = {
            'spark2_thrift1': 'SPARK2_THRIFT1_JDBC_URL',
            'spark2_thrift2': 'SPARK2_THRIFT2_JDBC_URL',
            'spark2_thrift3': 'SPARK2_THRIFT3_JDBC_URL'
        }
        service = service_name['Name']
        logger.debug("Service Name has %s", service)
        spark_thrift_server_key = spark_types_dict[service]
        logger.debug("spark_thrift_server_key is %s", spark_thrift_server_key)
        Processor.results.append(
            Processor.__execute_query(spark_thrift_server_key, service))

    @staticmethod
    def __execute_query(spark_thrift_server_key, service):
        res = Processor.__process_query(spark_thrift_server_key, service)
        if res:
            return service

    @staticmethod
    def __process_query(spark_thrift_server_key, service):
        sql = "select count(*) from cemod.tab"
        result = DbConnection().get_spark_connection_and_execute(
            sql, spark_thrift_server_key)
        logger.info("Result for spark %s is %s ", spark_thrift_server_key,
                    result)
        return result


class SelfHeal:
    def __init__(self, services_down_after_restart):
        self.service_down_after_restart = services_down_after_restart
        self.spark_service_queue_map = {
            'spark2_thrift1': "root.nokia.ca4ci",
            'spark2_thrift2': "root.nokia.ca4ci.flexi-report",
            'spark2_thrift3': "root.nokia.ca4ci.API-Q"
        }

    def process_self_heal_services(self, results, services_stopped,
                                   up_services_with_maintain_true):
        services_stopped_maintain_false, services_stopped_maintain_true = self.get_services_stopped_names(
            services_stopped)
        if not results and not services_stopped:
            logger.info("Services %s are hung, self heal is required",
                        spark_services)
            services_for_self_heal = set(spark_services)
            self_heal_services = self._process_queue_info(
                services_for_self_heal)
            self.__trigger_restart(services_for_self_heal)
        elif services_stopped_maintain_false:
            logger.info(
                "Services %s are down with maintenance false, self heal is required",
                services_stopped_maintain_false)
            self_heal_services = services_stopped_maintain_false
            self.__trigger_restart(services_stopped_maintain_false)
        else:
            self_heal_services, self.service_down_after_restart, services_stopped_maintain_true = self.__handle_non_hung_non_maintain_false(
                results, services_stopped, up_services_with_maintain_true)
        return self_heal_services, self.service_down_after_restart, services_stopped_maintain_true

    def __handle_non_hung_non_maintain_false(self, results, services_stopped,
                                             up_services_with_maintain_true):
        up_service_name_with_true = self.__get_upservice_names_with_maintain_true(
            up_services_with_maintain_true)
        services_stopped_maintain_false, services_stopped_maintain_true = self.get_services_stopped_names(
            services_stopped)
        service_stop_results = set(results).union(
            services_stopped_maintain_false, services_stopped_maintain_true,
            up_service_name_with_true)
        self_heal_services = set(spark_services) - service_stop_results
        services_for_self_heal = self._process_queue_info(self_heal_services)
        return self.__check_self_heal_condition(
            services_for_self_heal, services_stopped_maintain_true)

    def _process_queue_info(self, self_heal_services):
        queue_utilization_high = 0
        queue_utilization_less = 0
        processed_self_heal_services = []
        queues_allocated_vcore = QueueInformation().get_allocated_vcore()
        queues_utilized_vcore = QueueInformation().get_utilized_vcore_from_mdb(
        )
        root_queue_utilization = self._process_root_queue_info(queues_utilized_vcore, queues_allocated_vcore)
        for self_heal_service in self_heal_services:
            queue_name = self.spark_service_queue_map[self_heal_service]
            allocated_vcore_value = int(
                queues_allocated_vcore['{0}'.format(queue_name)])
            utilized_vcore_values = queues_utilized_vcore[queue_name]
            allocated_vcore_value_with_perc = (allocated_vcore_value *
                                               20) / 100
            for utilized_vcore_value in utilized_vcore_values:
                if float(utilized_vcore_value
                         ) < allocated_vcore_value_with_perc:
                    queue_utilization_less += 1
                else:
                    queue_utilization_high += 1
            if (queue_utilization_high < queue_utilization_less) and (root_queue_utilization == 'queue_low'):
                processed_self_heal_services.append(self_heal_service)
        return processed_self_heal_services
    
    def _process_root_queue_info(self, queues_utilized_vcore, queues_allocated_vcore):
        root_queue_utilization_high =0
        root_queue_utilization_low = 0
        utilized_vcore_values = queues_utilized_vcore['root.nokia.ca4ci']
        allocated_vcore_value = int(
                queues_allocated_vcore['{0}'.format('root.nokia.ca4ci.portal')])
        allocated_vcore_value_with_perc = (allocated_vcore_value *
                                               20) / 100
        for utilized_vcore_value in utilized_vcore_values:
            if float(utilized_vcore_value) < allocated_vcore_value_with_perc:
                root_queue_utilization_low +=1
            else:
                root_queue_utilization_high +=1
        if root_queue_utilization_high < root_queue_utilization_low:
            return "queue_low"
        else:
            return "queue_high"

    def get_services_stopped_names(self, services_stopped):
        services_names_with_false, services_names_with_true = [], []
        for service in services_stopped:
            if service['Maintenance Mode'] == False:
                services_names_with_false.append(service['Name'])
            else:
                services_names_with_true.append(service['Name'])
        return set(services_names_with_false), set(services_names_with_true)

    def __get_upservice_names_with_maintain_true(self, up_services):
        return set([service['Name'] for service in up_services])

    def __check_self_heal_condition(self, self_heal_services,
                                    services_stopped_maintain_true):
        if self_heal_services:
            logger.info("Services %s for self heal", self_heal_services)
            self.__trigger_restart(self_heal_services)
            return self_heal_services, self.service_down_after_restart, services_stopped_maintain_true
        else:
            if services_stopped_maintain_true:
                return None, None, services_stopped_maintain_true
            logger.info(
                "All Services are up and no hung service found or Services are up and maintenance is true"
            )
            exit(0)

    def __trigger_restart(self, self_heal_services):
        self_heal_sevice_status_list = []
        for self_heal_service in self_heal_services:
            api_url = "https://{}:{}/api/{}/clusters/{}/services/{}/commands/restart".format(
                cloudera_ip, port, ClouderaApiVersion.get_api_version(),
                cluster_name, self_heal_service)
            restart_info = requests.post(api_url,
                                         verify=False,
                                         auth=(user_name, user_password),
                                         headers={
                                             "Content-Type": "application/json"
                                         }).content
            if json.loads(restart_info)['active']:
                self_heal_sevice_status_list.append(self_heal_service)
        logger.debug("self_heal_sevice_status_list is ",
                     self_heal_sevice_status_list)
        self.__check_service_status(self_heal_sevice_status_list)

    def __check_service_status(self, service_names):
        time.sleep(240)
        for service_name in service_names:
            service_api = "https://{}:{}/api/v1/clusters/{}/services/{}".format(
                cloudera_ip, port, cluster_name, service_name)
            services_info = requests.get(service_api,
                                         verify=False,
                                         auth=(user_name, user_password))
            services_api_response = json.loads(services_info.content)
            if services_api_response['serviceState'] != "STARTED":
                self.service_down_after_restart.append({
                    'Name':
                    service_name,
                    'Service State':
                    services_api_response['serviceState']
                })


class QueueInformation:
    def get_allocated_vcore(self):
        queue_allocated_vcore = {}
        html = fromstring(get_vcore_output("selfheal"))
        th = html.xpath(
            '//table//th[@class="ui-state-default"]//text() | //table//tr[@class="even"]//text()[normalize-space()]'
        )
        for index, value in enumerate(th):
            if "root.nokia.ca4ci.flexi" in value or "root.nokia.ca4ci.API-Q" in value or "root.nokia.ca4ci.portal" in value:
                queue_allocated_vcore[value.strip().split(" ")[0].strip(
                    "'")] = th[index + 8].strip().split(",")[1].split(
                        ":")[1].split(">")[0]
        return queue_allocated_vcore

    def get_utilized_vcore_from_mdb(self):
        queue_utilzed_vcore = {
            "root.nokia.ca4ci.flexi-report": [],
            "root.nokia.ca4ci.API-Q": [],
            "root.nokia.ca4ci": []
        }
        query = PropertyFileUtil('queueInfo',
                                 'mariadbSqlSection').getValueForKey()
        minDate, maxDate = DateTimeUtil.get_last_15min_date(
        ), DateTimeUtil.now().strftime("%Y-%m-%d %H:%M:%S")
        for queue_name in queue_utilzed_vcore.keys():
            final_query = query.replace("minDate", minDate).replace(
                "maxDate", maxDate).replace("queueName", queue_name)
            logger.info("Processing for the queue %s", queue_name)
            logger.info("Final Query is %s", final_query)
            queue_utilzed_vcore[queue_name] = DbConnection(
            ).getMonitoringConnectionObject(final_query).split("\n")
        logger.info("Utilized Vcore is %s", queue_utilzed_vcore)
        return queue_utilzed_vcore


class Presenter:

    SERVICES_DOWN_KEY = AlarmKeys.SERVICES_DOWN.value
    SELF_HEAL_KEY = AlarmKeys.SELF_HEALED_HUNG_SERVICES.value

    def __init__(self, data):
        self._services_down, self._self_heal_services, self._services_stopped_after_self_heal = data

    def present(self):
        if len(self._services_down) > 0:
            self.__frame_html_and_send(
                ["List of Services"] + list(self._services_down),
                Presenter.SERVICES_DOWN_KEY, 'Services Down For Maintenance',
                "Spark Thrift Services Down")
        if self._self_heal_services:
            if len(self._self_heal_services) > 0:
                self.__frame_html_and_send(
                    self._self_heal_services, Presenter.SELF_HEAL_KEY,
                    'Self Healed Services - No action required',
                    "Spark Thrift Services Self Healed")
        else:
            logger.info("No services to self Heal")

        if self._services_stopped_after_self_heal:
            if len(self._services_stopped_after_self_heal) > 0:
                self.__frame_html_and_send(
                    self._services_stopped_after_self_heal,
                    Presenter.SERVICES_DOWN_KEY, 'Services Down',
                    "Spark Thrift Services not Self Healed")
        else:
            logger.info("No Services down after self heal")

    def __frame_html_and_send(self, json_list_for_alert, alarm_key, message,
                              subject):
        severity = SnmpAlarm.get_severity_for_email(alarm_key)
        if message == "Self Healed Services - No action required":
            html = HtmlUtil().generateHtmlFromList(
                message,
                ["List of Services Self Healed"] + list(json_list_for_alert))
            EmailUtils().frameEmailAndSend(
                "[{0} ALERT]: {1}".format(severity, subject), html,
                Presenter.SELF_HEAL_KEY)
            SnmpAlarm.send_alarm(Presenter.SELF_HEAL_KEY,
                                 list(json_list_for_alert))
        else:
            if self._services_down:
                severity = "WARNING"
            html = HtmlUtil().generateHtmlFromList(message,
                                                   json_list_for_alert)
            SnmpAlarm.send_alarm(Presenter.SERVICES_DOWN_KEY,
                                 list(json_list_for_alert))
            EmailUtils().frameEmailAndSend(
                "[{0} ALERT]: {1}".format(severity, subject), html,
                Presenter.SERVICES_DOWN_KEY)


if __name__ == "__main__":
    main()
