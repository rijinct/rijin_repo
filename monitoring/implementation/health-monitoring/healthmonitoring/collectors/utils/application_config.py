from xml.dom import minidom

from healthmonitoring.collectors import _LocalLogger
from healthmonitoring.framework import config

logger = _LocalLogger.get_logger(__name__)


class ApplicationConfig(object):

    _parsed_xml = None

    def __init__(self):
        if not ApplicationConfig._parsed_xml:
            logger.info('Reading application configuration xml')
            ApplicationConfig._parsed_xml = minidom.parse(
                config.properties.get(
                    'DirectorySection', 'ApplicationConfig'))

    def get_kafka_adap_port(self, topology_name):
        logger.info('Fetching port numbers for %s', topology_name)
        kafka_adap_port_prop = \
            ApplicationConfig._parsed_xml.getElementsByTagName(
                'Kafka-adap-port')[0].getElementsByTagName('property')
        for index in range(len(kafka_adap_port_prop)):
            if topology_name == kafka_adap_port_prop[index].\
                    attributes['name'].value:
                return kafka_adap_port_prop[index].\
                        attributes['value'].value
