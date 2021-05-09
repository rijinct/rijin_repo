from xml.dom.minidom import parse


class MonitoringUtils:

    @staticmethod
    def clamp(lower_bound, value, upper_bound):
        return max(lower_bound, min(value, upper_bound))

    @staticmethod
    def convert_to_tb(bytes):
        return bytes / (2**40)

class XmlParserUtils:

    MONITORING_XML_PATH = r'/opt/nsn/ngdb/monitoring/conf/monitoring.xml'

    @staticmethod
    def get_topologies_from_xml():
        parent_tag = XmlParserUtils.extract_parent_tag('Topologies')
        topology = XmlParserUtils.extract_child_tags('Topologies', 'Topology')
        primary_vals = {
            attr: value
            for attr, value in parent_tag.attributes.items()
        }
        return XmlParserUtils._process_topology_tag(topology, primary_vals)

    @staticmethod
    def _process_topology_tag(topology_tag, primary_vals):
        info = {}
        for element in topology_tag:
            topology = primary_vals.copy()
            topology.update(
                {attr: value
                 for (attr, value) in element.attributes.items()})
            info[topology['Name']] = topology
        return info

    @staticmethod
    def get_summary_report_content():
        summary_report_content = []
        xml = parse(XmlParserUtils.MONITORING_XML_PATH)
        parent_tag = xml.getElementsByTagName('SummaryReport')[0]
        properties = parent_tag.getElementsByTagName('property')
        for property in properties:
            summary_report_content.append(dict(property.attributes.items()))
        return summary_report_content

    @staticmethod
    def extract_parent_tag(parent):
        xml = parse(XmlParserUtils.MONITORING_XML_PATH)
        return xml.getElementsByTagName(parent)[0]

    @staticmethod
    def extract_child_tags(parent, child):
        parent_tag = XmlParserUtils.extract_parent_tag(parent)
        return parent_tag.getElementsByTagName(child)
    
    @staticmethod
    def get_hadoop_threshold():
        hadoop_threshold_content = []
        properties = XmlParserUtils.extract_child_tags('HADOOP_THRESHOLD','property')
        for property in properties:
            hadoop_threshold_content.append(dict(property.attributes.items()))
        return hadoop_threshold_content