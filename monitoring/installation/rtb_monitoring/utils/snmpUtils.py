#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  
# Version: 0.1
# Purpose: This class parses the XML for SNMP details
#
# Date: 23-01-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality
#
# 2.
#############################################################################

from xml.dom import minidom

class SnmpUtils:

        def getSnmpDetails(self):
                commonXml = "/opt/nsn/ngdb/ifw/etc/common/common_config.xml"
                xmlparser = minidom.parse(commonXml)
                snmpTag = xmlparser.getElementsByTagName('SNMP')
                propertySnmpTag = snmpTag[0].getElementsByTagName('property')
                for property in propertySnmpTag:
                        for propertynum in range(len(propertySnmpTag)):
                                if "ManagerIP" == propertySnmpTag[propertynum].attributes['name'].value:
                                        snmpIp = propertySnmpTag[propertynum].attributes['value'].value
                                if "ManagerPort" == propertySnmpTag[propertynum].attributes['name'].value:
                                        port = propertySnmpTag[propertynum].attributes['value'].value
                                if "Community" == propertySnmpTag[propertynum].attributes['name'].value:
                                        community = propertySnmpTag[propertynum].attributes['value'].value
                return snmpIp,port,community


