#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This class loads properties & returns the value of the key
# from the properties file
# Date:   08-2-2019
#############################################################################
#############################################################################
# Code Modification History
# 1.
#
# 2.
#############################################################################
import ConfigParser, os

class PropertyFileUtil:

        def __init__(self, monitoringType, propertyFileSection):
                self.monitoringType = monitoringType
                self.propertyFileSection = propertyFileSection
                self.configLoad = ConfigParser.RawConfigParser()
                self.configLoad.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'monitoring.properties'))

        def getValueForKey(self):
                return self.configLoad.get(self.propertyFileSection,self.monitoringType)