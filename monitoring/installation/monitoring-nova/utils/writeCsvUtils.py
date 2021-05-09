#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This class writes header (if required) and content to required
# CSV on instance creation
# Date:   05-12-2017
#############################################################################
#############################################################################
# Code Modification History
# 1.
#
# 2.
#############################################################################
import configparser, os, subprocess
from propertyFileUtil import PropertyFileUtil

class CsvWriter:

        def __init__(self, monitoringType, fileName, content=None, header=None ):
                self.monitoringType = monitoringType
                self.fileName = fileName
                self.configLoad = configparser.RawConfigParser()
                self.configLoad.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'monitoring.properties'))
                self.outputDirectory = self.getOutputDirectory()
                if header is not None:
                        self.writeHeaderAlways()
                else:
                        self.writeHeaderIfRequired()
                if content is not None:
                        monitoringFile = self.outputDirectory + self.fileName
                        with open(monitoringFile,'a') as outputCsvWriter:
                                outputCsvWriter.write(content+'\n')

        def getOutputDirectory(self,monitoringType=None):
                monitoringTypeValue = self.configLoad.get('DirectorySection',self.monitoringType)
                if not os.path.exists(monitoringTypeValue):
                        subprocess.getoutput('sudo chown ngdb:ninstall {0}'.format(PropertyFileUtil('monitoringOutputDirectory','DirectorySection').getValueForKey()))
                        os.makedirs(monitoringTypeValue)
                return monitoringTypeValue

        def writeHeaderIfRequired(self):
                monitoringFile = self.outputDirectory + self.fileName
                header = self.configLoad.get('HeaderSection',self.monitoringType)
                outputCsvWriter = open(monitoringFile,'a')
                if os.path.getsize(monitoringFile) == 0:
                                outputCsvWriter.write(header+'\n')
                                outputCsvWriter.close()
                                return True
                else:
                        return False

        def writeHeaderAlways(self):
                monitoringFile = self.outputDirectory + self.fileName
                header = self.configLoad.get('HeaderSection',self.monitoringType)
                outputCsvWriter = open(monitoringFile,'a')
                outputCsvWriter.write(header+'\n')
                outputCsvWriter.close()