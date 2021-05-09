#! /usr/bin/python
#####################################################################################
#####################################################################################
# (c)2016 NOKIA
# Author:  SDK Team
# Version: 0.1
# Purpose: This script exports Technical, Operational, Business metadata in xml format
# Date:    11-09-2019
#####################################################################################

import psycopg2
import lxml.etree
import sys
import traceback
import os
import json
import ntpath
from dicttoxml import dicttoxml
from datetime import datetime
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from logger_util import *
import xml_util
import xml_to_csv_converter
import constants
from getOperationalMetadata import getPreviousDayJobsRunStats
from dbConnectionManager import DbConnection
currentDate= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
scriptName = os.path.basename(sys.argv[0]).replace('.py','')
from config_map_util import ConfigMapUtil

create_logger()
LOGGER = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
LOGGER.setLevel(get_logging_instance(default_log_level))

def writePostgresOutputInXml(rows):
	outputFile = '{outputDirectory}/{inputArgument}Results_{currentDate}.xml'.format(outputDirectory=outputDirectory,inputArgument=inputArgument,currentDate=currentDate)
	with open(outputFile, 'w') as f:
		for row in rows:
			f.write("%s" % str(row).replace('\\n', '').replace('(\'', '').replace('\',)', '').replace('\\', ''))        
	LOGGER.info("Successfully exported Postgres metadata.")
	LOGGER.info("Transformation of xml in progress using xsl: %s",process_xsl_filename)
	xml_util.modify_and_regenerate_xml(outputFile, outputFile, constants.TAG_XPATH_LOC, constants.EXECUTOR, constants.SUBPROC_SCRIPT)
	xml_util.generate_ultimateSourceField_to_xml(outputFile, outputFile, constants.XML_DF_COLUMNS)
	xml_util.transform_xml(process_xsl_filename, outputFile, outputFile, 'w')
	LOGGER.info("Successfully transformed the XML file to %s",xml_util.get_absolute_filename(outputFile))

def validateArguments():
	if len(sys.argv) < 2:
		LOGGER.info("ERROR: Missing or invalid arguments!")
		LOGGER.info("Usage: %s <TECHNICAL/OPERATIONAL>",sys.argv[0])
		sys.exit(1)
	else:
		global inputArgument,metaDirectory,process_xsl_filename,outputDirectory
		inputArgument = sys.argv[1].lower()
		metaDirectory= '/opt/nsn/ngdb/monitoring/cad/metadata/{inputArgument}/sql'.format(inputArgument=inputArgument)
		outputDirectory = '/opt/nsn/ngdb/monitoring/output/cad/{inputArgument}'.format(inputArgument=inputArgument)
		if not os.path.exists(outputDirectory):
			os.makedirs(outputDirectory)
			os.system('chown ngdb:ninstall {}'.format(outputDirectory))
		process_xsl_filename=xml_util.get_custom_default_filename(constants.technical_meta_xsl_custom,constants.technical_meta_xsl_default) 

def writeHiveOutputInXml(query,outputFile):
	cmd_processmonitor = 'beeline -u \'%s\'  --silent=true --showHeader=false --outputformat=xmlelements -e \"SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring;set hive.exec.reducers.max=1;%s;\"' % (cemod_hive_url, query)
	os.system('%s > %s' %(cmd_processmonitor,outputFile))

def writeJsonOutputInXml(file, outputFileName):
	uniXmlFile = outputFileName.replace('.xml', '_temp.xml')
	LOGGER.info("Processing the uni file %s",file)
	with open(file) as json_file:
		xml = dicttoxml(json.load(json_file),attr_type=False)
		with open(uniXmlFile,'wb') as f:
			f.write(xml)
		xml_util.transform_xml(process_xsl_filename, uniXmlFile, outputFileName, 'a')
	os.remove(uniXmlFile)

def get_table_filter_condition():
	pattern_constructor = ''
	filter_condition = ConfigMapUtil.retrieve_table_pattern()
	LOGGER.info('Exporting tables: %s as configured in input',filter_condition)
	for table in filter_condition:
		table_condition = "m2.tablename like ''{}''".format(table)
		if (len(pattern_constructor) == 0):
			pattern_constructor = table_condition
		else:
			pattern_constructor = pattern_constructor + ' ' + 'or' + ' ' + table_condition
	return pattern_constructor

def executePostgresQueryToXML():
	inputQueryFile = '{metaDirectory}/{inputArgument}Metadata.sql'.format(metaDirectory=metaDirectory, inputArgument=inputArgument)
	filter_condition = get_table_filter_condition()
	query = open(inputQueryFile, 'r').read().replace('#FILTER_CONDITION', filter_condition)
	try:
		rows = DbConnection().getConnectionAndExecuteSql(query, 'postgres')
		writePostgresOutputInXml(rows)
	except:
		LOGGER.info("Error executing query to export")
		LOGGER.info(traceback.format_exc())

def executeHiveSql(file):
	LOGGER.info('Processing SQL file %s',file)
	outputFileName = ntpath.basename(file).replace('.sql','').strip()
	outputFile= '{outputDirectory}/{outputFileName}_{currentDate}.xml'.format(outputDirectory=outputDirectory,outputFileName=outputFileName,currentDate=currentDate)
	with open(file) as sqlFile:
		writeHiveOutputInXml(sqlFile.readline(),outputFile)
		LOGGER.info("Successfully transformed the XML file to %s",xml_util.get_absolute_filename(outputFile))

def generateOperationalResults():
		process_xsl_filename=xml_util.get_custom_default_filename(constants.operational_meta_xsl_custom,constants.operational_meta_xsl_default)
		getPreviousDayJobsRunStats(metaDirectory,outputDirectory,process_xsl_filename)

def main():
	validateArguments()
	if inputArgument == 'operational':
		generateOperationalResults()
	else:
		executePostgresQueryToXML()

if __name__ == "__main__":
		main()

