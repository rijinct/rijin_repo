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
from loggerUtil import loggerUtil
import xml_util
import xml_to_csv_converter
import constants
from getOperationalMetadata import getPreviousDayJobsRunStats
from postgres_connection import PostgresConnection
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(", "(\"").replace(")", "\")"))
currentDate= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
scriptName = os.path.basename(sys.argv[0]).replace('.py','')
logFile= '{scriptName}_{currentDate}'.format(scriptName=scriptName,currentDate=currentDate) 
LOGGER = loggerUtil.__call__().get_logger(logFile)

def writePostgresOutputInXml(rows):
	outputFile = '{outputDirectory}/{inputArgument}Metadata_{currentDate}.xml'.format(outputDirectory=outputDirectory,inputArgument=inputArgument,currentDate=currentDate)
	with open(outputFile, 'w') as f:
		for row in rows:
			f.write("%s" % str(row).replace('\\n', '').replace('(\'', '').replace('\',)', '').replace('\\', ''))        
	LOGGER.info("Successfully exported Postgres metadata.")
	LOGGER.info("Transformation of xml in progress...")
	xml_util.modify_and_regenerate_xml(outputFile, outputFile, constants.TAG_XPATH_LOC, constants.EXECUTOR, constants.SUBPROC_SCRIPT)
	xml_util.generate_ultimateSourceField_to_xml(outputFile, outputFile, constants.XML_DF_COLUMNS)
	xml_util.transform_xml(process_xsl_filename, outputFile, outputFile, 'w')
	LOGGER.info("Successfully transformed the XML file to {0}".format(xml_util.get_absolute_filename(outputFile)))

def validateArguments():
	if len(sys.argv) < 2:
		LOGGER.info("ERROR: Missing or invalid arguments!")
		LOGGER.info("Usage: {0} <TECHNICAL/OPERATIONAL/BUSINESS/DIMENSION>".format(sys.argv[0]))
		sys.exit(1)
	else:
		global inputArgument,metaDirectory,process_xsl_filename,outputDirectory
		inputArgument = sys.argv[1].lower()
		metaDirectory= '/opt/nsn/ngdb/monitoring/metadata/{inputArgument}/sql'.format(inputArgument=inputArgument)
		outputDirectory = '/opt/nsn/ngdb/monitoring/metadata/{inputArgument}/output'.format(inputArgument=inputArgument)
		if not os.path.exists(outputDirectory):
			os.makedirs(outputDirectory)
			os.system('chown ngdb:ninstall {}'.format(outputDirectory))		
		process_xsl_filename=xml_util.get_custom_default_filename(constants.technical_meta_xsl_custom,constants.technical_meta_xsl_default) 

def writeHiveOutputInXml(query,outputFile):
	cmd_processmonitor = 'beeline -u \'%s\'  --silent=true --showHeader=false --outputformat=xmlelements -e \"SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring;set hive.exec.reducers.max=1;%s;\"' % (cemod_hive_url, query)
	os.system('%s > %s' %(cmd_processmonitor,outputFile))

def writeJsonOutputInXml(file, outputFileName):
	uniXmlFile = outputFileName.replace('.xml', '_temp.xml')
	LOGGER.info("Processing the uni file {0}".format(file))
	with open(file) as json_file:
		xml = dicttoxml(json.load(json_file),attr_type=False)
		with open(uniXmlFile,'wb') as f:
			f.write(xml)
		xml_util.transform_xml(process_xsl_filename, uniXmlFile, outputFileName, 'a')
	os.remove(uniXmlFile)

def transformUNIFilesToXML():
	outputFile = '{outputDirectory}/{inputArgument}Metadata.xml'.format(outputDirectory=outputDirectory,inputArgument=inputArgument)
	UniversePath =  constants.UNI_FILES_DIR
	for dirpath, subdirs, files in os.walk(UniversePath):
		for file in files:
			if '.uni' in file:
				writeJsonOutputInXml(os.path.join(dirpath,file), outputFile)
	formatXMLFile(outputFile)
	LOGGER.info('Successfully exported businessMetadata in xml')  

def formatXMLFile(outputFile):
	formattedOutputFile = '{outputDirectory}/{inputArgument}Metadata_{currentDate}.xml'.format(outputDirectory=outputDirectory,inputArgument=inputArgument, currentDate=currentDate)
	xml_util.append_root_to_xml(outputFile)
	os.rename(outputFile,formattedOutputFile)
	xml_util.modify_and_regenerate_xml(formattedOutputFile, formattedOutputFile, constants.BUSINESS_METADATA_TAG_XPATH_LOC, constants.EXECUTOR, constants.SUBPROC_SCRIPT)
	LOGGER.info("Successfully transformed the XML file to {0}".format(xml_util.get_absolute_filename(formattedOutputFile)))
	convert_xml_to_csv(formattedOutputFile,formattedOutputFile.replace('.xml', '.csv'))
    
def convert_xml_to_csv(xml_file,csv_file):
	xml_to_csv_obj = xml_to_csv_converter.xmlTocsv(xml_file,csv_file,constants.csv_columns)    
	xml_to_csv_obj.create_df_from_xml()

	fileName = constants.kpi_file
	if(len(fileName) != 0): 
		filter_list = [line.rstrip('\n') for line in open(fileName)]   
		xml_to_csv_obj.filter_df_rows('kpiName',filter_list)

	xml_to_csv_obj.create_csv_from_df()
	LOGGER.info("Successfully exported to businessMetadata to csv file : {0}".format(xml_util.get_absolute_filename(csv_file)))


def get_table_filter_condition():
	pattern_constructor = ''
	for table in constants.TABLE_PATTERN:
		table_condition = "m2.tablename like ''{}''".format(table)
		if (len(pattern_constructor) == 0):
			pattern_constructor = table_condition
		else:
			pattern_constructor = pattern_constructor + ' ' + 'or' + ' ' + table_condition
	return pattern_constructor


def executePostgresQueryToXML():
	inputQueryFile = '{metaDirectory}/{inputArgument}Metadata.sql'.format(metaDirectory=metaDirectory, inputArgument=inputArgument)
	if (len(constants.TABLE_PATTERN) == 0):
		filter_condition = ''
	else:
		filter_condition = get_table_filter_condition()
	query = open(inputQueryFile, 'r').read().replace('#FILTER_CONDITION', filter_condition)
	try:
		con = psycopg2.connect(database=cemod_sdk_db_name, user=cemod_application_sdk_database_linux_user,
                           password="", host=cemod_postgres_sdk_fip_active_host,
                           port=cemod_application_sdk_db_port)
		cur = con.cursor()
		cur.execute(query)
		rows = cur.fetchall()
		writePostgresOutputInXml(rows)
	except:
		LOGGER.info("Error executing query to export")
		LOGGER.info(traceback.format_exc())
	finally:    
		con.close()

def generateXMLsForDimensions():
	for dirpath, dirnames, files in os.walk(metaDirectory): 
		if cemod_application_adaptation_FixedBroadBand_status == 'no':
			dirnames.remove('FixedLine')
		for file in files:
			if '.sql' in file:
				executeHiveSql(os.path.join(dirpath,file))

def executeHiveSql(file):
	LOGGER.info('Processing SQL file {0}'.format(file))
	outputFileName = ntpath.basename(file).replace('.sql','').strip()
	outputFile= '{outputDirectory}/{outputFileName}_{currentDate}.xml'.format(outputDirectory=outputDirectory,outputFileName=outputFileName,currentDate=currentDate)
	with open(file) as sqlFile:
		writeHiveOutputInXml(sqlFile.readline(),outputFile)
		LOGGER.info("Successfully transformed the XML file to {0}".format(xml_util.get_absolute_filename(outputFile)))

def generateOperationalResults():
		process_xsl_filename=xml_util.get_custom_default_filename(constants.operational_meta_xsl_custom,constants.operational_meta_xsl_default)
		getPreviousDayJobsRunStats(metaDirectory,outputDirectory,process_xsl_filename,logFile)

def main():
	validateArguments()
	if inputArgument == 'business':
		transformUNIFilesToXML()
	elif inputArgument == 'dimension':
		LOGGER.info('Generating XML with dimension details')
		generateXMLsForDimensions()
	elif inputArgument == 'operational':
		generateOperationalResults()
	else:
		executePostgresQueryToXML()

if __name__ == "__main__":
		main()

