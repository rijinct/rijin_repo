#! /usr/bin/python
#####################################################################################
#####################################################################################
# (c)2016 NOKIA
# Author:  SDK Team
# Version: 0.1
# Purpose: This script exports Operational metadata in xml format
# Date:    02-06-2020
#####################################################################################

import sys
import os
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
from datetime import timedelta
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from logger_util import *
import constants
import xml_util
import common_constants
from dbConnectionManager import DbConnection
from dbConnectionUtil import DbConnectionUtil
from config_map_util import ConfigMapUtil

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

currentDate= datetime.now()
previousDate = currentDate - timedelta(days = 1) 
scriptName = 'operationalResults'

def fetch_records_as_list(sql):
    result = DbConnection().getConnectionAndExecuteSql(sql, 'postgres')
    topology_table_mapping = {}
    for row in result:
        topology_table_mapping[row[1]] = row[0]
    return topology_table_mapping
    
def get_table_topology_mapping():
    result = fetch_records_as_list(common_constants.ADAPTATION_TOPOLOGY_MAPPING_QUERY)
    return result

def retrieveRecordCount(df_processMonitor,replaceString,columnName):
    df_sliced = df_processMonitor[df_processMonitor['job_name'].str.contains(replaceString)]
    df_sliced = df_sliced.replace(regex=[replaceString], value='')
    df_sliced['job_name'] = df_sliced.iloc[:,-1]
    df_sliced['job_name'] = df_sliced['job_name'].map(get_table_topology_mapping()).fillna(df_sliced['job_name'])
    df_sliced[columnName]=df_sliced.sum(axis=1)
    total = {columnName: 'sum'}
    return df_sliced.groupby('job_name', as_index=False).aggregate(total)

def getUsageJobStats():
    global df_usageJobs, doesUsageStatsExist
    usageFileName = 'total_count_usage_{0}'.format(currentDate.strftime('%Y%m%d') + '.csv')
    usageTableCsv = constants.monitoring_output_dir+ 'tableCount_Usage/'+usageFileName
    if not (os.path.exists(usageTableCsv)):
        logger.error('File {0} not found'.format(usageTableCsv))
        doesUsageStatsExist = False
        return
    doesUsageStatsExist = True
    logger.info('Reading CSV {0}'.format(usageTableCsv)) 
    df_usageJobs = pd.read_csv(usageTableCsv).drop(['Date','DateInLocalTZ','Hour','Mobile_Usage_Per_Hour','FL_Usage_Per_Hour'],axis=1)
    
    df_usageJobs=df_usageJobs.iloc[-1,:]
    df_usageJobs = pd.DataFrame(df_usageJobs)
    df_usageJobs['job_name']=df_usageJobs.index.str.upper()

    df_usageJobs['recordsRejected'] = 0
    df_usageJobs['recordsDuplicated'] = 0
    df_usageJobs = df_usageJobs.rename(columns={24: "recordsProcessed"})

    df_usageJobs = df_usageJobs.reindex(columns=['job_name','recordsProcessed','recordsRejected','recordsDuplicated'])
    
    df_usageJobs = df_usageJobs.replace(regex=['US_'], value='Usage_').replace(regex=['_1$'], value='_1_LoadJob')
    getUsageJobsStats()
    
def convertToInt(df):
    df = df.astype({'recordsProcessed':'int', 'recordsRejected':'int','recordsDuplicated':'int'})
    return df
        
def getUsageJobsStats():
    global df_usage_jobs
    total = {'recordsProcessed': 'sum','recordsRejected': 'sum','recordsDuplicated': 'sum'}
    df_usage_jobs = mergeWithEtlJobStats(df_usageJobs,'Usage')

def mergeWithEtlJobStats(df_job_stats,jobPattern):
    inputQueryFile = '{metadataDir}/filterUsageEntityJobsQuery.sql'.format(metadataDir=metadataDir)
    query = open(inputQueryFile, 'r').read()
    df_etl_usage_entity = DbConnectionUtil().getDataframeFromSql(query)
    df_jobs = pd.merge(df_etl_usage_entity, df_job_stats, on='job_name',how='left',sort='False')
    df_jobs = df_jobs.loc[df_jobs["job_name"].str.startswith(jobPattern, na=False)]
    return df_jobs
    
def getAggregateJobsStats():
    global df_aggregation_jobs    
    inputQueryFile = '{metadataDir}/filterAggJobsQuery.sql'.format(metadataDir=metadataDir)
    query = open(inputQueryFile, 'r').read()
    df_aggregation_jobs = DbConnectionUtil().getDataframeFromSql(query)
    
    df_aggregation_jobs['recordsProcessed'] = df_aggregation_jobs['description'].apply(retrieveProcessedRecords)
    df_aggregation_jobs = df_aggregation_jobs.drop('description', axis = 1) 

    df_aggregation_jobs['recordsRejected']=0
    df_aggregation_jobs['recordsDuplicated']=0
    df_aggregation_jobs = df_aggregation_jobs.reindex(columns=['job_name','start_time','status','recordsProcessed','recordsRejected','recordsDuplicated'])

def retrieveProcessedRecords(description):
    if(description !=None and 'Aggregation is completed' in description):
        return (description.rsplit(':',1)[1]).strip()
    else:
        return 0
        
def getEntityJobStats():
    global df_entityJobs, doesEntityStatsExist
    dimCountFileName = 'dimensionCount_{0}'.format(currentDate.strftime('%Y-%m-%d') + '.csv')
    dimCsv = constants.monitoring_output_dir+ 'dimensionCount/'+dimCountFileName
    if not (os.path.exists(dimCsv)):
        logger.error('File {0} not found'.format(dimCsv))
        doesEntityStatsExist = False
        return
    doesEntityStatsExist = True
    logger.info('Reading CSV {0}'.format(dimCsv)) 
    df_entity_csv = pd.read_csv(dimCsv)
    df_entity_csv.columns=['start_time','job_name','recordsProcessed']
    df_entity_csv['job_name'] = df_entity_csv['job_name'].str.upper().replace(regex=['ES_'], value='Entity_')
    df_entity_csv['job_name'] = df_entity_csv['job_name']+"_CorrelationJob"
    df_entity_csv = df_entity_csv.drop(['start_time'],axis=1)
    df_entity_corrupt = getCorruptedRecordsCount()
    if not df_entity_corrupt.empty:
        df_entityJobs = pd.merge(df_entity_csv, df_entity_corrupt, on='job_name',how='left',sort='False')
    else:
        df_entityJobs = df_entity_csv
        df_entityJobs['recordsDuplicated'] = 0
    df_entityJobs['recordsRejected'] = 0
    df_entityJobs.fillna(0, inplace=True)
    doesEntityStatsExist = True
    df_entityJobs = mergeWithEtlJobStats(df_entityJobs,'Entity')
    df_entityJobs['recordsProcessed'].mask(df_entityJobs['status'] == 'Fail', 0, inplace=True)

def getCorruptedRecordsCount():
    entity_job_duplicate_count_mapdict = {}
    for dirpath, dirnames, files in os.walk('/mnt/staging/archive/'): 
        for file in files:
            if 'duplicate_1.dis' in file and not file.startswith('.'):
                duplicate_rec_count = sum(1 for line in open(os.path.join(dirpath,file)) if line != "\n")
                jobName = "Entity_"+file.split("_1")[0]+"_1_CorrelationJob"
                entity_job_duplicate_count_mapdict[jobName]=duplicate_rec_count
    df_entity_corrupt = pd.DataFrame(list(entity_job_duplicate_count_mapdict.items()))
    if not df_entity_corrupt.empty:
        df_entity_corrupt.columns = ['job_name','recordsDuplicated']
    return df_entity_corrupt
    
def to_xml(df,filename,outputDir):
    res = ' '.join(df.apply(row_to_xml, axis=1))  
    with open(filename, 'w') as f:
        f.write(res)
    with open(filename, 'r') as f, open('{outputDir}/temp.xml'.format(outputDir=outputDir), 'w') as g:
        g.write('<Jobs>{}</Jobs>'.format(f.read()))
    os.remove(filename)

def row_to_xml(row):
    xml = '<row><job_name>{0}</job_name><start_time>{1}</start_time><status>{2}</status><recordsProcessed>{3}</recordsProcessed><recordsRejected>{4}</recordsRejected><recordsDuplicated>{5}</recordsDuplicated></row>'.format(row.job_name, row.start_time, row.status,row.recordsProcessed,row.recordsRejected,row.recordsDuplicated)
    return xml
    
def filterJobs(df):
    job_list = ConfigMapUtil.retrieve_export_jobs()
    if '' in job_list:
        return df
    else:
        logger.info("Filtering only {} jobs to export:".format(job_list))
        return df[np.isin(df['job_name'], job_list)]
    
def getPreviousDayJobsRunStats(metaDir,outputDir,process_xsl_filename):
    global metadataDir, df, outputDirectory
    metadataDir = metaDir
    outputFile= '{outputDir}/{scriptName}_{currentDate}.xml'.format(outputDir=outputDir,scriptName=scriptName,currentDate=currentDate.strftime("%Y-%m-%d_%H-%M-%S")) 
    
    getAggregateJobsStats()
    getUsageJobStats()
    getEntityJobStats()
    
    df = pd.DataFrame(df_aggregation_jobs)
    if doesEntityStatsExist:
        df = df.append(df_entityJobs,sort=False)
    if doesUsageStatsExist:
        df = df.append(df_usage_jobs,sort=False)
        
    df = df.fillna(0)
    df = df.replace(to_replace="null",value=0)
    df = filterJobs(df)
    df = convertToInt(df)
    
    if not df.empty:
        logger.info("Processing the xml file for operationalMetadata")
        to_xml(df,'{outputDir}/tempfile.xml'.format(outputDir=outputDir),outputDir)
        xml_util.transform_xml(process_xsl_filename, '{outputDir}/temp.xml'.format(outputDir=outputDir), '{outputFile}'.format(outputFile=outputFile), 'w')
        os.remove('{outputDir}/temp.xml'.format(outputDir=outputDir))
        logger.info("Operational Metadata is successfully exported to file: {0}".format(outputFile))
    else:
        logger.info("No data to export for the date: {0}".format(previousDate.strftime("%Y-%m-%d")))
