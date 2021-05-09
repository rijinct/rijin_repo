#! /bin/bash
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This Post Installation Script for Monitoring RPM is to create directories
# and cron entries needed for all Monitoring scripts
# Date:    31-08-2016
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################
. ~/.bash_profile
source $NGDB_HOME/ifw/lib/application/utils/application_definition.sh
monitoringHome="/var/local/monitoring"
scriptsHome="/opt/nsn/ngdb/monitoring"
smtp_sender_emailID=`echo ${cemod_smtp_sender_emailIDs[@]} | sed 's/ /,/g'`
valPassed=$1

function directoriesCreation()
{
echo "Starting Post Install Steps..."
## Directories Creation ##
mkdir -p /mnt/healthMonitoring/output
mkdir -p $monitoringHome
echo "Creating soft link"
cd $monitoringHome
ln -s /mnt/healthMonitoring/output/ /var/local/monitoring/output
mkdir -p $monitoringHome/log
## Directory for backlog_stats.pl ##
mkdir -p $monitoringHome/output/backlogHadoop
## Directory for Disk space ##
mkdir -p $monitoringHome/output/diskSpaceUtilization
## Directories for QS ##
mkdir -p $monitoringHome/output/qsJobStatus_Day
mkdir -p $monitoringHome/output/qsJobStatus_Week
mkdir -p $monitoringHome/output/qsCache
## Directories for check Open File ##
mkdir -p $monitoringHome/output/checkOpenFile
mkdir -p $monitoringHome/output/openFiles/
## Directories for Queue Monitoring ##
mkdir -p $monitoringHome/output/capacityScheduler
## Directories for check  getRejectionRecords ##
mkdir -p $monitoringHome/output/RejectionRecords
## Directories for Table Counts ##
mkdir -p $monitoringHome/output/tableCount_Dimension
mkdir -p $monitoringHome/output/Dimension_Count
## Directories for TNPJobs ##
mkdir -p $monitoringHome/output/Tnp_Latency
## Directory for boundary status ##
mkdir -p $monitoringHome/output/boundaryStatus
## Directory for Portal Audit log ##
mkdir -p $monitoringHome/output/portalLog
##Directory for processMonitor ##
mkdir -p $monitoringHome/output/processMonitor
echo "Directories creation is successful"
##Directory creation for topology_status_check
mkdir -p $monitoringHome/output/topology_status
##Directory creation for unkown.pl to get unknown count
mkdir -p $monitoringHome/output/Unknown_check
## Directory creation for work ##
mkdir -p $monitoringHome/work
## Directory creation for getErrors
mkdir -p $monitoringHome/output/errorLogs
## Directory creation for checkURL
mkdir -p $monitoringHome/output/checkURL
##Directory creation for dalquerylog.sh and sparkPortalQueries.sh
mkdir -p $monitoringHome/output/QueryAnalysis
mkdir -p $monitoringHome/output/dalServer
##Directory creation for system level stats
mkdir -p $monitoringHome/output/osStats
## Directory creation for sendSummaryReport (HTML Report)
mkdir -p $monitoringHome/output/sendSummaryReport
## Directory creation for job Failure check
mkdir -p $monitoringHome/output/jobsFailure
## Directory creation for table Structure
mkdir -p $monitoringHome/output/tableStructure
## Directory creation for Data Validation
mkdir -p $monitoringHome/output/dataValidation
## Directory creation for Postgres Connections
mkdir -p $monitoringHome/output/postgresUsersCount
## Directory creation for DataSourceRechability
mkdir -p $monitoringHome/output/dataSourceReachability
mkdir -p $monitoringHome/output/ApplicationNodesFileSysUse
}

function cronAddition()
{
### Cron Addition ###
## Cron entries for checkAggregations.sh ##
echo "##### Monitoring Scripts Cron entries ### " >> /var/spool/cron/root
echo "*/15 * * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getPostgresUserConnectionsAndAlert.py >> /var/local/monitoring/log/getPostgresUserConnectionsAndAlert.log 2>&1" >> /var/spool/cron/root
echo "*/15 * * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/alertFailedJobs.py 15MIN >> /var/local/monitoring/log/alertFailedJobs.log 2>&1" >> /var/spool/cron/root
echo "59 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/alertFailedJobs.py HOUR >> /var/local/monitoring/log/alertFailedJobs.log 2>&1" >> /var/spool/cron/root
echo "5 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/alertFailedJobs.py DAY >> /var/local/monitoring/log/alertFailedJobs.log 2>&1" >> /var/spool/cron/root
echo "1 9 * * 1 . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/alertFailedJobs.py WEEK >> /var/local/monitoring/log/alertFailedJobs.log 2>&1" >> /var/spool/cron/root
echo "30 10 1 * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/alertFailedJobs.py MONTH >> /var/local/monitoring/log/alertFailedJobs.log 2>&1" >> /var/spool/cron/root
echo "58 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py 15MIN >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "58 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py HOUR >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "5 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py DAY >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "1 9 * * 1 . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py WEEK >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "30 10 1 * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py MONTH >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "59 23 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py 15MINSUMMARY >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "59 23 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py HOURSUMMARY >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "15 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py DAYSUMMARY >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "10 9 * * 1 . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py WEEKSUMMARY >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "*/15 * * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py LONG15MIN >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "58 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py LONGHOUR >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "5 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py LONGDAY >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "5 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py LONGWEEK >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "30 10 1 * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkAggregations.py LONGMONTH >> /var/local/monitoring/log/checkAggregations.log 2>&1" >> /var/spool/cron/root
echo "5 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/mandatoryJobScheduling.py Usage >> /var/local/monitoring/log/mandatoryJobScheduling.log 2>&1" >> /var/spool/cron/root
echo "10 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/mandatoryJobScheduling.py Aggregation >> /var/local/monitoring/log/mandatoryJobScheduling.log 2>&1" >> /var/spool/cron/root
## Cron entries for QS_cache_count.py ##
echo "30 9 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/qsCacheCountCpLevel.py >> /var/local/monitoring/log/qsCacheCountCpLevel.log 2>&1" >> /var/spool/cron/root
echo "30 9 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/qsCacheCountTableLevel.py DAY >> /var/local/monitoring/log/qsCacheCountTableLevel.log 2>&1" >> /var/spool/cron/root
echo "1 9 * * 1 . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/qsCacheCountTableLevel.py WEEK >> /var/local/monitoring/log/qsCacheCountTableLevel.log 2>&1" >> /var/spool/cron/root
echo "1 9 * * 1 . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/qsCacheCountTableLevel.py DIMENSION >> /var/local/monitoring/log/qsCacheCountTableLevel.log 2>&1" >> /var/spool/cron/root
## Cron entries for checkOpenFile.py ##
echo "*/30 * * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkOpenFile.py >> /var/local/monitoring/log/checkOpenFile.log 2>&1" >> /var/spool/cron/root
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getOpenFilesAndAlert.py >> /var/local/monitoring/log/getOpenFilesAndAlert.log 2>&1" >> /var/spool/cron/root
## Cron entries for processMonitor.py
echo "30 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/processMonitor.py >> /var/local/monitoring/log/processMonitor.log 2>&1" >> /var/spool/cron/root
## Cron entries for checkURL.py
echo "15 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkURL.py >> /var/local/monitoring/log/checkURL.log 2>&1" >> /var/spool/cron/root
## Cron entries for  Traffic Profiling##
echo "0 3 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/usageTableCount.py >> /var/local/monitoring/log/usageTableCount.log 2>&1" >> /var/spool/cron/root
echo "0 2 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getAllDimensionCount.py >> /var/local/monitoring/log/getAllDimensionCount.log 2>&1" >> /var/spool/cron/root
echo "0 4 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/table_counts_and_var_factor.py day" >> /var/spool/cron/root
echo "0 6 * * 1 . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/table_counts_and_var_factor.py week" >> /var/spool/cron/root
echo "0 11 1 * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/table_counts_and_var_factor.py month" >> /var/spool/cron/root
## Cron entries for checkJobs.py ##
echo "09 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkJobs.py USAGE >> /var/local/monitoring/log/checkJobs.log 2>&1" >> /var/spool/cron/root
echo "15 */1 * * * .  /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkJobs.py TNP >> /var/local/monitoring/log/checkJobs.log 2>&1" >> /var/spool/cron/root
echo "5 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkJobs.py DAYQS >> /var/local/monitoring/log/checkJobs.log 2>&1" >> /var/spool/cron/root
echo "1 9 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkJobs.py WEEKQS >> /var/local/monitoring/log/checkJobs.log 2>&1" >> /var/spool/cron/root
## Cron entry for Backlog ##
echo "59 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkBacklogs.py >> /var/local/monitoring/log/backlogs.log 2>&1" >> /var/spool/cron/root
## Cron entry for TNP ##
echo "10 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getTnpLatency.py >> /var/local/monitoring/log/getTnpLatency.log 2>&1" >> /var/spool/cron/root
## Cron entry for Service Last Restart Time ##
echo "*/15 * * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/Servicestability.py" >> /var/spool/cron/root
## Cron entry for bepd data traffic monitoring ##
echo "0 9 * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/data_traffic_monitor.py" >> /var/spool/cron/root
## Cron entry for application services statistics monitoring ##
echo "*/30 * * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/service_stats_info.py" >> /var/spool/cron/root
## Cron entry for etl topologies status on nodes and etl topologies statistics ##
echo "*/15 * * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/etl_topologies_status.py" >> /var/spool/cron/root
## Cron entry to check time spans in file at etl ##
echo "*/15 * * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/check_time_spans_in_file_at_etl.py" >> /var/spool/cron/root
## Cron entry for hdfs ngdb file system usage ##
echo "59 */1 * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_ngdb_file_system_usage.py TOTAL" >> /var/spool/cron/root
echo "0 8 * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_ngdb_file_system_usage.py HDFS" >> /var/spool/cron/root
echo "15 8 * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_ngdb_file_system_usage.py NGDB" >> /var/spool/cron/root
echo "0 9 * * *  . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_ngdb_file_system_usage.py DIMENSION" >> /var/spool/cron/root
echo "30 8 * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_ngdb_file_system_usage.py USAGE" >> /var/spool/cron/root
echo "5 8 * * *  . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_ngdb_file_system_usage.py 15MIN" >> /var/spool/cron/root
echo "15 9 * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_ngdb_file_system_usage.py HOUR" >> /var/spool/cron/root
echo "0 12 * * *  . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_ngdb_file_system_usage.py DAY" >> /var/spool/cron/root
echo "0 14 * * 1  . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_ngdb_file_system_usage.py WEEK" >> /var/spool/cron/root
echo "30 10 1 * *  . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_ngdb_file_system_usage.py MONTH" >> /var/spool/cron/root
## Cron entry for hdfs hbase file system usage ##
echo "*/15 * * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/hdfs_hbase_file_system_usage.py" >> /var/spool/cron/root
## Cron entry for Boundary status ##
echo "59 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getBoundary.py 15MIN >> /var/local/monitoring/log/boundary15Min.log 2>&1" >> /var/spool/cron/root
echo "59 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getBoundary.py HOUR >> /var/local/monitoring/log/boundaryHour.log 2>&1" >> /var/spool/cron/root
echo "5 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getBoundary.py DAY >> /var/local/monitoring/log/boundaryDay.log 2>&1" >> /var/spool/cron/root
echo "1 9 * * 1 . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getBoundary.py WEEK >> /var/local/monitoring/log/boundaryWeek.log 2>&1" >> /var/spool/cron/root
## Cron entry for Spark and DAL Logs ##
echo "59 */1 * * * . /root/.bash_profile;sh /opt/nsn/ngdb/monitoring/scripts/queriesCount.sh FLEXI >> /var/local/monitoring/log/sparkPortalQueries.log 2>&1" >> /var/spool/cron/root
echo "59 */1 * * * . /root/.bash_profile;sh /opt/nsn/ngdb/monitoring/scripts/queriesCount.sh PORTAL >> /var/local/monitoring/log/sparkPortalQueries.log 2>&1" >> /var/spool/cron/root
echo "5 * * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/dalquerylog.py  >> /var/local/monitoring/log/dalquerylog.log 2>&1" >> /var/spool/cron/root
echo "*/15 * * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkConnectors.py status >> /var/local/monitoring/log/checkConnectors.log 2>&1" >> /var/spool/cron/root
echo "*/30 * * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/checkConnectors.py lag >> /var/local/monitoring/log/checkConnectors.log 2>&1" >> /var/spool/cron/root
echo "30 9 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/sendSummaryReport.py >> /var/local/monitoring/log/sendSummaryReport.log 2>&1" >> /var/spool/cron/root
echo "30 9 * * 1 . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/threadCount.py >> /var/local/monitoring/log/threadCount.log 2>&1" >> /var/spool/cron/root
echo "09 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getBoundary.py USAGE >> /var/local/monitoring/log/boundaryUsage.log 2>&1" >> /var/spool/cron/root
echo "*/15 * * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/checkDiskUsage.py >> /var/local/monitoring/log/diskUsage.log 2>&1" >> /var/spool/cron/root
echo "*/15 * * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/alertFailedJobs.py USAGE >> /var/local/monitoring/log/alertFailedJobsUsage.log 2>&1" >> /var/spool/cron/root
## Cron entry for Reagg Changes ##
echo "58 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/alertFailedJobs.py REAGG >> /var/local/monitoring/log/failedReagg.log 2>&1" >> /var/spool/cron/root
echo "58 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getBoundary.py REAGG >> /var/local/monitoring/log/boundaryReagg.log 2>&1" >> /var/spool/cron/root
## Cron entry for Yarn Queue Usage ##
echo "*/1 * * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/yarn_queue_usage.py " >> /var/spool/cron/root
#### End of Postgres Scripts ####
if [[ $cemod_platform_distribution_type == "cloudera" ]];then
        ## Cron entry for getMissingBucket ##
        echo "59 */1 * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/getMissingBucket.py >> /var/local/monitoring/log/getMissingBucket.log 2>&1" >> /var/spool/cron/root
        ## Cron entry for spark Restart ##
        echo "59 */1 * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/sparkRestart.py >> /var/local/monitoring/log/sparkRestart.log 2>&1" >> /var/spool/cron/root
fi

if [[ $cemod_application_content_pack_ICE_status == "yes" ]];then
        ## Cron entry for getBachlogs for AE ##
        echo "59 */1 * * * . /root/.bash_profile;python /opt/nsn/ngdb/monitoring/scripts/checkBacklogsAE.py >> /var/local/monitoring/log/checkBacklogsAE.log 2>&1" >> /var/spool/cron/root
fi
## Cron entry for DataSource Rechability ##
echo "*/15 * * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/dataSourceReachability.py >> /var/local/monitoring/log/dataSourceReachability.log 2>&1" >> /var/spool/cron/root
## Cron entry for hdfsPerformance.py ##
echo "10 */8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getHdfsPerformance.py >> /var/local/monitoring/log/hdfsPerformance.log 2>&1" >> /var/spool/cron/root
## Cron entry for Couch Base Stats ##
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/couchbasestats.py" >> /var/spool/cron/root
## Cron entry for Portal Scripts ##
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/getClientIpsSummaryView.py" >> /var/spool/cron/root
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/portalauditlogerrors.py" >> /var/spool/cron/root
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/portalauditlognodataerrors.py" >> /var/spool/cron/root
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/portalauditlogqueryanalysis.py" >> /var/spool/cron/root
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/portalconnections.py" >> /var/spool/cron/root
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/portalmemoryusage.py" >> /var/spool/cron/root
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/tomcatmemory.py" >> /var/spool/cron/root
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/portalreportsperuser.py" >> /var/spool/cron/root
echo "5 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/portalpopularreports.py" >> /var/spool/cron/root
echo "5 8 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/portalusersactivity.py" >> /var/spool/cron/root
## Cron entry for Export job stats collections Scripts ##
echo "*/15 * * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/exportjobsfilecount.py sdk" >> /var/spool/cron/root
echo "5 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/exportjobsfilecount.py cct" >> /var/spool/cron/root
## Cron entry for CEI Index Comparison and Notification Scripts ##
echo "45 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/compare_cei_index_and_alert.py" >> /var/spool/cron/root
## Cron entry for Schedule Restart ##
echo "0 * * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/scheduler_restart.py >> /var/local/monitoring/log/monitorSchedulerService.log 2>&1" >> /var/spool/cron/root
## Cron entry to Check Hung Jobs ##
echo "10,58 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/check_and_fix_hung_jobs.py DAY 2" >> /var/spool/cron/root
echo "50 */1 * * * . /root/.bash_profile; python /opt/nsn/ngdb/monitoring/scripts/check_and_fix_hung_jobs.py WEEK 3" >> /var/spool/cron/root
echo "0 9 * * * . /root/.bash_profile; /usr/local/bin/python3.7 /opt/nsn/ngdb/monitoring/scripts/getMetadataAsXML.py operational" >> /var/spool/cron/root
echo "##### End of Monitoring Scripts Cron entries ### " >> /var/spool/cron/root

dos2unix -q $scriptsHome/scripts/*

echo "Cron Entries Addition is successful"

echo "Modifying the path in utils"

sed -i 's/\/opt\/nsn\/rtb\/monitoring\/conf\/monitoring.xml/\/opt\/nsn\/ngdb\/ifw\/etc\/common\/common_config.xml/g' $scriptsHome/utils/sendMailUtil.py
sed -i "s/127.0.0.1/$cemod_smtp_ip/g;s/nsn.mail.com/$cemod_smtp_host/g;s/root@localhost/$smtp_sender_emailID/g" $scriptsHome/conf/ceimonitoring.xml
}


function removalCron()
{
echo "Starting removal of Cron"
echo "Starting cron entries clean-up"
echo "Taking backup of Cron.."
touch $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
crontab -l > $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
echo "Backup of cron is done"
crontab -r
sed -i '/^##### /,/^##### /d' $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
cat $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt >> /var/spool/cron/root
chmod 600 /var/spool/cron/root
echo "Clean-up of Cron is completed"
echo "Restored Cron"
}

function pythonInstall()
{
touch /var/local/monitoring/log/python_installation.log
echo "Moving to tmp Dierctory"
cd /tmp/python-dependencies
echo "Installing html-1.16.tar.gz"
tar xf html-1.16.tar.gz
cd html-1.16/
python setup.py install 2>&1>>/var/local/monitoring/log/python_installation.log
echo "html-1.16.tar.gz installed"
cd ..

echo "Installing pip-8.1.0.tar.gz"
tar xf pip-8.1.0.tar.gz
cd pip-8.1.0/
python setup.py install 2>&1>>/var/local/monitoring/log/python_installation.log
echo "pip-8.1.0.tar.gz installed"
cd ..

echo "Installing dicttoxml-1.7.4.tar.gz"
tar xf dicttoxml-1.7.4.tar.gz
cd dicttoxml-1.7.4/
python setup.py install 2>&1>>/var/local/monitoring/log/python_installation.log
echo "dicttoxml-1.7.4.tar.gz installed"
}

function python3_dependency_check()
{
/usr/local/bin/pip3.7 list | grep  $1 > /dev/null
echo $?
}

function python3_install()
{
if [[ $(python3_dependency_check SQLAlchemy) -ne 0 ]]
then
echo "Installing SQLAlchemy-1.3.11.tar.gz"
tar xf /opt/nsn/ngdb/pythonPackages3.7/packages/SQLAlchemy-1.3.11.tar.gz
cd SQLAlchemy-1.3.11/
/usr/local/bin/python3.7 setup.py install 2>&1>>/var/local/monitoring/log/python_installation.log
echo "SQLAlchemy-1.3.11.tar.gz installed"
fi
if [[ $(python3_dependency_check psycopg2) -ne 0 ]]
then
echo "Installing psycopg2-binary-2.8.3.tar.gz"
tar xf /opt/nsn/ngdb/pythonPackages3.7/packages/psycopg2-binary-2.8.3.tar.gz
cd psycopg2-binary-2.8.3/
/usr/local/bin/python3.7 setup.py  build_ext --pg-config /usr/pgsql-9.5/bin/pg_config install 2>&1>>/var/local/monitoring/log/python_installation.log
echo "psycopg2-binary-2.8.3.tar.gz installed"
fi
}

function checkIfTableExists()
{
	table_name=$1
	tableExists=`ssh ${cemod_postgres_sdk_fip_active_host} "/opt/nsn/ngdb/pgsql/bin/psql -U $cemod_sdk_schema_name -d $cemod_sdk_db_name  -c \\"SELECT EXISTS (SELECT 1 FROM   information_schema.tables  WHERE  table_schema = '$cemod_sdk_schema_name' AND    table_name = '$table_name' );\\"" | egrep -v "row|--|exists" | tr -d [:space:]`
	echo $tableExists
}

function createMonitoringTable()
{
	tableExists=$(checkIfTableExists "hm_stats")
	if [[ "$tableExists" != "t" ]];then
		ssh ${cemod_postgres_sdk_fip_active_host} "/opt/nsn/ngdb/pgsql/bin/psql -U $cemod_sdk_schema_name -d $cemod_sdk_db_name  -c \"CREATE TABLE hm_stats (HM_DATE TIMESTAMP, HM_TYPE VARCHAR(50), HM_JSON JSON);\""
	fi
}

function freshInstallOrUpgrade()
{
	if [[ $valPassed -eq 2 ]];then
		echo "Its upgrade"
		removalCron
	else
		echo "Its fresh-install"
		pythonInstall
	fi
	directoriesCreation
	cronAddition
	createMonitoringTable
	python3_install
}

freshInstallOrUpgrade


