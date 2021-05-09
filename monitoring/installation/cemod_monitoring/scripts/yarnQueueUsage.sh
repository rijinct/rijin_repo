#! /bin/bash
############################################################################
#                           YARN CPU QUEUE Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  pawan.kumar@nokia.com,deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: Collect the queue utilization of Yarn queue and write the output into a csv file.
#          in last one hour
# Date:    06-10-2016
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

BASE_DIR="/opt/nsn/ngdb"
CONF_FILE="$BASE_DIR/monitoring/conf/yarn_cpu_queue.conf"
LOG_DIR="/var/local/monitoring/log"
CSVFILE="/var/local/monitoring/output/capacityScheduler/yarn_queue_`date +"%Y-%m-%d"`.csv"
CSVFILEDIRECTORY="/var/local/monitoring/output/capacityScheduler"
LOG_FILE="$LOG_DIR/yarn_cpu_queue.log"
#yarnSiteXMLLocationCloudera="/etc/hadoop/conf.cloudera.YARN/"
yarncoreSiteXMLLocationCloudera="/etc/hadoop/conf/"
yarnSiteXmlLocationWandisco="$BASE_DIR/hadoop/etc/hadoop/"
utilsDirectory="/opt/nsn/ngdb/monitoring/utils"
source $NGDB_HOME/ifw/lib/application/utils/application_definition.sh

###Wandisco###
function queue_CEMoD_wandisco
{
schedulerClassTemp=`ssh $cemod_hdfs_master_hosts "grep -A 3 \"yarn.resourcemanager.scheduler.class\" $yarnSiteXmlLocationWandisco/yarn-site.xml"`
echo $schedulerClassTemp | grep "Fair"
if [[ $? -eq 0 ]];then
        echo "Its fair scheduler"
        queueCalculationFairSchedulerWandisco
else
        queueCalculationCapacitySchedulerWandisco
fi
}
function queueCalculationFairSchedulerWandisco()
{
ifsBackup=$IFS
IFS=","
rmIds=(`ssh $cemod_hdfs_master_hosts "grep -A 1 \"yarn.resourcemanager.ha.rm-ids\" $yarnSiteXmlLocationWandisco/yarn-site.xml | tail -1 | gawk -F\\"value\" '{print \\$2}' | gawk 'BEGIN {FS=\\">|<\\";} {print \\$2}'"`)
echo "rm Ids fetched are ${rmIds[*]}"
lengthRmIds=${#rmIds[@]}
lengthHadoopHosts=${#cemod_hdfs_master_hosts[@]}
echo "Length is $lenghtRmIds"
for (( i=0; i<$lengthRmIds; i++ ))
do
        echo "Values in array are ${rmIds[$i]}"
        for (( j=0; j<$lengthHadoopHosts; j++ ))
        do
                checkState=`ssh ${cemod_hdfs_master_hosts[$j]} "su - hadoop -c 'yarn rmadmin -getServiceState ${rmIds[$i]}'"`
                if [[ $checkState == "active" ]];then
                        echo "rm id ${rmIds[$i]} is active"
                        getHostName=`ssh ${cemod_hdfs_master_hosts[$j]} "grep -A 1 \"yarn.resourcemanager.hostname.${rmIds[$i]}\" $yarnSiteXmlLocationWandisco/yarn-site.xml | tail -1| gawk -F\\"value\\" '{print \\$2}' | gawk 'BEGIN{FS=\\"<|>\\"} {print \\$2}' | gawk -F\\":\\" '{print \\$1}'"`
                        break;
                else
                        echo "Its not active";
                fi
        done
done

defaultjobvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=default | grep ">root.default<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
etljobvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=default | grep ">root.etljobs<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
dwmjobvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=default | grep ">root.dwmjobs<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
tnpjobvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=default | grep ">root.tnpjobs<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
flexireportvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=default | grep ">root.flexi-report<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
portalvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=default | grep ">root.portal<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`

now=`date +"%T"`
if [ ! -f $CSVFILE ]
then
        touch $CSVFILE
        echo "Date,Default,ETL,DWM,TNP,FLEXI_REPORT,PORTAL" > $CSVFILE
fi
sed -i "1s/.*/Date,Default,ETL,DWM,TNP,FLEXI_REPORT,PORTAL/"  $CSVFILE
echo $now","${defaultjobvalue}","${etljobvalue}","${dwmjobvalue}","${tnpjobvalue}","${flexireportvalue}","${portalvalue} >> $CSVFILE
}
function queueCalculationCapacitySchedulerWandisco()
{
ifsBackup=$IFS
IFS=","
rmIds=(`ssh $cemod_hdfs_master_hosts "grep -A 1 \"yarn.resourcemanager.ha.rm-ids\" $yarnSiteXmlLocationWandisco/yarn-site.xml | tail -1 | gawk -F\\"value\\" '{print \\$2}' | gawk 'BEGIN {FS=\\">|<\\";} {print \\$2}'`)
echo "rm Ids fetched are ${rmIds[*]}"
lengthRmIds=${#rmIds[@]}
lengthHadoopHosts=${#cemod_hdfs_master_hosts[@]}
echo "Length is $lenghtRmIds"
for (( i=0; i<$lengthRmIds; i++ ))
        do
                echo "Values in array are ${rmIds[$i]}"
                for (( j=0; j<$lengthHadoopHosts; j++ ))
                do
                        checkState=`ssh ${cemod_hdfs_master_hosts[$j]} "su - hadoop -c 'yarn rmadmin -getServiceState ${rmIds[$i]}'"`
                        if [[ $checkState == "active" ]];then
                                echo "rm id ${rmIds[$i]} is active"
                                getHostName=`ssh ${cemod_hdfs_master_hosts[$j]} "grep -A 1 \"yarn.resourcemanager.hostname.${rmIds[$i]}\" $yarnSiteXmlLocationWandisco/yarn-site.xml | tail -1| gawk -F\\"value\\" '{print \\$2}' | gawk 'BEGIN{FS=\\"<|>\\"} {print \\$2}' | gawk -F\\":\\" '{print \\$1}'"`
                                break;
                        else
                                echo "Its not active";
                        fi
                done
done
defaultjobvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=default | grep -A15 "'default' Queue" | grep -A2 "Absolute Used Capacity" | tail -1 | sed 's/ //g'`
etljobvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=etljobs | grep -A15 "'etljobs' Queue" | grep -A2 "Absolute Used Capacity" | tail -1 | sed 's/ //g'`
dwmjobvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=dwmjobs | grep -A15 "'dwmjobs' Queue" | grep -A2 "Absolute Used Capacity" | tail -1 | sed 's/ //g'`
tnpjobvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=tnpjobs | grep -A15 "'tnpjobs' Queue" | grep -A2 "Absolute Used Capacity" | tail -1 | sed 's/ //g'`
flexireportvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=default | grep "flexireport Queue" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
portalvalue=`curl -s http://$getHostName:8088/cluster/scheduler?openQueues=default | grep ">root.portal<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`

now=`date +"%T"`
if [ ! -f $CSVFILE ]
then
        touch $CSVFILE
        echo "Date,Default,ETL,DWM,TNP,FLEXI_REPORT,PORTAL" > $CSVFILE
fi
sed -i "1s/.*/Date,Default,ETL,DWM,TNP,FLEXI_REPORT,PORTAL/"  $CSVFILE
echo $now","${defaultjobvalue}","${etljobvalue}","${dwmjobvalue}","${tnpjobvalue}","${flexireportvalue}","${portalvalue} >> $CSVFILE
}
####Cloudera###
function queue_CEMoD_cloudera
{
schedulerClassTemp=`grep -A 1 "yarn.resourcemanager.scheduler.class" $yarncoreSiteXMLLocationCloudera/yarn-site.xml | tail -1`
echo $schedulerClassTemp | grep "Fair"
if [[ $? -eq 0 ]];then
        echo "Its fair scheduler"
        queueCalculationFairSchedulerCloudera
else
        echo "Its Capacity scheduler"
fi
}
function queueCalculationFairSchedulerCloudera()
{
ifsBackup=$IFS
IFS=","
rmIds=(`grep -A 1 "yarn.resourcemanager.ha.rm-ids" $yarncoreSiteXMLLocationCloudera/yarn-site.xml | tail -1 | gawk -F"value" '{print $2}' | gawk 'BEGIN {FS=">|<";} {print $2}'`)

echo "rm Ids fetched are ${rmIds[*]}"
lengthRmIds=${#rmIds[@]}
echo "Length is $lenghtRmIds"
for (( i=0; i<$lengthRmIds; i++ ))
do
        echo "value of rm id for which checking state is ${rmIds[$i]}"
        checkState=`su - ngdb -c "yarn rmadmin -getServiceState ${rmIds[$i]}" `
        sslenabled=(`grep -A 1 "hadoop.ssl.enabled" $yarncoreSiteXMLLocationCloudera/core-site.xml | tail -1 | gawk -F"value" '{print $2}' | gawk 'BEGIN {FS=">|<";} {print $2}'`)
        if [[ $checkState == "active" ]] && [[ $sslenabled == "true" ]];then
                echo "rm id ${rmIds[$i]} is active"
                gethttpsaddress=(`grep -A 1 "yarn.resourcemanager.webapp.https.address.${rmIds[$i]}" $yarncoreSiteXMLLocationCloudera/yarn-site.xml | tail -1 | gawk -F"value" '{print $2}' | gawk 'BEGIN {FS=">|<";} {print $2}'`)
                getHostName=https://$gethttpsaddress
        elif [[ $checkState == "active" ]] && [[ $sslenabled == "false" ]];then
                gethttpadress=(`grep -A 1 "yarn.resourcemanager.webapp.address.${rmIds[$i]}" $yarncoreSiteXMLLocationCloudera/yarn-site.xml | tail -1 | gawk -F"value" '{print $2}' | gawk 'BEGIN {FS=">|<";} {print $2}'`)
                getHostName=http://$gethttpadress
                break;
        else
                echo "Its not active";
        fi
done

defaultjobvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.default<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
dimensionjobvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.dimension<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
fifminjobvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.15minutes<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
usagejobvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.usage<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
hourjobvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.hour<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
dayjobvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.day<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
weekjobvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.week<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
monthjobvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.month<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
tnpjobvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.tnp<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
flexireportvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.flexi-report<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
portalvalue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.portal<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
sparkApiQueueValue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.API-Q<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
monitoringValue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.monitoring<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
pysparkValue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.pyspark-15minutes<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
dqiValue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.DQI<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
isaValue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci.ISA<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`
totalValue=`curl -k $getHostName/cluster/scheduler?openQueues=default | grep ">root.nokia.ca4ci<" | grep -i "used" | gawk -F"used</span>" '{print $1}' | gawk -F">" '{print $NF}'`

now=`date +"%T"`
if [ ! -f $CSVFILE ]
then
        touch $CSVFILE
                echo "Date,Default,DIMENSION,15MIN,USAGE,HOUR,DAY,WEEK,MONTH,TNP,FLEXI_REPORT,PORTAL,SPARK_API,MONITORING,PYSPARK,DQI,ISA,TOTAL" > $CSVFILE
fi
sed -i "1s/.*/Date,Default,DIMENSION,15MIN,USAGE,HOUR,DAY,WEEK,MONTH,TNP,FLEXI_REPORT,PORTAL,SPARK_API,MONITORING,PYSPARK,DQI,ISA,TOTAL/"  $CSVFILE
echo $now","${defaultjobvalue}","${dimensionjobvalue}","${fifminjobvalue}","${usagejobvalue}","${hourjobvalue}","${dayjobvalue}","${weekjobvalue}","${monthjobvalue}","${tnpjobvalue}","${flexireportvalue}","${portalvalue}","${sparkApiQueueValue}","${monitoringValue}","${pysparkValue}","${dqiValue}","${isaValue}","${totalValue}>> $CSVFILE
dateValue=`date +"%Y-%m-%d %H:%M:%S"`
content=$dateValue","${defaultjobvalue}","${dimensionjobvalue}","${fifminjobvalue}","${usagejobvalue}","${hourjobvalue}","${dayjobvalue}","${weekjobvalue}","${monthjobvalue}","${tnpjobvalue}","${flexireportvalue}","${portalvalue}","${sparkApiQueueValue}","${monitoringValue}","${pysparkValue}","${dqiValue}","${isaValue}","${totalValue}


postgresCsvFile="/var/local/monitoring/output/capacityScheduler/yarn_queue_`date +"%Y-%m-%d-%H-%M-%S"`.csv"
postgresCsvJson="/var/local/monitoring/output/capacityScheduler/yarn_queue_*.json"

touch $postgresCsvFile
echo "Date,Default,DIMENSION,15MIN,USAGE,HOUR,DAY,WEEK,MONTH,TNP,FLEXI_REPORT,PORTAL,SPARK_API,MONITORING,PYSPARK,DQI,ISA,TOTAL" > $postgresCsvFile
echo "$content" >> $postgresCsvFile

rm -rf $CSVFILEDIRECTORY/sed*
}

function pushToPostgresDB()
{
        jsonFileName=`python $utilsDirectory/jsonUtils.py $postgresCsvFile`
        python $utilsDirectory/dbUtils.py $jsonFileName "yarnqueue"
        rm -rf $postgresCsvFile
        rm -rf $postgresCsvJson
}

###Identify the distribution (Cloudera/Wandisco) ###

if [ "$cemod_platform_distribution_type" == "cloudera" ]
then
        echo " Platform is in Cloudera" >> $LOG_FILE
        queue_CEMoD_cloudera
        pushToPostgresDB
else
        echo "Platform is Wandisco" >> $LOG_FILE
                queue_CEMoD_wandisco
fi
