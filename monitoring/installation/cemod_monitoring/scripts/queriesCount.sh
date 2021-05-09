#! /bin/bash
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:
# Version: 0.1
# Purpose:
#
# Date:    09-02-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. Script to Check Count of Portal Queries and FLEXI Queries on SPARK
#
#############################################################################
. ~/.bash_profile
source $NGDB_HOME/ifw/lib/application/utils/application_definition.sh

sparkThriftLog="/var/log"
sparkThriftPortalLogWandisco="/var/local/spark/"
patternDate=`date +"%y/%m/%d %H"`
date=`date +"%H"`
outPutDirectory="/var/local/monitoring/output/QueryAnalysis"
type=$1;

function getClouderaSparkPortalQueries()
{
if [[ "$type" == "PORTAL" ]];then
        echo "Spark hosts are ${cemod_spark_thrift_hosts[@]}"
        lengthOfSparkHosts=${#cemod_spark_thrift_hosts[@]}
        for (( i=0; i<$lengthOfSparkHosts; i++ ))
        do
                latestSparkLogFile=`ssh ${cemod_spark_thrift_hosts[$i]} "ls -lrt $sparkThriftLog/sparkthrift/ | tail -1 | gawk -F\\" \\" '{print \\$9}' "`
                countOfQueries=`ssh ${cemod_spark_thrift_hosts[$i]} "grep \"Running query 'SELECT \" $sparkThriftLog/sparkthrift/$latestSparkLogFile "|wc -l`
                echo "$countOfQueries"
                echo "$date,(${cemod_spark_thrift_hosts[$i]})Spark ThriftServer Portal,$countOfQueries " >> $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv
                grep "Hour,Host,Count of Queries"  $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv >/dev/null 2>&1
                if [[ $? -ne 0 ]]; then
                        sed -i '1 i\Hour,Host,Count of Queries' $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv
                fi
        done
elif [[ "$type" == "FLEXI" ]];then
        echo "Spark hosts are ${cemod_spark_thrift_flexi_hosts[@]}"
        lengthOfSparkHosts=${#cemod_spark_thrift_flexi_hosts[@]}
        for (( i=0; i<$lengthOfSparkHosts; i++ ))
        do
                latestSparkLogFile=`ssh ${cemod_spark_thrift_flexi_hosts[$i]} "ls -lrt $sparkThriftLog/sparkthriftflexi/ | tail -1 | gawk -F\\" \\" '{print \\$9}' "`
                countOfQueries=`ssh ${cemod_spark_thrift_flexi_hosts[$i]} "grep  \"Running query 'SELECT \" $sparkThriftLog/sparkthriftflexi/$latestSparkLogFile "|wc -l`
                echo "$countOfQueries"
                echo "$date,(${cemod_spark_thrift_flexi_hosts[$i]})Spark ThriftServer Flexi,$countOfQueries " >> $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv
                grep "Hour,Host,Count of Queries"  $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv >/dev/null 2>&1
                if [[ $? -ne 0 ]]; then
                        sed -i '1 i\Hour,Host,Count of Queries' $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv
                fi
                 done
                else
                        echo "Script has not executed with  command line parameter";
                fi
}
function getWandiscoSparkPortalQueries()
{
if [[ "$type" == "PORTAL" ]];then
        echo "Spark hosts are ${cemod_spark_thrift_hosts[@]}"
        lengthOfSparkHosts=${#cemod_spark_thrift_hosts[@]}
        for (( i=0; i<$lengthOfSparkHosts; i++ ))
        do
                latestSparkwandiscoDir=`ssh ${cemod_spark_thrift_hosts[$i]} "ls -lrt $sparkThriftPortalLogWandisco/spark-hadoop-org.apache.spark.sql.hive.thriftserver.HiveThriftServer2-1-* | tail -1 | gawk -F\\" \\" '{print \\$9}'"`
                echo "latest spark file is $latestSparkwandiscoDir"
                countOfQueries=`ssh ${cemod_spark_thrift_hosts[$i]} "grep \"$patternDate\" $latestSparkwandiscoDir | grep \"thriftserver.SparkExecuteStatementOperation: Running query 'SELECT\" "|wc -l`
                echo "$countOfQueries"
                echo "$date,(${cemod_spark_thrift_hosts[$i]})Spark ThriftServer portal,$countOfQueries " >> $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv
                grep "Hour,Host,Count of Queries"  $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv >/dev/null 2>&1
                if [[ $? -ne 0 ]]; then
                        sed -i '1 i\Hour,Host,Count of Queries' $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv
                   fi
             done

elif [[ "$type" == "FLEXI" ]];then
         echo "Spark hosts are ${cemod_spark_thrift_flexi_hosts[@]}"
         lengthOfSparkHosts=${#cemod_spark_thrift_flexi_hosts[@]}
         for (( i=0; i<$lengthOfSparkHosts; i++ ))
         do
                latestSparkwandiscoDir=`ssh ${cemod_spark_thrift_flexi_hosts[$i]} "ls -lrt $sparkThriftPortalLogWandisco/spark-hadoop-org.apache.spark.sql.hive.thriftserver.HiveThriftServer2-2-* | tail -1 | gawk -F\\" \\" '{print \\$9}'"`
                countOfQueries=`ssh ${cemod_spark_thrift_flexi_hosts[$i]} "grep \"$patternDate\" $latestSparkwandiscoDir | grep \"thriftserver.SparkExecuteStatementOperation: Running query 'SELECT\" "|wc -l`
                echo "$countOfQueries"
                echo "$date,(${cemod_spark_thrift_flexi_hosts[$i]})Spark ThriftServer flexi,$countOfQueries " >> $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv
                grep "Hour,Host,Count of Queries"  $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv >/dev/null 2>&1
                if [[ $? -ne 0 ]]; then
                        sed -i '1 i\Hour,Host,Count of Queries' $outPutDirectory/PortalFlexiQueries_`date +"%Y-%m-%d"`.csv
                fi
        done
else
                echo "Script has not executed with  command line parameter";
fi
}

if [[ $cemod_platform_distribution_type = "cloudera" ]];then
getClouderaSparkPortalQueries
else
getWandiscoSparkPortalQueries
fi
