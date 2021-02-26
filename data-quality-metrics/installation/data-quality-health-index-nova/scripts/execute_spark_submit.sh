#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export PYSPARK_PYTHON=/usr/local/bin/python3.7
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.7


driver_script='/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/dq_score_calculator_driver.py'
pyspark_settings='/mnt/ContentAndAdaptation/pyspark-loader/conf/pyspark_settings.yml'
common_py_file='/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/file_util.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/constants.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/hdfs_file_utils.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/common_utils.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/logger_util.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/dqhi_score_handler.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/connection_wrapper.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/query_executor.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/score_calculator.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/common_constants.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/dq_logging_util.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/date_utils.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/rules.py,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/connection_wrapper_nova.py'
property_files="/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/conf/table_column_list.properties,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/conf/rule_name_id_dict.properties,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/conf/hive_tables.properties,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/conf/table_column_ids_dict.properties,/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/conf/dq_field_def_df.csv"
config_gen='/mnt/ContentAndAdaptation/pyspark-loader/tools/pre_processor.py'
LB=$1
UB=$2
HIVE_TBL=$3
HDFS_PATH=$4


if [ ${IS_K8S+x} ];
then
    start=$(date +%s.%N)
    custom_conf="$DQHI_SPARK_CONF"
    log_path='/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/log/spark_execution_'dqhi'_'`date +"%d-%m-%Y_%H%M%S"`.log
    echo "/opt/spark/bin/spark-submit --master yarn --deploy-mode cluster $custom_conf --py-files $common_py_file --files $property_files $driver_script $LB $UB $HIVE_TBL $HDFS_PATH" >> ${log_path} 2>&1
    counter=1
    while true
        do
            /opt/spark/bin/spark-submit --master yarn --deploy-mode cluster $custom_conf --py-files $common_py_file --files $property_files $driver_script $LB $UB $HIVE_TBL $HDFS_PATH >> ${log_path} 2>&1
            if [[ ( "$?" -ne 0 ) && ( $counter < 4 ) ]]
                then
                    echo "spark-submit looks to have failed during $counter attempt, retrying $counter"
                    sleep 1200
                    counter=`expr $counter + 1`
            else
                break
            fi
        done
    app_id=`grep "Application report for application_" ${log_path} | head -1 | awk -F " " '{print $4}'`
    if [ -z $app_id ] 
    then 
        echo "Unable to submit spark application. Please check the logs in ${log_path}"
    else
        echo "Spark-submit job status can be checked in $app_id"
    fi

fi
