#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export PYSPARK_PYTHON=/usr/local/bin/python3.7
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.7


driver_script='/mnt/ContentAndAdaptation/pyspark-loader/framework/__main__.py'
pyspark_settings='/mnt/ContentAndAdaptation/pyspark-loader/conf/pyspark_settings.yml'
common_py_file='/mnt/ContentAndAdaptation/pyspark-loader/framework/connector.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/aggregator.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/reader.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/writer.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/specification.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/util.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/connection.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/dataframe.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/query_generator.py'
config_gen='/mnt/ContentAndAdaptation/pyspark-loader/tools/pre_processor.py'
yaml_path='/mnt/ContentAndAdaptation/pyspark-loader/yaml/'
#dimension_yaml_path='/mnt/ContentAndAdaptation/pyspark-loader/conf/dimension_extensions/'


if [ -z ${IS_K8S+x} ];
then	
	start=$(date +%s.%N)
	export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2
	export PYSPARK_PYTHON=/usr/local/bin/python3.7
	export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.7
	export SPARK_CONF_DIR=$SPARK_HOME/conf
	
	job_name=`/usr/local/bin/python3.7 $config_gen -action job_name $*`
	yaml_files_path=${yaml_path}'replaced_'$job_name'.yaml'
	process_all=`/usr/local/bin/python3.7 $config_gen -action process_all -f $pyspark_settings -n $job_name $*`
	custom_conf=`/usr/local/bin/python3.7 $config_gen -action conf -f $pyspark_settings -n $job_name`
	log_path='/opt/nsn/ngdb/scheduler/app/log/spark_execution_'$job_name'_'`date +"%d-%m-%Y_%H%M%S"`.log
	duration=$(echo "$(date +%s.%N) - $start" | bc)
	execution_time=`printf "%.2f seconds" $duration`

	echo "Script Execution Time: $execution_time" >>  ${log_path} 2>&1
	echo "Baremetal !" >>  ${log_path} 2>&1
	echo "spark2-submit --master yarn --deploy-mode cluster $custom_conf --py-files $common_py_file --files $yaml_files_path $driver_script $process_all" >>  ${log_path} 2>&1
	spark2-submit --master yarn --deploy-mode cluster $custom_conf --py-files $common_py_file --files $yaml_files_path $driver_script $process_all >> ${log_path} 2>&1
	
else
  start=$(date +%s.%N)
	job_name=`python $config_gen -action job_name $*`
	yaml_files_path=${yaml_path}'replaced_'$job_name'.yaml'
	process_all=`python $config_gen -action process_all -f $pyspark_settings -n $job_name $*`
	custom_conf=`python $config_gen -action conf -f $pyspark_settings -n $job_name`

	log_path='/mnt/staging/archive/pyspark-loader/logs/spark_execution_'$job_name'_'`date +"%d-%m-%Y_%H%M%S"`.log
	duration=$(echo "$(date +%s.%N) - $start" | bc)
	execution_time=`printf "%.2f seconds" $duration`

	echo "Script Execution Time: $execution_time" >>  ${log_path} 2>&1
	echo "Nova !" >>  ${log_path} 2>&1
	echo "/opt/spark/bin/spark-submit --master yarn --deploy-mode cluster $custom_conf --py-files $common_py_file --files $yaml_files_path $driver_script $process_all " >> ${log_path} 2>&1
	/opt/spark/bin/spark-submit --master yarn --deploy-mode cluster $custom_conf --py-files $common_py_file --files $yaml_files_path $driver_script $process_all >> ${log_path} 2>&1

fi
