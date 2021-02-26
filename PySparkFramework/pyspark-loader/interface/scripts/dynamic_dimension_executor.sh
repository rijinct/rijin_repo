#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export PYSPARK_PYTHON=/usr/local/bin/python3.7
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.7


driver_script='/mnt/ContentAndAdaptation/pyspark-loader/framework/dynamic_dimension_loader.py'
pyspark_settings='/mnt/ContentAndAdaptation/pyspark-loader/conf/pyspark_settings.yml'
common_py_file='/mnt/ContentAndAdaptation/pyspark-loader/framework/connector.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/reader.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/specification.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/util.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/dimension_common.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/dynamic_dimension_mapper.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/connection.py,/mnt/ContentAndAdaptation/pyspark-loader/framework/dataframe.py'
config_gen='/mnt/ContentAndAdaptation/pyspark-loader/tools/pre_processor.py'

groupname=`echo $* | cut -d' ' -f2 | cut -d'/' -f6`
tablename=`echo $* | cut -d' ' -f2 | cut -d'/' -f7`

temp_filecount=`hdfs dfs -ls /ngdb/es/${groupname}/${tablename}/file_load_info=1*/_temporary* | wc -l`
latest_filecount=`hdfs dfs -ls /ngdb/es/${groupname}/${tablename}/file_load_info=latest/*.parquet | wc -l`
latest1_filecount=`hdfs dfs -ls /ngdb/es/${groupname}/${tablename}/file_load_info=latest1*/*.parquet | wc -l`
sdts=`hdfs dfs -ls /ngdb/es/${groupname}/${tablename} | grep -v Found | cut -d'=' -f2 | grep -v latest | sort -ur`
dts=( ${sdts} )

cleanup() {
	echo ${groupname} >>  ${log_path} 2>&1
	echo ${tablename} >>  ${log_path} 2>&1
	echo "temp_filecount:" $temp_filecount "latest_filecount:" $latest_filecount "latest1_filecount:" $latest1_filecount "sorted dts:" $sdts >>  ${log_path} 2>&1
	if [ $temp_filecount -gt 0 ];
	then
		echo "hdfs dfs -rm -r /ngdb/es/${groupname}/${tablename}/file_load_info=latest1" >>  ${log_path} 2>&1
		hdfs dfs -rm -r /ngdb/es/${groupname}/${tablename}/file_load_info=latest1
		echo "hdfs dfs -rm -r /ngdb/es/${groupname}/${tablename}/file_load_info=1*/_temporary" >>  ${log_path} 2>&1
		hdfs dfs -rm -r /ngdb/es/${groupname}/${tablename}/file_load_info=1*/_temporary
	else
		if [ $latest1_filecount -gt 0 ];
		then
			echo "hdfs dfs -rm -r /ngdb/es/${groupname}/${tablename}/file_load_info=latest" >>  ${log_path} 2>&1
			hdfs dfs -rm -r /ngdb/es/${groupname}/${tablename}/file_load_info=latest
			echo "hdfs dfs -mv /ngdb/es/${groupname}/${tablename}/file_load_info=latest1 /ngdb/es/${groupname}/${tablename}/file_load_info=latest" >>  ${log_path} 2>&1
			hdfs dfs -mv /ngdb/es/${groupname}/${tablename}/file_load_info=latest1 /ngdb/es/${groupname}/${tablename}/file_load_info=latest
		fi

		len=${#dts[@]}
		echo $len >> ${log_path} 2>&1
		for (( i=1; i<$len; i++ )); 
		do 
			echo "${dts[$i]}" >> ${log_path} 2>&1
			echo "hdfs dfs -mv /ngdb/es/${groupname}/${tablename}/file_load_info=${dts[$i]}/* /ngdb/es/${groupname}/${tablename}/file_load_info=${dts[0]}/" >>  ${log_path} 2>&1
			hdfs dfs -mv /ngdb/es/${groupname}/${tablename}/file_load_info=${dts[$i]}/* /ngdb/es/${groupname}/${tablename}/file_load_info=${dts[0]}/
			echo "hdfs dfs -rmdir /ngdb/es/${groupname}/${tablename}/file_load_info=${dts[$i]}" >>  ${log_path} 2>&1
			hdfs dfs -rmdir /ngdb/es/${groupname}/${tablename}/file_load_info=${dts[$i]}
		done
	fi
}

if [ -z ${IS_K8S+x} ];
then	
	export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2
	export PYSPARK_PYTHON=/usr/local/bin/python3.7
	export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.7
	export SPARK_CONF_DIR=$SPARK_HOME/conf
	
	job_name=`/usr/local/bin/python3.7 $config_gen -action job_name $*`
	custom_conf=`/usr/local/bin/python3.7 $config_gen -action conf -f $pyspark_settings -n $job_name`
	
	log_path='/opt/nsn/ngdb/scheduler/app/log/spark_execution_'$job_name'_'`date +"%d-%m-%Y_%H%M%S"`.log
	echo "Baremetal !" >>  ${log_path} 2>&1
	cleanup
	echo "spark2-submit --master yarn --deploy-mode cluster $custom_conf --py-files $common_py_file $driver_script $* " >> ${log_path} 2>&1
	spark2-submit --master yarn --deploy-mode cluster $custom_conf --py-files $common_py_file $driver_script $* >> ${log_path} 2>&1
	
else	
	job_name=`python $config_gen -action job_name $*`
	custom_conf=`python $config_gen -action conf -f $pyspark_settings -n $job_name`

	log_path='/mnt/staging/archive/pyspark-loader/logs/spark_execution_'$job_name'_'`date +"%d-%m-%Y_%H%M%S"`.log
	echo "Nova !" >>  ${log_path} 2>&1
	cleanup
	echo "/opt/spark/bin/spark-submit --master yarn --deploy-mode cluster $custom_conf --py-files $common_py_file $driver_script $* " >> ${log_path} 2>&1
	/opt/spark/bin/spark-submit --master yarn --deploy-mode cluster $custom_conf --py-files $common_py_file $driver_script $* >> ${log_path} 2>&1

fi

exit_status=$?

rm /mnt/staging/archive/${groupname}/${tablename}/corrupt/*.dis
hdfs dfs -get /tmp/duplicate/${tablename}/*.csv /mnt/staging/archive/${groupname}/${tablename}/corrupt/${tablename}_duplicate_1.dis

echo "Dynamic dimension job completion status ${exit_status}" >>  ${log_path} 2>&1
exit ${exit_status}