#! /bin/bash

. ~/.bash_profile

valPassed=$1

source /opt/nsn/ngdb/ifw/utils/application_definition.sh

chmod 775 /opt/nsn/ngdb/ContentAndAdaptation/pyspark-loader
chown ngdb:ninstall /opt/nsn/ngdb/ContentAndAdaptation/pyspark-loader

dos2unix /opt/nsn/ngdb/ContentAndAdaptation/pyspark-loader/scripts/*
chmod 775 /opt/nsn/ngdb/ContentAndAdaptation/pyspark-loader/scripts/*
chmod 775 /opt/nsn/ngdb/ContentAndAdaptation/pyspark-loader/framework/*
chmod 775 /opt/nsn/ngdb/ContentAndAdaptation/pyspark-loader/conf/*

mkdir -p /mnt/staging/archive/pyspark-loader/logs/
chown ngdb:ninstall /mnt/staging/archive/pyspark-loader/logs/
chmod 775 /mnt/staging/archive/pyspark-loader/logs/

if [ -z ${IS_K8S+x} ];
then
	for i in ${project_scheduler_hosts[@]}
	do
	  ssh $i "ln -sf /etc/hive/conf.cloudera.HIVE/hive-site.xml /opt/cloudera/parcels/SPARK2/lib/spark2/conf/"
	done

else
	echo ""
fi 

echo "Installation completed !!!"


