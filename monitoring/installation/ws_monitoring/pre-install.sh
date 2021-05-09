#! /bin/bash
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: Pre-install script for Monitoring RPM 
# Date:    31-08-2016
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################
valPassed=$1
if [[ $valPassed -eq 1 ]];then
	echo "Starting Pre-install Steps..."
	rpm -qa|grep "HEALTH-MONITORING-WEBSERVICE"
	if [[ $? -eq 0 ]];then
		echo "Monitoring RPM is already installed, so installation will not proceed"
		exit 1
	fi
elif [[ $valPassed -eq 2 ]];then
	#rm -rf $CATALINA_HOME/webapps/HealthMonitoring*
	rm -rf $CATALINA_HOME/webapps/jobMonitoringClient*
	rm -rf $CATALINA_HOME/webapps/jobMonitoringServer*
else
	echo "Not a valid scenario"
fi	
