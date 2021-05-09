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
. ~/.bash_profile


function backupFile()
{
	mkdir -p /opt/nsn/ngdb/update/backup/
	cp -a -- "$1" "/opt/nsn/ngdb/update/backup/${1##*/}_$current_datetime.bkp"
	echo "Backup file created: /opt/nsn/ngdb/update/backup/${1##*/}_$current_datetime.bkp"
}

if [ $1 -eq 1 ] 
then
	echo "Installation is started"
elif [ $1 -ge 2 ] 
then
	echo "Pre-upgrade is started"
	echo "Taking backup of configuration files in /opt/nsn/ngdb/update/backup/ location"
	current_datetime="$(date +"%Y%m%d_%H%M%S")"

	configFile="$NGDB_HOME/monitoring/scripts/constants.py"
	backupFile $configFile

	xslFile="$NGDB_HOME/monitoring/metadata/technical/custom/technicalMetadata.xsl"
	if [ -f "$xslFile" ]; then
		backupFile $xslFile
	fi

	xslFile="$NGDB_HOME/monitoring/metadata/operational/custom/operationalMetadata.xsl"
	if [ -f "$xslFile" ]; then
		backupFile $xslFile
	fi
fi
