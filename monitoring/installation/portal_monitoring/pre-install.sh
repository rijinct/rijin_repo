#! /bin/bash
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2018 NOKIA
# Author:  Monitoring Team
# Version: 0.1
# Purpose: Pre-install script for Monitoring RPM
# Date:    18-01-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

if [ $1 -eq 1 ] 
then
	echo "Installation is started"
elif [ $1 -ge 2 ] 
then
	echo "Pre-upgrade is started"
fi