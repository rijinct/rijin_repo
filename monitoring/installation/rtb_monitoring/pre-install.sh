#! /bin/bash
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: Pre-install script for Monitoring RPM
# Date:    28-11-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################
valPassed=$1

if [[ $valPassed -eq 1 ]];then
        echo "Starting Pre-install Steps..."
        rpm -qa|grep "MONITORING"
        if [[ $? -eq 0 ]];then
                echo "Monitoring RPM is already installed, so installation will not proceed"
                exit 1
        else
                echo "Proceeding RPM installation.."
        fi
else
        echo "Proceeding with Upgrade.."
fi