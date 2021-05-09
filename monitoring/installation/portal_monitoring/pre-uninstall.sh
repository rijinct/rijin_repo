#! /bin/bash
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring Team
# Version: 0.1
# Purpose: Un-install script for Monitoring RPM
# Date:    18-01-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################
. ~/.bash_profile
valPassed=$1

function unInstall()
{
		IFW_HOME="/home/portal"
        echo "Starting cron entries clean-up"
        echo "Taking backup of Cron.."
        touch $IFW_HOME/CronTab_`date +%Y-%m-%d`.txt
        crontab -l > $IFW_HOME/CronTab_`date +%Y-%m-%d`.txt
        echo "Backup of cron is done"
        crontab -r
        sed -i '/ ###/,/ ###/d' $IFW_HOME/CronTab_`date +%Y-%m-%d`.txt
        cat $IFW_HOME/CronTab_`date +%Y-%m-%d`.txt >> /var/spool/cron/root
        chmod 600 /var/spool/cron/root
        echo "Clean-up of Cron is completed"
        echo "Restored Cron"
        echo "Un-installation Successful"
}

if [[ $valPassed -eq 0 ]];then
        echo "Proceeding with Complete Un-installation"
        unInstall
else
        echo "Upgrade Un-installation,nothing to perform"
fi