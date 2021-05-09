#! /bin/bash
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring Team
# Version: 0.1
# Purpose: This Post Installation Script for Monitoring RPM is to create directories
# and cron entries needed for all Monitoring scripts
# Date:    18-11-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################
. ~/.bash_profile
portalOutput="/var/local/monitoring"
portalScriptsPath="/opt/portal/monitoring/scripts"
valPassed=$1

function directoriesCreation()
{
        echo "Starting Post Install Steps..."
        ## Output Directory Creation ##
        mkdir -p $portalOutput
        ## Output Directories for different monitoring utilities #
        mkdir -p $portalOutput/output/diskSpaceUtilization
        mkdir -p $portalOutput/output/memoryUtilization
        mkdir -p $portalOutput/output/serviceStability
        mkdir -p $portalOutput/output/cpuUtilization
        mkdir -p $portalOutput/output/postgresUsersCount
        mkdir -p $portalOutput/output/webserviceMemory
        mkdir -p $portalOutput/output/auditLogErrors
        mkdir -p $portalOutput/output/portalConnectionStats
        mkdir -p $portalOutput/output/auditLogQueryAnalysis
        ## Directory creation for work ##
        mkdir -p $portalOutput/work
        ## Directory creation for log ##
        mkdir -p $portalOutput/log
}

function cronAddition()
{
        ### Cron Addition ###
        echo "##### Monitoring Scripts Cron entries ### " >> /var/spool/cron/root
        echo "*/15 * * * * . /root/.bash_profile; python $portalScriptsPath/checkDiskUsage.py >> $portalOutput/log/checkDiskUsage.log 2>&1" >> /var/spool/cron/root
        echo "*/15 * * * * . /root/.bash_profile; python $portalScriptsPath/checkServices.py >> $portalOutput/log/checkServices.log 2>&1" >> /var/spool/cron/root
        echo "*/15 * * * * . /root/.bash_profile; python $portalScriptsPath/checkCpuUsage.py >> $portalOutput/log/checkCpuUsage.log 2>&1" >> /var/spool/cron/root
        echo "*/15 * * * * . /root/.bash_profile; python $portalScriptsPath/checkPostgresUser.py >> $portalOutput/log/checkPostgresUser.log 2>&1" >> /var/spool/cron/root
        echo "##### End of Monitoring Scripts Cron entries ### " >> /var/spool/cron/root
        dos2unix -q $portalScriptsPath/*
        echo "Cron Entries Addition is successful"
}

function removalCron()
{
                IFW_HOME="/home/portal"
        echo "Starting removal of Cron"
        echo "Starting cron entries clean-up"
        echo "Taking backup of Cron.."
        touch $IFW_HOME/CronTab_`date +%Y-%m-%d`.txt
        crontab -l > $IFW_HOME/CronTab_`date +%Y-%m-%d`.txt
        echo "Backup of cron is done"
        crontab -r
        sed -i '/#####/,/#####/d' $IFW_HOME/CronTab_`date +%Y-%m-%d`.txt
        cat $IFW_HOME/CronTab_`date +%Y-%m-%d`.txt >> /var/spool/cron/root
        chmod 600 /var/spool/cron/root
        echo "Clean-up of Cron is completed"
        echo "Restored Cron"
}

if [[ $valPassed -eq 2 ]];then
        echo "Its upgrade"
        removalCron
        directoriesCreation
        cronAddition
else
        echo "Its fresh-install"
        directoriesCreation
        cronAddition
fi