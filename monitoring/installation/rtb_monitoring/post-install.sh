#! /bin/bash
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This Post Installation Script for Monitoring RPM is to create directories
# and cron entries needed for all Monitoring scripts
# Date:    28-11-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################
. ~/.bash_profile
monitoringOutput="/var/local/monitoring"
monitoringScriptsPath="/opt/nsn/rtb/monitoring/scripts"
valPassed=$1
function directoriesCreation()
{
        echo "Starting Post Install Steps..."
        ## Output Directory Creation ##
        mkdir -p $monitoringOutput
        ## Output Directories for different monitoring utilities #
        mkdir -p $monitoringOutput/output/diskSpaceUtilization
        mkdir -p $monitoringOutput/output/memoryUtilization
        mkdir -p $monitoringOutput/output/cpuUtilization
		mkdir -p $monitoringOutput/output/serviceStability
		mkdir -p $monitoringOutput/output/dataSourceReachability
        mkdir -p $monitoringOutput/output/cemodSourceReachability		
        ## Directory creation for work ##
        mkdir -p $monitoringOutput/work
        ## Directory creation for log ##
        mkdir -p $monitoringOutput/log
}
function getTypeOfLinux()
{
        typeOfOs=`cat /etc/*Version | grep "RHEL" | gawk -F" " '{print $2}' | gawk -F"_" '{print $3}' | tr -d [U9]`
        echo $typeOfOs
}

function cronAddition()
{
        ### Cron Addition ###
        echo "##### Monitoring Scripts Cron entries ### " >> /var/spool/cron/root
        echo "*/15 * * * * . /root/.bash_profile; python $monitoringScriptsPath/checkDiskUsage.py >> $monitoringOutput/log/checkDiskUsage.log 2>&1" >> /var/spool/cron/root
        echo "*/15 * * * * . /root/.bash_profile; python $monitoringScriptsPath/checkMemoryUsage.py >> $monitoringOutput/log/checkMemoryUsage.log 2>&1" >> /var/spool/cron/root
        echo "*/15 * * * * . /root/.bash_profile; python $monitoringScriptsPath/checkServicesStability.py >> $monitoringOutput/log/checkServices.log 2>&1" >> /var/spool/cron/root
		echo "*/15 * * * * . /root/.bash_profile; python $monitoringScriptsPath/checkCPUUsage.py >> $monitoringOutput/log/checkCPUUsage.log 2>&1" >> /var/spool/cron/root
		if [[ `getTypeOfLinux` == "RHEL6" ]];then
			echo "*/15 * * * * . /root/.bash_profile; python $monitoringScriptsPath/checkDrbd.py >> $monitoringOutput/log/checkDrbd.log 2>&1" >> /var/spool/cron/root
		fi
		echo "*/5 * * * * . /root/.bash_profile; python $monitoringScriptsPath/checkDataSourcePing.py >> $monitoringOutput/log/checkDataSource.log 2>&1" >> /var/spool/cron/root
        echo "##### End of Monitoring Scripts Cron entries ### " >> /var/spool/cron/root
        dos2unix $monitoringScriptsPath/*
        echo "Cron Entries Addition is successful"
}

function removalCron()
{
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