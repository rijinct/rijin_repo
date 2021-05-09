#! /bin/bash
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: Un-install script for Monitoring RPM 
# Date:    31-08-2016
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################
. ~/.bash_profile
source $NGDB_HOME/ifw/lib/application/utils/application_definition.sh
echo $NGDB_HOME
valPassed=$1
function unInstall()
{
echo "Starting Uninstallation..."
echo "Starting clean-up.."
rm -rf $NGDB_HOME/monitoring
echo "Clean-up of directories is completed"
echo "Starting cron entries clean-up"
echo "Taking backup of Cron.."
touch $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
crontab -l > $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
echo "Backup of cron is done"
crontab -r
sed -i '/ ### /,/ ### /d' $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
cat $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt >> /var/spool/cron/root
chmod 600 /var/spool/cron/root
echo "Clean-up of Cron is completed"
echo "Restored Cron"
echo "Un-installation Successful"
}

function deleteMonitoringTable()
{
	ssh ${cemod_postgres_sdk_fip_active_host} "/opt/nsn/ngdb/pgsql/bin/psql -U $cemod_sdk_schema_name -d $cemod_sdk_db_name  -c \"drop table if exists hm_stats;\""

}

if [[ $valPassed -eq 0 ]];then
echo "Proceeding with Complete Un-installation"
pip uninstall -y html
echo "Removed html-1.16.tar.gz"
unInstall
deleteMonitoringTable
else
echo "Upgrade Un-installation,nothing to perform"
fi

