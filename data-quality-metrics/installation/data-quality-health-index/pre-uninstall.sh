. ~/.bash_profile
source $NGDB_HOME/ifw/lib/application/utils/application_definition.sh
valPassed=$1
DATA_QUALITY_METRICS_HOME="/opt/nsn/ngdb/data-quality-metrics"
repo_password=`perl /opt/nsn/ngdb/ifw/lib/common/decryption_util.pl 1118F3D997E3009ED8D66602C6582BD5`
data_password=`perl /opt/nsn/ngdb/ifw/lib/common/decryption_util.pl 03CA1BA71F7BC6AC092E9E883B5079D9`
ws_password=`perl /opt/nsn/ngdb/ifw/lib/common/decryption_util.pl CCC26EF3647B60616513A052D3B340FC`
um_password=`perl /opt/nsn/ngdb/ifw/lib/common/decryption_util.pl CB871ADAEA7B0FB59E525C9AA0FBF8FC`
psqlStatement="psql -h $project_postgres_sdk_fip_active_host -d $project_sdk_db_name -v repo_password=\'${repo_password}\' -v data_password=\'${data_password}\' -v ws_password=\'${ws_password}\' -v um_password=\'${um_password}\' "

function unInstall()
{
echo "Starting Uninstallation..."
echo "Starting clean-up.."
deleteDqhiTable
echo "Deleted dqhi tables"
rm -rf $NGDB_HOME/data-quality-metrics
echo "Clean-up of directories is completed"
echo "Starting cron entries clean-up"
echo "Taking backup of Cron.."
touch $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
crontab -l > $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
echo "Backup of cron is done"
crontab -r
sed -i '/ ######/,/ ######/d' $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
cat $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt >> /var/spool/cron/root
chmod 600 /var/spool/cron/root
echo "Clean-up of Cron is completed"
echo "Restored Cron"
echo "Un-installation Successful"
}

function deleteDqhiTable()
{
su - postgres -c "$psqlStatement"<<EOF >> /dev/null

\i $DATA_QUALITY_METRICS_HOME/data-quality-health-index/sql/schema/delete_dqhi_tables.sql
\q
EOF
}

if [[ $valPassed -eq 0 ]];then
echo "Proceeding with Complete Un-installation"
unInstall
else
echo "Upgrade Un-installation,nothing to perform"
fi

