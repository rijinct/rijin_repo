
. ~/.bash_profile
source $NGDB_HOME/ifw/lib/application/utils/application_definition.sh
DATA_QUALITY_METRICS_HOME="/opt/nsn/ngdb/data-quality-metrics"
dqhi_path=$DATA_QUALITY_METRICS_HOME/data-quality-health-index
valPassed=$1
repo_password=`perl /opt/nsn/ngdb/ifw/lib/common/decryption_util.pl 1118F3D997E3009ED8D66602C6582BD5`
data_password=`perl /opt/nsn/ngdb/ifw/lib/common/decryption_util.pl 03CA1BA71F7BC6AC092E9E883B5079D9`
ws_password=`perl /opt/nsn/ngdb/ifw/lib/common/decryption_util.pl CCC26EF3647B60616513A052D3B340FC`
um_password=`perl /opt/nsn/ngdb/ifw/lib/common/decryption_util.pl CB871ADAEA7B0FB59E525C9AA0FBF8FC`
psqlStatement="psql -h $project_postgres_sdk_fip_active_host -d $project_sdk_db_name -v repo_password=\'${repo_password}\' -v data_password=\'${data_password}\' -v ws_password=\'${ws_password}\' -v um_password=\'${um_password}\' "

function createCronEntries()
{
echo "###### Cron entry for Data Quality Metrics Scripts ######" >> /var/spool/cron/root
echo "10 0 * * * . /root/.bash_profile; cd /opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/; /usr/local/bin/python3.7 -m com.rijin.dqhi.score_calculator_wrapper" >> /var/spool/cron/root
echo "###### End of Data Quality Metrics Scripts Cron entries ###### " >> /var/spool/cron/root

dos2unix -q $scriptsHome/scripts/*

echo "Cron Entries Addition is successful"
}


function removalCron()
{
echo "Starting removal of Cron"
echo "Starting cron entries clean-up"
echo "Taking backup of Cron.."
touch $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
crontab -l > $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
echo "Backup of cron is done"
crontab -r
sed -i '/######/,/######/d' $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt
cat $NGDB_HOME/CronTab_`date +%Y-%m-%d`.txt >> /var/spool/cron/root
chmod 600 /var/spool/cron/root
echo "Clean-up of Cron is completed"
echo "Restored Cron"
}



function checkIfTableExists()
{
	table_name=$1
	tableExists=`ssh ${project_postgres_sdk_fip_active_host} "/opt/nsn/ngdb/pgsql/bin/psql -U $project_sdk_schema_name -d $project_sdk_db_name  -c \\"SELECT EXISTS (SELECT 1 FROM   information_schema.tables  WHERE  table_schema = '$project_sdk_schema_name' AND    table_name = '$table_name' );\\"" | egrep -v "row|--|exists" | tr -d [:space:]`
	echo $tableExists
}

function createDQHITables()
{
	tableExists=$(checkIfTableExists "dq_kpi_definition")
	if [[ "$tableExists" != "t" ]];then
		su - postgres -c "$psqlStatement"<<EOF >> /dev/null
		
		\i $DATA_QUALITY_METRICS_HOME/data-quality-health-index/sql/schema/create_dqhi_tables.sql		
		\q
EOF
	echo "DQHI tables are created."
	fi
}

function deleteDqhiTable()
{
	su - postgres -c "$psqlStatement"<<EOF >> /dev/null

	\i $DATA_QUALITY_METRICS_HOME/data-quality-health-index/sql/schema/delete_dqhi_tables.sql
	\q
EOF
}

function freshInstallOrUpgrade()
{
	if [[ $valPassed -eq 2 ]];then
		echo "Its upgrade"
		removalCron
	fi
	createCronEntries
	deleteDqhiTable
	createDQHITables
}

freshInstallOrUpgrade
