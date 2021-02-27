
. ~/.bash_profile
source $RITHOMAS_HOME/ifw/lib/application/utils/application_definition.sh
export scheduler_HOME=$RITHOMAS_HOME/scheduler/app
export SDK_HOME=$RITHOMAS_HOME/sdk/app
scheduler_LOG_DIR="$scheduler_HOME/log"
LOG_FILE="$scheduler_LOG_DIR/schedulerInstallation.log"
UPGRADE_LOG_FILE="$scheduler_LOG_DIR/schedulerUpgrade_`date +"%d-%m-%Y_%H%M%S"`.log"
keytab_location=$project_application_rithomas_user_home_dir/$project_application_rithomas_user.keytab
principal=`klist -kt  $keytab_location | head -4|tail -1|awk '{print $4}'`


hostname=`hostname --fqdn`

updateConfigFiles() {
if [ ${project_scheduler_hosts[0]} == $hostname ]
then
	for eachHiveHost in `echo ${project_hive_hosts[@]}`
	do
		ssh $eachHiveHost "chmod g+w /var/local/hive/hive-querylog/" 2> /dev/null
		ssh $eachHiveHost "mkdir -p 775 $RITHOMAS_HOME/scheduler/app/bin/"
		`scp $RITHOMAS_HOME/scheduler/app/bin/get-record-count.sh $eachHiveHost:$RITHOMAS_HOME/scheduler/app/bin/`
		`scp $RITHOMAS_HOME/scheduler/app/bin/get-record-count-HS2.sh $eachHiveHost:$RITHOMAS_HOME/scheduler/app/bin/`
		ssh $eachHiveHost "chown :ninstall -R $RITHOMAS_HOME/scheduler/"
		ssh $eachHiveHost "chmod 775 -R $RITHOMAS_HOME/scheduler/"
	done
fi
	
}

createDirectories() {
	touch $LOG_FILE
createDirectories
assignPermissions
updateConfigFiles
appendQuartAgentPathToschedulerStartscript 
		

if [ $1 -eq 1 ] ; then
		echo "RPM Successfully installed." | tee -a $LOG_FILE
	else
		echo "RPM Successfully Upgraded." | tee -a $UPGRADE_LOG_FILE
fi


