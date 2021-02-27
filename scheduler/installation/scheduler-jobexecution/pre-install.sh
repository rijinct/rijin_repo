
. ~/.bash_profile
export scheduler_HOME=$RITHOMAS_HOME/scheduler/app

backupFiles()
{
	if [ -f $scheduler_HOME/conf/spark_settings.xml ]; then
		cp $scheduler_HOME/conf/CEM_FV_File_Details_Config.xml "$scheduler_HOME/conf/CEM_FV_File_Details_Config_`date +"%d-%m-%Y_%H%M%S"`.xml"
		cp $scheduler_HOME/conf/spark_settings.xml "$scheduler_HOME/conf/spark_settings_`date +"%d-%m-%Y_%H%M%S"`.xml"
		cp $scheduler_HOME/conf/hive_settings.xml "$scheduler_HOME/conf/hive_settings_`date +"%d-%m-%Y_%H%M%S"`.xml"
	fi
}

backupFiles