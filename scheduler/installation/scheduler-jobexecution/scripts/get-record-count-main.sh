. ~/.bash_profile
source $RITHOMAS_HOME/ifw/lib/application/utils/application_definition.sh

scheduler_BIN=$RITHOMAS_HOME/scheduler/app/bin/
hive_log_dir=$1
query=$2
flag=$3

hostname=`hostname --fqdn`
distribution=`perl -e "require \"$RITHOMAS_HOME/ifw/utils/Logging.pl\"; require \"$RITHOMAS_HOME/ifw/utils/Utilities.pl\";print getPropertyValuebyTPName("Distribution","distribution");"`
scriptToExecute="get-record-count.sh"
if [ "$distribution" == "cloudera" ]
then
        scriptToExecute="get-record-count-HS2.sh"
fi
for eachHiveHost in `echo ${project_hive_hosts[@]}`
do
	ssh $eachHiveHost "sh $scheduler_BIN/$scriptToExecute $hive_log_dir $query $flag"
done
