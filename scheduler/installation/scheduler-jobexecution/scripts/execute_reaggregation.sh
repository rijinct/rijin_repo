JOB_NAME=$1
enableGroupList=""
disableGroupList=""

function usage() {
echo "Please give the inputs in proper order and format..
Usage:$FILE_NAME (<AggregationJobId> '<StartTime>' '<EndTime>' [<RegionID>])
Usage:$FILE_NAME (-e <JobGroup> -d <JobGroup> '<StartTime>' '<EndTime>' [<RegionID>])
Usage:$FILE_NAME (-e '<JobGroup1>,<JobGroup2>..' -d '<JobGroup1>,<JobGroup2>..' '<StartTime>' '<EndTime>' [<RegionID>])
Usage:$FILE_NAME (-e '<JobGroup1>,<JobGroup2>..' '<StartTime>' '<EndTime>' [<RegionID>])
Usage:$FILE_NAME (-d '<JobGroup1>,<JobGroup2>..' '<StartTime>' '<EndTime>' [<RegionID>])" >>$LOG_FILE_NAME;
echo -e "\nPlease give the inputs in proper order and format..
Usage:$FILE_NAME (<AggregationJobId> '<StartTime>' '<EndTime>' [<RegionID>])
Usage:$FILE_NAME (-e <JobGroup> -d <JobGroup> '<StartTime>' '<EndTime>' [<RegionID>])
Usage:$FILE_NAME (-e '<JobGroup1>,<JobGroup2>..' -d '<JobGroup1>,<JobGroup2>..' '<StartTime>' '<EndTime>' [<RegionID>])
Usage:$FILE_NAME (-e '<JobGroup1>,<JobGroup2>..' '<StartTime>' '<EndTime>' [<RegionID>])
Usage:$FILE_NAME (-d '<JobGroup1>,<JobGroup2>..' '<StartTime>' '<EndTime>' [<RegionID>])"
exit;
}

echo "Start of program:`date`" >>$LOG_FILE_NAME;

if [ $NUM_OF_ARGS -lt 3 ]
then
usage
fi

while getopts 'e:d:' opt; do
  case $opt in
    e)
        enableGroupList=$OPTARG
		START_TIME="$5"
		END_TIME="$6"
		REGION_ID="$7"
      ;;
    d)
        disableGroupList=$OPTARG
		START_TIME="$5"
		END_TIME="$6"
		REGION_ID="$7"
      ;;
    *)
      usage
      ;;
  esac
done

if [ -z $enableGroupList ] || [ -z $disableGroupList ] 
then
START_TIME="$3"
END_TIME="$4"
REGION_ID="$5"
fi

if [ -z $enableGroupList ] && [ -z $disableGroupList ] 
then
JOB_NAME=$1
START_TIME="$2"
END_TIME="$3"
REGION_ID="$4"
fi

res=`echo $START_TIME | grep -E '^20[0-9]{2}-[0-9]{2}-[0-9]{2} [0-2][0-9]:[0-5][0-9]:[0-5][0-9]$'`;
if [ $? -ne 0 ]; then
		echo "Please input the start date in the format: YYYY-MM-DD HH24:MI:SS";
		exit;
fi

res=`echo $END_TIME | grep -E '^20[0-9]{2}-[0-9]{2}-[0-9]{2} [0-2][0-9]:[0-5][0-9]:[0-5][0-9]$'`;
if [ $? -ne 0 ]; then
		echo "Please input the end date in proper format: YYYY-MM-DD HH24:MI:SS";
		exit;
fi


echo "Parameters for reaggregation:" >>$LOG_FILE_NAME;
echo "Job name: $1 $enableGroupList" >> $LOG_FILE_NAME;
echo "Start time: $START_TIME" >> $LOG_FILE_NAME;
echo "End time: $END_TIME" >> $LOG_FILE_NAME;
echo "Region id: $REGION_ID" >> $LOG_FILE_NAME;
commonsLang3JarPath=`ls $scheduler_HOME/app/lib/3rdparty/commons-lang3-*`
eclipselinkJarPath=`ls $scheduler_HOME/app/lib/3rdparty/eclipselink-*`
javaxPersistenceJarPath=`ls $scheduler_HOME/app/lib/3rdparty/javax.persistence-*`
javaxElJarPath=`ls $scheduler_HOME/app/lib/3rdparty/javax.el-*`
forcedOrderClassPath=$commonsLang3JarPath:$eclipselinkJarPath:$javaxPersistenceJarPath:$javaxElJarPath

echo -e "Configuring job for re-aggregation......\n"

if [ ! -z $enableGroupList ] || [ ! -z $disableGroupList ]
then
{
	SDK_SCHED_CMD="$JAVA_HOME/bin/java -Dscheduler_HOME=$scheduler_HOME\ -DSDK_HOME=$SDK_HOME\
		-Dorg.quartz.properties=$scheduler_HOME/app/conf/client.properties\
		-Dquartz.jmx.url=service:jmx:rmi:///jndi/rmi://${project_scheduler_active_fip_host}:1099/jmxrmi\
		-Dquartz.scheduler.object.name=quartz:type=Quartzscheduler,name=Jmxscheduler,instanceId=NONE_CLUSTER\
