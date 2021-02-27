hive_log_dir=$1
query=$2
flag=$3
cd $hive_log_dir
file_name=`find . -maxdepth 1 -name \hadoop-cmf-HIVE-HIVESERVER2-*.log.out.1 -print`

if [[ $file_name == "" ]]
then
	numOfRows=`cat $hive_log_dir/hadoop-cmf-HIVE-HIVESERVER2-*.log.out | grep $thread | grep "FSStatsscheduler" | grep "Read stats for" | grep "numRows" |grep -oP 'numRows\K.*' |sed -e 's/^[ \t]*//'`
	
else
	if [[ $thread == "" ]]
	then
		thread=`cat $hive_log_dir/hadoop-cmf-HIVE-HIVESERVER2-*.log.out.1 | grep $query -C 2 |grep "Executing command" | grep -o 'Thread-[^"]*]'`
	fi
	
	if [[ $numOfRows == "" ]]
	then
		numOfRows=`cat $hive_log_dir/hadoop-cmf-HIVE-HIVESERVER2-*.log.out.1 | grep $thread | grep "FSStatsscheduler" | grep "Read stats for" | grep "numRows" |grep -oP 'numRows\K.*' |sed -e 's/^[ \t]*//'`
	fi
	
fi

if [ $Total -gt 0 ] ; then
	echo ~$Total
fi
