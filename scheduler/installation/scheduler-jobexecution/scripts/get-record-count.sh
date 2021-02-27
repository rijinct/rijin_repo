hive_log_dir=$1
query=$2
flag=$3
cd $hive_log_dir
file_name=`printf '%s\0' *.txt | xargs -0 grep -l "$query"`
if [ "$file_name" == "" ]; then
exit 0
fi
awk 'f;/'"$query"'/{f=1}' $file_name | grep "TaskEnd ROWS_INSERTED" | awk -F\" '{print $2}'
if [ $flag == "true" ]
then
rm -rf "$hive_log_dir""/""$file_name"
fi