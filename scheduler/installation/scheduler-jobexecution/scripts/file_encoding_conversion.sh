fileCountCheck=`ls $1 | wc -l`
if [[ $fileCountCheck -eq 0 ]];then
        >&2 echo "No files present in $1"
        exit -1
fi
