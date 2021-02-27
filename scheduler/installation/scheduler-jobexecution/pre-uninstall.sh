scheduler_HOME="/opt/project/rithomas/scheduler"

if [ $1 == 0 ]
then

echo "Unistalling scheduler RPM....."
echo "DO YOU REALLY WANT TO UNINSTALL PROJECT-RITHOMAS-scheduler RPM (YES/NO):"
while [ true ]
do
  read answer1 < /dev/tty;
  answer=`echo $answer1 | tr [:lower:] [:upper:]`
  if [ "$answer" = "NO" ]
  then
    echo "EXITING RPM UNINSTALLATION...";
    exit -1;
  fi
  if [ "$answer" = "YES" ]
  then 
    break;
  else
    echo "Please input only YES/NO";
  fi
done

su - rithomas -c "sh /opt/project/rithomas/scheduler/app/bin/stop-scheduler-service.sh"


sleep 1

echo "Successfully uninstalled PROJECT-RITHOMAS-scheduler RPM."
fi
