############################################################################
#                               MONITORING
#############################################################################
#############################################################################
# (c)2016 Nokia
# Author: monitoring tool team
# Version: 1.0
# Date:20-09-2017
# Purpose :  to get the thread count 
#############################################################################
# Code Modification History
# 1. Bug storm nodes not able to get by utility script
#############################################################################

import os,csv,commands,datetime,re
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")

def headerToCsv():
        f1 = open(Output_file, "a+")
        prev_data = open(Output_file, 'r').read()
        header = 'Date, Node, user, Process Name, Thread Count'
        if prev_data == '':
                f1.write(header + '\n')
        f1.close

def hadoopNameNodes():
        global hadoopnodes
        hadoopnodes = cemod_hive_hosts.split()
        return hadoopnodes

def etlNodes():
        etlNodes = cemod_kafka_app_hosts.split()
        return etlNodes

def boxiNodes():
        boxinodes = cemod_boxi_hosts.split()
        return boxinodes

def analyticNodes():
        analyticnodes = cemod_analytics_hosts.split()
        print 'ANALYTICS',analyticnodes
        return analyticnodes

def configureValueToSeeAbnormality(process2):
	global LOG_HANDLE
	logs = commands.getoutput("ssh %s ps -eLf|grep %s|grep -vi jps"%(n,process2[0]))
	LOG_HANDLE.write(logs + '\n')

def countProcess(count):
        if not process[1]:
                pname = commands.getoutput("ssh %s ps -eaf|grep %s|grep -v root | gawk '{for (I=1;I<=NF;I++) if ($I == \"-name\") {print $(I+1)};}'| cut -d. -f2"%(n,process[0]))
                writer = currentTime + n + "," + uid + "," + pname + "," + count
        else:
                writer = currentTime + n + "," + uid + "," + process[1] + "," + count
                LOG_HANDLE.write(writer + '\n')
        return writer

def evaluateProcess(value):
	global uid,process
	process = re.split("\s+",value)
	count = commands.getoutput("ssh %s ps -eLf|grep -c %s|grep -vi jps"%(n,process[0]))
	if int(count) >= 400:      ####### Configure value to see the abnormality
		writer = configureValueToSeeAbnormality(process)
        uid = commands.getoutput("ssh %s ps -eLf|grep %s|awk \'{print $1}\'|grep -v root|sort -u"%(n,process[0])).replace("\n"," ")
	witer=countProcess(count)
	return witer
	
def processNode():
	global OUTPUT_FILE
	cmd_jps = 'ssh %s jps|egrep -vi \"jps|FsShell|process information unavailable\"|awk \'{print $1,$2}\''%(n)
	name = commands.getoutput(cmd_jps)
	process = re.split("\n",name)
	print "Processes: ",process
	try:
		for value in process:
			writer=evaluateProcess(value)
			print writer
			OUTPUT_FILE.write(writer + '\n')
	except ValueError: print 'No process found for %s' %n

def getThreadCount():
	global OUTPUT_FILE,n
	
	OUTPUT_FILE = open(Output_file, "a+")
	nodes = hadoopNameNodes() + etlNodes() + analyticNodes() + boxiNodes()
	for n in nodes:
		processNode()
	return n

def main():
	global LOG_HANDLE, OUTPUT_FILE, Output_file, currentTime
	
	now = datetime.datetime.now()
	date = now.strftime("%Y-%m-%d")
	currentTime = now.strftime("%Y-%m-%d %H:%M,")

	Log = "/var/local/monitoring/log/threadcount_%s.log"%(date)
	Output_file = "/var/local/monitoring/output/osStats/threadcount_%s.csv"%(date)

	LOG_HANDLE = open(Log, "a+")
	headerToCsv()
	getThreadCount()
	OUTPUT_FILE.close()
	LOG_HANDLE.close()


if __name__ == "__main__":
	main()
