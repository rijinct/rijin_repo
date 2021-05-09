############################################################################
#                           Monitor QS_cache_count
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  shivam.1.sharma@nokia.com
# Version: 0.1
# Purpose:
#
# Date:    19-10-2016
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

import commands, datetime, time, smtplib,sys
from xml.dom import minidom
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from snmpUtils import SnmpUtils

global getSforCP
global addToCsv
global final
global cqi_cache_val
global cei_cache_val
global ri_cache_val
global ott_cache_val
global hvci_cache_val
global CP_S_String
global final_out
global message
global sender
global receivers

logPath = '/var/local/monitoring/'
commonIfwPath = '/opt/nsn/ngdb/ifw/etc/common'

status,startTime = commands.getstatusoutput('date')

'''Create an output "QS_cache_count.csv"'''
def createOutputCSV():
        #status,makeDIR = commands.getstatusoutput('mkdir -p %soutput/qsCache')
        header = 'Date,S_Cumulative,S,O,D,S_CQI_Expected,S_CQI_Actual,S_CEI_Expected,S_CEI_Actual,S_VOLTE_Expected,S_VOLTE_Actual\n'
        status1,output1 = commands.getstatusoutput('ls %soutput/qsCache/QS_cache_count_DAY.csv' %(logPath))
        if status1 != 0:
                new_file = open(logPath+'output/qsCache/QS_cache_count_DAY.csv','w')
                new_file.write(header)
                new_file.close()
        status2,output2 = commands.getstatusoutput('ls %soutput/qsCache/QS_cache_count_WEEK.csv' %(logPath))
        if status2 != 0:
                new_file = open(logPath+'output/qsCache/QS_cache_count_WEEK.csv','w')
                new_file.write(header)
                new_file.close()

'''Get current date dt from cache_tab'''
def getdt():
        global minTime_epoch
        global maxTime_epoch
        minTime = datetime.time().min
        maxTime = datetime.time().max
        minTime = datetime.datetime.combine(today,minTime)
        maxTime = datetime.datetime.combine(today,maxTime)
        minTime_epoch = int(time.mktime(time.strptime(str(minTime), '%Y-%m-%d %H:%M:%S')))*1000
        maxTime_epoch = int(time.mktime(time.strptime(str(maxTime), '%Y-%m-%d %H:%M:%S.%f')))*1000

def getDateTime():
        currentTime = str(datetime.datetime.now().time()).split('.')[0]
        dateTime = str(today)+ ' ' + currentTime
        return dateTime

def getWEEK_fromWS_Scheduled():
        global tag
        global expectedCount_week
        expectedCount_week = {}

        getSODCount('WEEK')

#'''CQI week count'''
        cmd = 'ssh %s "psql sai saiws -c \\"select count(distinct ws_request::text) from ws_scheduled_cache_tab where (ws_request::text ilike %s or ws_request::text ilike %s) and aggregation_table_name ilike %s and aggregation_table_name not ilike %s and is_scheduled=%s;\\""' %(postgres,"'%<subscription>CEMBOARD:CQI%'","'%<subscription>CEMBOARD:OTT%'","'%week%'","'%VOLTE%'","'t'")
        status,ws_CQI_week = commands.getstatusoutput(cmd)
        ws_CQI_week = ws_CQI_week.split()[2]
        if status == 0:
                expectedCount_week['CQI']=ws_CQI_week
        else: print 'Unable to get CQI week count from ws_scheduled_cache_tab'

#'''CEI week count'''
        cmd = 'ssh %s "psql sai saiws -c \\"select count(distinct ws_request::text) from ws_scheduled_cache_tab where ws_request::text ilike %s and aggregation_table_name ilike %s and is_scheduled=%s;\\""' %(postgres,"'%<subscription>CEMBOARD%'","'%CEI2%week%'","'t'")
        status,ws_CEI_week = commands.getstatusoutput(cmd)
        ws_CEI_week = ws_CEI_week.split()[2]
        if status == 0:
                expectedCount_week['CEI']=ws_CEI_week
        else: print 'Unable to get CEI week count from ws_scheduled_cache_tab'

#'''HVCI week count'''
        cmd = 'ssh %s "psql sai saiws -c \\"select count(distinct ws_request::text) from ws_scheduled_cache_tab where ws_request::text ilike %s and aggregation_table_name ilike %s;\\""' %(postgres,"'%<subscription>CEMBOARD%'","'%hv%week%'")
        status,ws_HVCI_week = commands.getstatusoutput(cmd)
        ws_HVCI_week = ws_HVCI_week.split()[2]
        if status == 0:
                expectedCount_week['HVCI']=ws_HVCI_week
        else: print 'Unable to get CEI week count from ws_scheduled_cache_tab'

#'''VOLTE week count'''
        cmd = 'ssh %s "psql sai saiws -c \\"select count(distinct ws_request::text) from ws_scheduled_cache_tab where ws_request::text ilike %s and aggregation_table_name ilike %s and is_scheduled=%s;\\""' %(postgres,"'%<subscription>CEMBOARD%'","'%volte%week%'","'t'")
        status,ws_VOLTE_week = commands.getstatusoutput(cmd)
        ws_VOLTE_week = ws_VOLTE_week.split()[2]

        if status == 0:
                expectedCount_week['VOLTE']=ws_VOLTE_week
        else: print 'Unable to get VOLTE week count from ws_scheduled_cache_tab'


#'''FL OTT week count'''
        cmd = 'ssh %s "psql sai saiws -c \\"select count(distinct ws_request::text) from ws_scheduled_cache_tab where ws_request::text ilike %s and aggregation_table_name ilike %s and is_scheduled=%s;\\""' %(postgres,"'%<subscription>CEMBOARD:FL OTT%'","'%week%'","'t'")
        status,ws_FL_OTT_week = commands.getstatusoutput(cmd)
        ws_FL_OTT_week = ws_FL_OTT_week.split()[2]

        if status == 0:
                expectedCount_week['FL_OTT']=ws_FL_OTT_week
        else: print 'Unable to get FL OTT week count from ws_scheduled_cache_tab'

        tag = 'WEEK'
        if 'CQI' in expectedCount_week.keys():
                                checkCQI(tag,expectedCount_week['CQI'])
        if 'CEI' in expectedCount_week.keys():
                                checkCEI(tag,expectedCount_week['CEI'])
        if 'HVCI' in expectedCount_week.keys():
                                checkHVCI(tag,expectedCount_week['HVCI'])
        if 'FL_OTT' in expectedCount_week.keys():
                                checkFLOTT(tag,expectedCount_week['FL_OTT'])
        if 'VOLTE' in expectedCount_week.keys():
                                checkVOLTE(tag,expectedCount_week['VOLTE'])

        getSforCP()
        addToCsv()

def getDAY_fromWS_Scheduled():
        global tag
        global expectedCount_day
        expectedCount_day = {}

        getSODCount('DAY')
#'''CQI day count'''
        cmd = 'ssh %s "psql sai saiws -c \\"select count(distinct ws_request::text) from ws_scheduled_cache_tab where (ws_request::text ilike %s or ws_request::text ilike %s) and aggregation_table_name ilike %s and aggregation_table_name not ilike %s and is_scheduled=%s;\\""' %(postgres,"'%<subscription>CEMBOARD:CQI%'","'%<subscription>CEMBOARD:OTT%'","'%day%'","'%VOLTE%'","'t'")
        status,ws_CQI_day = commands.getstatusoutput(cmd)
        ws_CQI_day = ws_CQI_day.split()[2]
        if status == 0:
                expectedCount_day['CQI']=ws_CQI_day
        else: print 'Unable to get CQI day count from ws_scheduled_cache_tab'

#'''CEI day count'''
        cmd = 'ssh %s "psql sai saiws -c \\"select count(distinct ws_request::text) from ws_scheduled_cache_tab where ws_request::text ilike %s and aggregation_table_name ilike %s and is_scheduled=%s;\\""' %(postgres,"'%<subscription>CEMBOARD%'","'%CEI2%day%'","'t'")
        status,ws_CEI_day = commands.getstatusoutput(cmd)
        ws_CEI_day = ws_CEI_day.split()[2]
        if status == 0:
                expectedCount_day['CEI']=ws_CEI_day
        else: print 'Unable to get CEI day count from ws_scheduled_cache_tab'

#'''HVCI day count'''
        cmd = 'ssh %s "psql sai saiws -c \\"select count(distinct ws_request::text) from ws_scheduled_cache_tab where ws_request::text ilike %s and aggregation_table_name ilike %s;\\""' %(postgres,"'%<subscription>CEMBOARD%'","'%hv%day%'")
        status,ws_HVCI_day = commands.getstatusoutput(cmd)
        ws_HVCI_day = ws_HVCI_day.split()[2]
        if status == 0:
                expectedCount_day['HVCI']=ws_HVCI_day
        else: print 'Unable to get CEI day count from ws_scheduled_cache_tab'

#'''VOLTE day count'''
        cmd = 'ssh %s "psql sai saiws -c \\"select count(distinct ws_request::text) from ws_scheduled_cache_tab where ws_request::text ilike %s and aggregation_table_name ilike %s and is_scheduled=%s;\\""' %(postgres,"'%<subscription>CEMBOARD%'","'%volte%day%'","'t'")
        status,ws_VOLTE_day = commands.getstatusoutput(cmd)
        ws_VOLTE_day = ws_VOLTE_day.split()[2]
        if status == 0:
                expectedCount_day['VOLTE']=ws_VOLTE_day
        else: print 'Unable to get VOLTE day count from ws_scheduled_cache_tab'

#'''FL OTT day count'''
        cmd = 'ssh %s "psql sai saiws -c \\"select count(distinct ws_request::text) from ws_scheduled_cache_tab where ws_request::text ilike %s and aggregation_table_name ilike %s and is_scheduled=%s;\\""' %(postgres,"'%<subscription>CEMBOARD:FL OTT%'","'%day%'","'t'")
        status,ws_FL_OTT_day = commands.getstatusoutput(cmd)
        ws_FL_OTT_day = ws_FL_OTT_day.split()[2]
        if status == 0:
                expectedCount_day['FL_OTT']=ws_FL_OTT_day
        else: print 'Unable to get FL OTT day count from ws_scheduled_cache_tab'

        tag = 'DAY'
        if 'CQI' in expectedCount_day.keys():
                checkCQI(tag,expectedCount_day['CQI'])
        if 'CEI' in expectedCount_day.keys():
                checkCEI(tag,expectedCount_day['CEI'])
        if 'HVCI' in expectedCount_day.keys():
                checkHVCI(tag,expectedCount_day['HVCI'])
        if 'FL_OTT' in expectedCount_day.keys():
                checkFLOTT(tag,expectedCount_day['FL_OTT'])
        if 'VOLTE' in expectedCount_day.keys():
                checkVOLTE(tag,expectedCount_day['VOLTE'])

        getSforCP()
        addToCsv()

def checkCEI(tag,ceiValue):
        global cei_cache_val
        ceiValue = int(ceiValue)
        if tag == 'DAY':
                cmd = 'ssh %s "psql sai saiws -c \\"select count(*) from cache_tab where last_fetched_time between \'%s\' and \'%s\' and request::text ilike %s and table_name ilike %s and source=\'S\';\\""' %(postgres,minTime_epoch,maxTime_epoch,"'%<subscription>CEMBOARD%'","'%CEI2%day%'")
        elif tag == 'WEEK':
                cmd = 'ssh %s "psql sai saiws -c \\"select count(*) from cache_tab where last_fetched_time between \'%s\' and \'%s\' and request::text ilike %s and table_name ilike %s and source=\'S\';\\""' %(postgres,minTime_epoch,maxTime_epoch,"'%<subscription>CEMBOARD%'","'%CEI2%week%'")
        else: print "No tag available"
        status,cei_cache_val = commands.getstatusoutput(cmd)
        cei_cache_val = int(cei_cache_val.split()[2])
        if cei_cache_val < ceiValue:
                return 'Raise an alarm: Low CEI Count',str(cei_cache_val)
        else: return str(cei_cache_val)

def checkHVCI(tag,hvciValue):
        global hvci_cache_val
        hvciValue = int(hvciValue)
        if tag == 'DAY':
                cmd = 'ssh %s "psql sai saiws -c \\"select count(*) from cache_tab where last_fetched_time between \'%s\' and \'%s\' and request::text ilike %s and table_name ilike %s and source=\'S\';\\""' %(postgres,minTime_epoch,maxTime_epoch,"'%<subscription>CEMBOARD%'","'%hv%day%'")
        elif tag == 'WEEK':
                cmd = 'ssh %s "psql sai saiws -c \\"select count(*) from cache_tab where last_fetched_time between \'%s\' and \'%s\' and request::text ilike %s and table_name ilike %s and source=\'S\';\\""' %(postgres,minTime_epoch,maxTime_epoch,"'%<subscription>CEMBOARD%'","'%hv%week%'")
        else: print "No tag available"
        status,hvci_cache_val = commands.getstatusoutput(cmd)
        hvci_cache_val = int(hvci_cache_val.split()[2])
        if hvci_cache_val < hvciValue:
                return 'Raise an alarm: Low HVCI Count',str(hvci_cache_val)
        else: return str(hvci_cache_val)

def checkCQI(tag,cqiValue):
        global cqi_cache_val
        cqiValue = int(cqiValue)
        if tag == 'DAY':
                cmd = 'ssh %s "psql sai saiws -c \\"select count(*) from cache_tab where last_fetched_time between \'%s\' and \'%s\' and (request::text ilike %s or request::text ilike %s) and table_name ilike %s and table_name not ilike %s and source=\'S\';\\""' %(postgres,minTime_epoch,maxTime_epoch,"'%<subscription>CEMBOARD:CQI%'","'%<subscription>CEMBOARD:OTT%'","'%day%'","'%VOLTE%'")
        elif tag == 'WEEK':
                cmd = 'ssh %s "psql sai saiws -c \\"select count(*) from cache_tab where last_fetched_time between \'%s\' and \'%s\' and (request::text ilike %s or request::text ilike %s) and table_name ilike %s and table_name not ilike %s and source=\'S\';\\""' %(postgres,minTime_epoch,maxTime_epoch,"'%<subscription>CEMBOARD:CQI%'","'%<subscription>CEMBOARD:OTT%'","'%week%'","'%VOLTE%'")
        status,cqi_cache_val = commands.getstatusoutput(cmd)
        cqi_cache_val = int(cqi_cache_val.split()[2])
        if cqi_cache_val < cqiValue:
                return 'Raise an alarm: Low CQI Count',str(cqi_cache_val)
        else: return str(cqi_cache_val)

def checkVOLTE(tag,volteValue):
        global volte_cache_val
        volteValue = int(volteValue)
        if tag == 'DAY':
                cmd = 'ssh %s "psql sai saiws -c \\"select count(*) from cache_tab where last_fetched_time between \'%s\' and \'%s\' and request::text ilike %s and table_name ilike %s and source=\'S\';\\""' %(postgres,minTime_epoch,maxTime_epoch,"'%<subscription>CEMBOARD%'","'%volte%day%'")
        elif tag == 'WEEK':
                cmd = 'ssh %s "psql sai saiws -c \\"select count(*) from cache_tab where last_fetched_time between \'%s\' and \'%s\' and request::text ilike %s and table_name ilike %s and source=\'S\';\\""' %(postgres,minTime_epoch,maxTime_epoch,"'%<subscription>CEMBOARD%'","'%volte%week%'")
        status,volte_cache_val = commands.getstatusoutput(cmd)
        volte_cache_val = int(volte_cache_val.split()[2])
        if volte_cache_val < volteValue:
                return 'Raise an alarm: Low VOLTE Count',str(volte_cache_val)
        else: return str(volte_cache_val)

def checkFLOTT(tag,flottValue):
        global flott_cache_val
        flottValue = int(flottValue)
        if tag == 'DAY':
                cmd = 'ssh %s "psql sai saiws -c \\"select count(*) from cache_tab where last_fetched_time between \'%s\' and \'%s\' and request::text ilike %s and table_name ilike %s and source=\'S\';\\""' %(postgres,minTime_epoch,maxTime_epoch,"'%<subscription>CEMBOARD:FL OTT%'","'%day%'")
        elif tag == 'WEEK':
                cmd = 'ssh %s "psql sai saiws -c \\"select count(*) from cache_tab where last_fetched_time between \'%s\' and \'%s\' and request::text ilike %s and table_name ilike %s and source=\'S\';\\""' %(postgres,minTime_epoch,maxTime_epoch,"'%<subscription>CEMBOARD:FL OTT%'","'%week%'")
        status,flott_cache_val = commands.getstatusoutput(cmd)
        flott_cache_val = int(flott_cache_val.split()[2])
        if flott_cache_val < flottValue:
                return 'Raise an alarm: Low FL OTT Count',str(flott_cache_val)
        else: return str(flott_cache_val)

def getSODCount(AggType):
        global final
        S_cumulative_cmd = 'ssh %s \'su - postgres -c "psql sai -c \\"select count(*) from saiws.cache_tab group by source order by source desc;\\""\'' %(postgres)
        status,S_cum = commands.getstatusoutput(S_cumulative_cmd)
        try:
                if int(S_cum.split()[2]) > 0:
                        final = S_cum.split()[2]+','
                else: print S_cum,'S cumulative count is invalid'
        except ValueError:
                final = ','

        if AggType == 'WEEK':
                input = 'ssh %s "psql sai saiws -c \\"select count(*),source from cache_tab where last_fetched_time between \'%s\' and \'%s\' and table_name not ilike %s and table_name not ilike %s group by source order by source desc;\\""' %(postgres,minTime_epoch,maxTime_epoch,"'es%'","'%day%'")
        elif AggType == 'DAY':
                input = 'ssh %s "psql sai saiws -c \\"select count(*),source from cache_tab where last_fetched_time between \'%s\' and \'%s\' and table_name not ilike %s and table_name not ilike %s group by source order by source desc;\\""' %(postgres,minTime_epoch,maxTime_epoch,"'es%'","'%week%'")
        status2, output2= commands.getstatusoutput(input)
        out = []
        line = output2.split('\n')

        for i in range(len(line)):
                if 'O' in line[i] or 'D' in line[i] or 'S' in line[i]:
                        out.append(line[i])

        if 'S' not in ''.join(out):
                out.insert(0,' | ')
        if 'O' not in ''.join(out):
                out.insert(1,'| ')
        if 'D' not in ''.join(out):
                out.insert(2,' | ')

        for j in range(len(out)):
                s = out[j].strip().split('|')
                final = final + s[0].strip() + ','
        final = getDateTime() + ',' + final[:-1]

'''Get CP specific S counts'''
def getSforCP():
        global SforCP
        SforCP = {}

        try:
                if cqi_cache_val >= 0:
                        SforCP['CQI']=str(cqi_cache_val)
                else: SforCP['CQI']=''

        except NameError:
                print 'CQI data not found'
                SforCP['CQI']=''
        try:
                if cei_cache_val >= 0:
                        SforCP['CEI']=str(cei_cache_val)
                else: SforCP['CEI']=''
        except NameError:
                print 'CEI data not found'
                SforCP['CEI']=''
        try:
                if volte_cache_val >= 0:
                        SforCP['VOLTE']=str(volte_cache_val)
                else: SforCP['VOLTE']=''
        except NameError:
                print 'VOLTE data not found'
                SforCP['VOLTE']=''
        try:
                if flott_cache_val >= 0:
                        SforCP['FL_OTT']=str(flott_cache_val)
                else: SforCP['OTT']=''
        except NameError:
                print 'FL OTT data not found'
                SforCP['FL_OTT']=''

'''Dump output in QS_cache_count.csv'''
def addToCsv():
        final_out = ''
        CP_count = []
        if tag == 'DAY':
                for i in (['CQI','CEI','VOLTE']):
                        CP_count.append(expectedCount_day[i])
                        CP_count.append(SforCP[i])
                CP_String = ','.join(CP_count)
                final_out = final + ',' + CP_String + '\n'
                dump_out = open(logPath+'output/qsCache/QS_cache_count_DAY.csv','a')
                dump_out.write(final_out)
                dump_out.close()
        elif tag == 'WEEK':
                for i in (['CQI','CEI','VOLTE']):
                        CP_count.append(expectedCount_week[i])
                        CP_count.append(SforCP[i])
                CP_String = ','.join(CP_count)
                final_out = final + ',' + CP_String + '\n'
                dump_out = open(logPath+'output/qsCache/QS_cache_count_WEEK.csv','a')
                dump_out.write(final_out)
                dump_out.close()

def getEnabledCps(typeOfCp):
        xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        parentTopologyTag = xmlparser.getElementsByTagName('QsCacheConfig')
        propertyTag=parentTopologyTag[0].getElementsByTagName('property')
        for propertyelements in propertyTag:
                return [ propertyTag[propertyNum].attributes['enabled'].value for propertyNum in range(len(propertyTag)) if typeOfCp == propertyTag[propertyNum].attributes['Name'].value ][0]

def send_alert():
        global html
        global qsCps
        qsCps = []
        html = '''<html><head><style>
              table, th, td {
              border: 1px solid black;
              border-collapse: collapse;
            }
            </style></head><body>'''
        for key in SforCP:
                if SforCP[key] == '0' and getEnabledCps(key).lower().strip() == 'yes':
                        qsCps.append(key)
                        html += '<h4>QS Cache Count for %s is 0</h4>' %(key)

        html +=  '''<br></body></html>'''
        return html


def getNodeName():
        nodeName_cmd = 'sed -n "/<Distribution/,/<\/Distribution/p" /opt/nsn/ngdb/ifw/config/ifw_ngdb_config.xml|grep -i cluster|awk -F "value=\\"" \'{print $2}\'|awk -F "\\"" \'{print $1}\''
        return commands.getoutput(nodeName_cmd)

def getSmtpIp():
        smtpIpCmd= ('cat %s/common_config.xml | sed -n \'/SMTP/,/SMTP/p\' | grep "IP" | gawk -F"=" \'{print $3}\' | cut -d"/" -f1 | sed -e \'s/"//\' | cut -d\'"\' -f1 ' %commonIfwPath)
        if len(commands.getoutput(smtpIpCmd)) != 0:
                return commands.getoutput(smtpIpCmd)
        else:
                print 'Please configure valid SMTP IP to send Email'
                return ' '

def getRecipientIDs():
        recipientIDCmd= ('cat %s/common_config.xml | grep -i "RecepientEmailIDs" | gawk -F"\\"" \'{print $4}\'' %commonIfwPath)
        recipientID = commands.getoutput(recipientIDCmd)
        receivers = []
        if len(recipientID) != 0:
                for id in recipientID.split(';'):
                        receivers.append(id)
                return receivers
        else:
                print 'Please configure valid recipient list to send Email'
                return ' '

def getSenderIDs():
        senderIDCmd= ('cat %s/common_config.xml | grep -i "SenderEmailID" | gawk -F"\\"" \'{print $4}\'' %commonIfwPath)
        if len(commands.getoutput(senderIDCmd)) != 0:
                return commands.getoutput(senderIDCmd)
        else:
                print 'Please configure valid sender email ID'
                return ' '

def getPostgres():
        postgres = cemod_postgres_sdk_fip_active_host
        if postgres == '':
                exit(0)
        else:
                return postgres


def frameSnmpContentAndSend():
        snmpIp,snmpPort,snmpCommunity = SnmpUtils.getSnmpDetails(SnmpUtils())
        status,sendSnmpTrap = commands.getstatusoutput('/usr/bin/snmptrap -v 2c -c {0} {1}:{2} \'\' SAI-MIB::qsCacheCpLevel SAI-MIB::cpNames s "{3}"'.format(snmpCommunity,snmpIp,snmpPort,qsCps))
        if status is 0:
                print 'CPs with 0 Cache',qsCps
                print 'SNMP Traps sent successfully.'
        else:
                print 'Error in sending SNMP Trap.'


'''Execute the script'''

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")
nodeName =  getNodeName()
smtpIp = getSmtpIp()
postgres = getPostgres()
global today
createOutputCSV()
today = datetime.date.today()
getdt()

currentDay = commands.getoutput('date +%A')
if currentDay == 'Monday':
        getWEEK_fromWS_Scheduled()
        getDAY_fromWS_Scheduled()
else: getDAY_fromWS_Scheduled()

status,endTime = commands.getstatusoutput('date')

#Send Alert
counter = 0
for key in SforCP:
        if SforCP[key] == '0':
                html = send_alert()
                break
        else:
                counter += 1

if counter == len(SforCP.keys()):
        '''Delete maxVal.txt'''
        status2, output2= commands.getstatusoutput('rm -rf maxVal.txt')
        exit(0)


sender = getSenderIDs()
receivers = getRecipientIDs()
message = """From: %s
To: %s
MIME-Version: 1.0
Content-type: text/html
Subject: [CRITICAL_ALERT]:QS Alert on %s"
<b>****AlertDetails****</b>
%s


""" %(sender,receivers,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),html)


if '0' in html:
        smtpObj = smtplib.SMTP(smtpIp)
        smtpObj.sendmail(sender, receivers, message)
        frameSnmpContentAndSend()
        smtpObj.quit()
        print "Successfully sent email"
        exit(2)
else:
        print 'None of the QS CP is equal to 0'
        exit(0)


'''Delete maxVal.txt'''
status2, output2= commands.getstatusoutput('rm -rf maxVal.txt')