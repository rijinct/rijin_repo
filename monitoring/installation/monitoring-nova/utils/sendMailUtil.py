#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This class parses the XML for SMTP details and sends email
#
# Date: 23-01-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality
#
# 2.
#############################################################################
import smtplib,os
from xml.dom import minidom
from logger_util import *
from send_alarm import SnmpAlarm

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

class EmailUtils:

    def getEmailDetails(self):
        return (open("/opt/nsn/ngdb/monitoring/extEndPointsConfigMap/CEMOD_POP3_SERVER_IP",'r').read(),open("/opt/nsn/ngdb/monitoring/extEndPointsConfigMap/CEMOD_SMTP_SENDER_EMAILID",'r').read(),open("/opt/nsn/ngdb/monitoring/extEndPointsConfigMap/CEMOD_SMTP_RECEIVER_EMAILIDS",'r').read())


    def getEmailDetailsCei(self):
        commonXml = "/opt/nsn/ngdb/monitoring/conf/monitoring.xml"
        xmlparser = minidom.parse(commonXml)
        smtpTag = xmlparser.getElementsByTagName('SMTP')
        propertySmtpTag = smtpTag[0].getElementsByTagName('property')
        for property in propertySmtpTag:
                for propertynum in range(len(propertySmtpTag)):
                        if "IP" == propertySmtpTag[propertynum].attributes['name'].value:
                                smtpIp = propertySmtpTag[propertynum].attributes['value'].value
                        if "SenderEmailID" == propertySmtpTag[propertynum].attributes['name'].value:
                                sender = propertySmtpTag[propertynum].attributes['value'].value
                        if "RecepientEmailIDs" == propertySmtpTag[propertynum].attributes['name'].value:
                                receivers = propertySmtpTag[propertynum].attributes['value'].value

        return smtpIp,sender,receivers

    def frameEmailAndSend(self, emailSubject , emailBody , alert_key, cei=None):
        if SnmpAlarm.get_enable_prop(alert_key)['email']:
            if cei:
                smtpDetails = self.getEmailDetailsCei()
            else:
                smtpDetails = self.getEmailDetails()
                message = """From: %s
To: %s
MIME-Version: 1.0
Content-type: text/html
Subject: %s

<p style="font-size: large; font-style: bold">****AlertDetails****</p>
%s

""" %(smtpDetails[1],','.join(smtpDetails[2].split(";")),emailSubject,emailBody)
            try:
                smtpObj = smtplib.SMTP(smtpDetails[0])
                smtpObj.sendmail(smtpDetails[1], smtpDetails[2].split(";"), message)
                smtpObj.quit()
            except smtplib.SMTPException:
                logger.exception('Error: Unable to send email alert')
        else:
            logger.info("Email Alert is disabled for {0}".format(alert_key))

