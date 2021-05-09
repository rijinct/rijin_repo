#! /usr/bin/python
#############################################################################
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
import smtplib
from xml.dom import minidom

class EmailUtils:

        def getEmailDetails(self):
                if cei_enabled != "yes":
                    commonXml = "/opt/nsn/ngdb/ifw/etc/common/common_config.xml"
                else: commonXml = "/opt/nsn/ngdb/monitoring/conf/ceimonitoring.xml"
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
                                        receipientList = receivers.split(";")

                return smtpIp,sender,receipientList

        def frameEmailAndSend(self, emailSubject , emailBody, cei=None):
                global cei_enabled
                cei_enabled = cei
                smtpIp,sender,receivers = EmailUtils.getEmailDetails(EmailUtils())
                message = """From: %s
To: %s
MIME-Version: 1.0
Content-type: text/html
Subject: %s

<p style="font-size: large; font-style: bold">****AlertDetails****</p>
%s

""" %(sender,receivers,emailSubject,emailBody)                
                try:
                        smtpObj = smtplib.SMTP(smtpIp)
                        smtpObj.sendmail(sender, receivers, message)
                        smtpObj.quit()
                except smtplib.SMTPException:
                        print 'Error: Unable to send email alert'
