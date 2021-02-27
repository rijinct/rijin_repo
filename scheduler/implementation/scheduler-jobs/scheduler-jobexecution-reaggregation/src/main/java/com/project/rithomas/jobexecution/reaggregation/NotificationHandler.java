
package com.project.rithomas.jobexecution.reaggregation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.etl.common.ETLConstants;
import com.project.rithomas.etl.notifier.NotificationInitializationException;
import com.project.rithomas.etl.notifier.email.EmailConstants;
import com.project.rithomas.etl.notifier.email.EmailNotificationSender;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class NotificationHandler {

	private static final String COMPLETED_PART_STATUS = "Completed_Part";

	private static final String COMPLETED_STATUS = "Completed";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(NotificationHandler.class);

	public void handleNotifications(WorkFlowContext context,
			ReaggregationListUtil reaggListUtil, String jobId,
			String targetTable, String emailMessage, String sourceJobsInQuotes)
			throws JobExecutionException, NotificationInitializationException {
		StringBuilder emailMessageBuilder = new StringBuilder(emailMessage);
		List<String> reportTimeList = reaggListUtil
				.getReaggregatedReportTimes(context, jobId, sourceJobsInQuotes);
		if (reportTimeList != null && !reportTimeList.isEmpty()) {
			emailMessageBuilder = emailMessageBuilder.append("Job name: ")
					.append(jobId).append("\nTable name: ").append(targetTable)
					.append("\nRe-aggregation completed for: ");
			for (String reportTime : reportTimeList) {
				emailMessageBuilder.append(reportTime).append("\n");
				if (reportTime.contains("(")) {
					reportTime = reportTime.split("\\(")[0];
				}
				context.setProperty(JobExecutionContext.REAGG_LAST_LB,
						reportTime);
			}
			String smtpConfValue = (String) context
					.getProperty(JobExecutionContext.ENABLE_SMTP);
			String isSmtpConfigured = smtpConfValue != null ? smtpConfValue
					: "TRUE";
			LOGGER.info("IS SMTP CONFIGURED :{}", isSmtpConfigured);
			if ("TRUE".equalsIgnoreCase(isSmtpConfigured)) {
				sendNotificationMail(context, emailMessageBuilder.toString());
			}
			reaggListUtil.updateReaggregationList(context, jobId,
					sourceJobsInQuotes, null, COMPLETED_STATUS,
					COMPLETED_PART_STATUS, null);
			String snmpIP = (String) context
					.getProperty(JobExecutionContext.ENABLE_SNMP);
			String isSnmpConfigured = snmpIP != null ? snmpIP : "TRUE";
			LOGGER.info("IS SNMP CONFIGURED :{}", isSnmpConfigured);
			if ("TRUE".equalsIgnoreCase(isSnmpConfigured)) {
				sendSNMPNotification(context, jobId);
			}
			context.setProperty(JobExecutionContext.DESCRIPTION,
					"Re-aggregation completed till " + context
							.getProperty(JobExecutionContext.REAGG_LAST_LB));
		}
	}

	private void sendNotificationMail(WorkFlowContext context,
			String emailMessage) throws NotificationInitializationException {
		LOGGER.info(emailMessage.toString());
		Map<String, Object> notificationData = new HashMap<>();
		String emailHost = (String) context
				.getProperty(JobExecutionContext.EMAIL_HOST);
		String emailPort = context
				.getProperty(JobExecutionContext.EMAIL_PORT) != null
						? (String) context
								.getProperty(JobExecutionContext.EMAIL_PORT)
						: "25";
		String sender = (String) context
				.getProperty(JobExecutionContext.SENDER);
		String recipient = (String) context
				.getProperty(JobExecutionContext.RECEIPIENT);
		if (emailHost != null && sender != null && recipient != null) {
			notificationData.put(EmailConstants.EMAIL_HOST, emailHost);
			notificationData.put(EmailConstants.EMAIL_PORT, emailPort);
			notificationData.put(EmailConstants.SENDER, sender);
			notificationData.put(EmailConstants.RECIPIENT, recipient);
			String subject = (String) context
					.getProperty(JobExecutionContext.REAGG_SUBJECT);
			EmailNotificationSender emailNotificationSender = new EmailNotificationSender();
			try {
				emailNotificationSender.init(notificationData);
				emailNotificationSender.sendEmailNotification(notificationData,
						emailMessage, subject);
			} catch (Exception excp) {
				throw new NotificationInitializationException(
						excp.getMessage());
			}
		} else {
			LOGGER.warn(
					"Email host ({}) or email port ({}) or sender ({}) or recipient ({}) not configured. Hence reaggregation will not be notified.",
					emailHost, emailPort, sender, recipient);
			throw new NotificationInitializationException("Email host ("
					+ emailHost + ") or email port (" + emailPort
					+ ") or sender (" + sender + ") or recipient (" + recipient
					+ ") not configured. Hence reaggregation will not be notified.");
		}
	}

	private void sendSNMPNotification(WorkFlowContext context, String jobId)
			throws NotificationInitializationException {
		String snmpIP = (String) context
				.getProperty(JobExecutionContext.SNMP_IP_ADDRESS);
		String snmpVersion = (String) context
				.getProperty(JobExecutionContext.SNMP_VERSION);
		String snmpCommunityString = (String) context
				.getProperty(JobExecutionContext.SNMP_COMMUNITY_STRING);
		if (snmpIP != null && snmpVersion != null
				&& snmpCommunityString != null) {
			Map<String, Object> notificationData = new HashMap<>();
			notificationData.put(ETLConstants.SNMP_MANAGER_IP_PORT, snmpIP);
			notificationData.put(ETLConstants.SNMP_MANAGER_VERSION,
					snmpVersion);
			notificationData.put(ETLConstants.COMMUNITY_STRING,
					snmpCommunityString);
			notificationData.put(ETLConstants.SOURCE_NAME, jobId);
			String message = "SNMP Trap message: Re-aggregation completed till "
					+ context.getProperty(JobExecutionContext.REAGG_LAST_LB);
			notificationData.put(ETLConstants.DESCRIPTION, message);
			ReaggSnmpNotificationSender reaggSnmpNotificationSender = new ReaggSnmpNotificationSender();
			try {
				reaggSnmpNotificationSender.init(notificationData);
				reaggSnmpNotificationSender.sendSnmpAlert(notificationData);
			} catch (Exception excp) {
				throw new NotificationInitializationException(
						excp.getMessage());
			}
			LOGGER.info("SNMP Trap message: Re-aggregation completed till {}",
					context.getProperty(JobExecutionContext.REAGG_LAST_LB));
		} else {
			LOGGER.warn(
					"SNMP IP address ({}) and/or SNMP verion ({}) and/or SNMP community string ({}) are not configured for notifying reaggregation. ",
					snmpIP, snmpVersion, snmpCommunityString);
			throw new NotificationInitializationException("SNMP IP address ("
					+ snmpIP + ") and/or SNMP verion (" + snmpVersion
					+ ") and/or SNMP community string (" + snmpCommunityString
					+ ") are not configured for notifying reaggregation. ");
		}
	}
}
