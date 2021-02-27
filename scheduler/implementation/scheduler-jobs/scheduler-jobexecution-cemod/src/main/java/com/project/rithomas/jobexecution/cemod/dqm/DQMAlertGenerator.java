
package com.project.rithomas.jobexecution.project.dqm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.etl.common.ETLConstants;
import com.project.rithomas.etl.notifier.NotificationInitializationException;
import com.project.rithomas.etl.notifier.email.EmailConstants;
import com.project.rithomas.etl.notifier.email.EmailNotificationSender;
import com.project.rithomas.etl.properties.runtime.ETLRuntimePropRetriever;
import com.project.rithomas.etl.threshold.ETLThresholdRetriever;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class DQMAlertGenerator extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DQMAlertGenerator.class);

	private static final boolean DEBUGMODE = LOGGER.isDebugEnabled();

	private Map<String, Object> notificationData = new HashMap<String, Object>();;

	private DQMSNMPNotificationSender dqmsnmpNotificationSender;

	private EmailNotificationSender emailNotificationSender;

	private String snmpEnabled;

	private String emailEnabled;

	private boolean duplicateCheckReqd = true;

	private static final String YES = "YES";

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		String dqmScanInterval = (String) context
				.getProperty(JobExecutionContext.DQM_SCAN_INTERVAL);
		Long currentTime = System.currentTimeMillis();
		Long cycleStartTime = getTimeOfLoadCycle("START",currentTime,
				dqmScanInterval);
		Long cycleEndTime = getTimeOfLoadCycle("END",currentTime,
				dqmScanInterval);
		ETLThresholdRetriever etlThresholdRetriever = new ETLThresholdRetriever();
		int rejectedRecThreshold = etlThresholdRetriever
				.retrieveRejectedRecordsThreshold();
		int duplicateRecThreshold = etlThresholdRetriever
				.retrieveDuplicateRecordsThreshold();
		String comparator = etlThresholdRetriever
				.retrieveComparatorForRejectedRec();
		String comparatorForDupRec = etlThresholdRetriever
				.retrieveComparatorForDuplicateRec();
		ETLRuntimePropRetriever etlRuntimePropRetriever = new ETLRuntimePropRetriever();
		snmpEnabled = etlRuntimePropRetriever.retrieveSnmpEnabled();
		emailEnabled = etlRuntimePropRetriever.retrieveEmailEnabled();
		if ("NO".equalsIgnoreCase((String) context
				.getProperty(JobExecutionContext.DUPLICATE_CHECK_REQD))) {
			duplicateCheckReqd = false;
		}
		Map<String, Integer> topoNameDiscRecMap = new HashMap<String, Integer>();
		Connection con = null;
		Statement st = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		try {
			if (snmpEnabled.equalsIgnoreCase(YES)) {
				dqmsnmpNotificationSender = new DQMSNMPNotificationSender();
				retrieveSnmpData();
			}
			if (emailEnabled.equalsIgnoreCase(YES)) {
				emailNotificationSender = new EmailNotificationSender();
				retrieveEmailData();
			}
			if (snmpEnabled.equalsIgnoreCase(YES)
					|| emailEnabled.equalsIgnoreCase(YES)) {
				con = ConnectionManager
						.getConnection(schedulerConstants.HIVE_DATABASE);
				st = con.createStatement();
				LOGGER.debug("cycleStartTime is : {}", cycleStartTime);
				LOGGER.debug("cycleEndTime is : {}", cycleEndTime);
				String sql1 = null;
				sql1 = String.format(
						"select topology_name,sum(disc_recs) from(select distinct topology_name,chunk_file_name,disc_recs from process_monitor where load_time>='%s' and load_time<='%s' and topology_name!='') process_monitor group by topology_name",
						cycleStartTime, cycleEndTime);
				rs1 = st.executeQuery(sql1);
				while (rs1.next()) {
					int discRecCount = rs1.getInt(2);
					topoNameDiscRecMap.put(rs1.getString(1), discRecCount);
				}
				LOGGER.debug("topoNameDiscRecMap:{}", topoNameDiscRecMap);
				rs1.close();
				String sql2 = null;
				sql2 = String.format(
						"select topology_name,sum(bad_recs),sum(processed_recs) from process_monitor where load_time>='%s' and load_time<='%s' and topology_name!='' and dupl_recs is null group by topology_name",
						cycleStartTime, cycleEndTime);
				context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
						sql2);
				rs2 = st.executeQuery(sql2);
				while (rs2.next()) {
					String topologyName = rs2.getString(1);
					int badRecCount = rs2.getInt(2);
					int goodRecCount = rs2.getInt(3);
					LOGGER.debug("Calculating unprocessedRecCount for {}",
							topologyName);
					float unprocessedRecCount = badRecCount
							+ (topoNameDiscRecMap.get(topologyName) != null
									? topoNameDiscRecMap.get(topologyName) : 0);
					float totalRecCount = goodRecCount + unprocessedRecCount;
					float unprocessedRecPercentage = 0;
					if (totalRecCount != 0) {
						unprocessedRecPercentage = (unprocessedRecCount
								/ totalRecCount) * 100;
					}
					LOGGER.debug(
							"Unprocessed Record Percentage: {} for topology_name: {}",
							unprocessedRecPercentage, topologyName);
					if (comparator.equals("=")) {
						if (unprocessedRecPercentage >= rejectedRecThreshold) {
							sendAlarmForThresholdReached(rejectedRecThreshold,
									topologyName, cycleStartTime, cycleEndTime);
						}
					} else if (comparator.equals(">")
							&& (unprocessedRecPercentage > rejectedRecThreshold)) {
						sendAlarmForThresholdReached(rejectedRecThreshold,
								topologyName, cycleStartTime, cycleEndTime);
					}
				}
				rs2.close();
				if (duplicateCheckReqd) {
					sql2 = String.format(
							"select topology_name,sum(dupl_recs),sum(tot_recs) from process_monitor where load_time>='%s' and load_time<='%s' and topology_name!='' and dupl_recs is not null group by topology_name",
							cycleStartTime, cycleEndTime);
					context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
							sql2);
					rs2 = st.executeQuery(sql2);
					while (rs2.next()) {
						String topologyName = rs2.getString(1);
						int duplRecCount = rs2.getInt(2);
						int totalRecCount = rs2.getInt(3);
						double duplicateRecPercentage = 0;
						if (totalRecCount != 0) {
							duplicateRecPercentage = (duplRecCount * 100.0
									/ totalRecCount);
						}
						LOGGER.debug(
								"Duplicate Record Percentage: {} for topology_name: {}",
								duplicateRecPercentage, topologyName);
						if (comparatorForDupRec.equals("=")) {
							if (duplicateRecPercentage >= duplicateRecThreshold) {
								String description = getDescriptionDuplicateViolation(
										duplicateRecThreshold, topologyName,
										cycleStartTime, cycleEndTime);
								sendAlarmForThresholdReached(description,
										topologyName);
							}
						} else if (comparatorForDupRec.equals(">")
								&& (duplicateRecPercentage > duplicateRecThreshold)) {
							String description = getDescriptionDuplicateViolation(
									duplicateRecThreshold, topologyName,
									cycleStartTime, cycleEndTime);
							sendAlarmForThresholdReached(description,
									topologyName);
						}
					}
					rs2.close();
				}
				st.close();
			}
			context.setProperty(JobExecutionContext.DESCRIPTION,
					"DQM SNMP alert job ran successfully");
			success = true;
		} catch (SQLException e) {
			LOGGER.error("SQL Exception {}", e.getMessage());
			updateErrorStatus(context, e);
			throw new WorkFlowExecutionException(
					"SQLException: " + e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOGGER.error("Class not found Exception {}", e.getMessage());
			updateErrorStatus(context, e);
			throw new WorkFlowExecutionException(
					"ClassNotFoundException: " + e.getMessage(), e);
		} catch (NotificationInitializationException e) {
			LOGGER.error("Alert properties are not initialized. {}",
					e.getMessage());
			updateErrorStatus(context, e);
			throw new WorkFlowExecutionException(
					"NotificationInitializationException: " + e.getMessage(),
					e);
		} catch (Exception e) {
			LOGGER.error("Exception while sending trap. {}", e.getMessage());
			updateErrorStatus(context, e);
			throw new WorkFlowExecutionException("Exception: " + e.getMessage(),
					e);
		} finally {
			ConnectionManager.closeResultSet(rs1);
			ConnectionManager.closeResultSet(rs2);
			ConnectionManager.releaseConnection(con,
					schedulerConstants.HIVE_DATABASE);
			ConnectionManager.closeStatement(st);
			if (!success) {
				UpdateJobStatus updateJobStatus = new UpdateJobStatus();
				updateJobStatus.updateFinalJobStatus(context);
			}
		}
		return success;
	}

	private Long getTimeOfLoadCycle(String flag, Long currentTime,
			String dqmScanInterval) {
		int minInterval = getMinInterval(dqmScanInterval);
		Calendar calendar = new GregorianCalendar();
		calendar.setTimeInMillis(currentTime);
		int minute = getLeastRoundedMinute(minInterval, calendar);
		if(flag.equals("START")){
			minute = minute + minInterval;
		}
		calendar.add(Calendar.MINUTE, -minute);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTimeInMillis();
	}
	
	private int getMinInterval(String dqmScanInterval) {
		String[] intervalArray = dqmScanInterval.toUpperCase().split(
				"MIN".toUpperCase());
		return Integer.parseInt(intervalArray[0]);
	}

	private int getLeastRoundedMinute(int minInterval, Calendar calendar) {
		int unroundedMinutes = calendar.get(Calendar.MINUTE);
		return unroundedMinutes % minInterval;
	}

	private void sendAlarmForThresholdReached(int rejectedRecThreshold,
			String dataSourceName, Long cycleStartTime, Long cycleEndTime)
			throws Exception {
		String description = getDescription(rejectedRecThreshold,
				dataSourceName, cycleStartTime, cycleEndTime);
		sendAlarmForThresholdReached(description, dataSourceName);
		if (DEBUGMODE) {
			LOGGER.debug(
					"alarm raised for data source : {} and rejected record threshold is: {}",
					dataSourceName, rejectedRecThreshold);
		}
	}

	private String getDescription(int rejectedRecThreshold,
			String dataSourceName, Long cycleStartTime, Long cycleEndTime) {
		String startTime = JobExecutionContext.formatter.print(cycleStartTime);
		String endTime = JobExecutionContext.formatter.print(cycleEndTime);
		String description = "Rejected records percentage has reached the threshold "
				+ rejectedRecThreshold + "% for data source " + dataSourceName
				+ " in the time interval " + startTime + " to " + endTime;
		return description;
	}

	private String getDescriptionDuplicateViolation(int duplicateRecThreshold,
			String dataSourceName, Long cycleStartTime, Long cycleEndTime) {
		String startTime = JobExecutionContext.formatter.print(cycleStartTime);
		String endTime = JobExecutionContext.formatter.print(cycleEndTime);
		String description = "Duplicate records percentage has reached the threshold "
				+ duplicateRecThreshold + "% for data source " + dataSourceName
				+ " in the time interval " + startTime + " to " + endTime;
		return description;
	}

	private void sendAlarmForThresholdReached(String description,
			String dataSourceName) throws Exception {
		String subject = "Alert raised for rejected/duplicate records threshold violation";
		if (snmpEnabled.equalsIgnoreCase(YES)) {
			notificationData.put(ETLConstants.DESCRIPTION, description);
			notificationData.put(ETLConstants.SOURCE_NAME, dataSourceName);
			dqmsnmpNotificationSender.sendSnmpAlert(notificationData);
		}
		if (emailEnabled.equalsIgnoreCase(YES)) {
			emailNotificationSender.sendEmailNotification(notificationData,
					description, subject);
		}
	}

	private void retrieveEmailData()
			throws NotificationInitializationException {
		ETLRuntimePropRetriever etlRuntimePropRetriever = new ETLRuntimePropRetriever();
		String emailHost = etlRuntimePropRetriever.retrieveEmailHost();
		notificationData.put(EmailConstants.EMAIL_HOST, emailHost);
		String emailPort = etlRuntimePropRetriever.retrieveEmailPort();
		notificationData.put(EmailConstants.EMAIL_PORT, emailPort);
		String sender = etlRuntimePropRetriever.retrieveSender();
		notificationData.put(EmailConstants.SENDER, sender);
		String recipient = etlRuntimePropRetriever.retrieveRecipient();
		notificationData.put(EmailConstants.RECIPIENT, recipient);
		String user = etlRuntimePropRetriever.retrieveUser();
		notificationData.put(EmailConstants.USER, user);
		String password = etlRuntimePropRetriever.retrievePassword();
		notificationData.put(EmailConstants.PASSWORD, password);
		emailNotificationSender.init(notificationData);
	}

	private void retrieveSnmpData() throws NotificationInitializationException {
		ETLRuntimePropRetriever etlRuntimePropRetriever = new ETLRuntimePropRetriever();
		String snmpIPPort = etlRuntimePropRetriever.retrieveSnmpManagerIPPort();
		notificationData.put(ETLConstants.SNMP_MANAGER_IP_PORT, snmpIPPort);
		String snmpVersion = etlRuntimePropRetriever
				.retrieveSnmpManagerVersion();
		notificationData.put(ETLConstants.SNMP_MANAGER_VERSION, snmpVersion);
		String communityString = etlRuntimePropRetriever
				.retrieveSnmpCommunityString();
		notificationData.put(ETLConstants.COMMUNITY_STRING, communityString);
		dqmsnmpNotificationSender.init(notificationData);
	}

	private void updateErrorStatus(WorkFlowContext context, Exception e)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "E");
		context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
				e.getMessage());
		updateJobStatus.updateFinalJobStatus(context);
	}
}
