
package com.project.rithomas.jobexecution.reaggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.etl.notifier.email.EmailNotificationSender;
import com.project.rithomas.jobexecution.aggregation.AggregationUtil;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.AutoTriggerJobs;
import com.project.rithomas.jobexecution.common.DQICalculator;
import com.project.rithomas.jobexecution.common.GetJobMetadata;
import com.project.rithomas.jobexecution.common.JobTriggerPrecheck;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.util.CommonAggregationUtil;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.PartitionUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.RuntimeProperty;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.model.meta.query.RuntimePropertyQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DriverManager.class, DBConnectionManager.class,
		AggregationUtil.class, ReaggregationManager.class,
		ReaggregationListUtil.class, ConnectionManager.class,
		RetrieveDimensionValues.class, HiveConfigurationProvider.class,
		ApplicationLoggerFactory.class, ReaggregationSourceConfiguration.class,
		ReaggregationQueryBuilder.class, PartitionUtil.class,
		HiveTableQueryUtil.class, CommonAggregationUtil.class,
		UpdateJobStatus.class, UpdateBoundary.class, QueryExecutor.class,
		BoundaryQuery.class, TimeZoneUtil.class, HiveReaggregation.class,
		ReaggCommonUtil.class, ReaggregationHandler.class,
		DependentJobUtil.class,NotificationHandler.class })
public class ReaggregationManagerTest {

	ReaggregationManager reaggManager = new ReaggregationManager();

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	UpdateJobStatus updateJobStatus;

	@Mock
	UpdateBoundary updateBoundary;

	@Mock
	QueryExecutor queryExec;

	@Mock
	BoundaryQuery boundaryQuery;

	@Mock
	RuntimePropertyQuery runtimePropQuery;

	@Mock
	DBConnectionManager dbConnManager;

	@Mock
	ResultSet result;

	@Mock
	Connection connection;

	@Mock
	PreparedStatement statement;

	@Mock
	Session session;

	@Mock
	EmailNotificationSender emailNotifSender;

	@Mock
	ReaggSnmpNotificationSender reaggSNMPNotifSender;

	@Mock
	DQICalculator dqiCalculator;

	@Mock
	ReaggregationListUtil reaggListUtil;

	@Mock
	GetJobMetadata getJobMetadata;

	@Mock
	AggregationUtil aggregationUtil;

	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;

	@Mock
	HiveRowCountUtil rowCountUtil;

	@Mock
	ApplicationLoggerUtilInterface applicationLogger;

	@Mock
	JobTriggerPrecheck precheck;

	@Mock
	AutoTriggerJobs autoTrigger;

	@Mock
	ReaggregationSourceConfiguration mockReaggregationSourceConfiguration;

	@Mock
	ReaggregationQueryBuilder mockReaggregationQueryBuilder;

	@Mock
	ArchivedPartitionHelper helper;

	ArrayList<String> hiveSetting = new ArrayList<String>();
	{
		hiveSetting.add(
				"CREATE TEMPORARY FUNCTION uf_trunc AS 'com.project.rithomas.hive.udf.TruncUDF'");
	}

	@Before
	public void setUp() throws Exception {
		System.out.println(JobExecutionContext.SOURCE_CHECK_ALL);
//		Whitebox.setInternalState(reaggManager, "aggregationUtil",
//				aggregationUtil);
		// setting new object
		mockStatic(HiveConfigurationProvider.class);
		mockStatic(CommonAggregationUtil.class);
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		Mockito.when(mockHiveConfigurationProvider.getQueryHints(
				"Perf_SMS_1_WEEK_AggregateJob",
				schedulerConstants.HIVE_DATABASE)).thenReturn(hiveSetting);
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(boundaryQuery);
		List<String> sourceJobs = new ArrayList<String>();
		sourceJobs.add("Perf_SMS_1_15MIN_ReaggregateJob");
		Mockito.when(
				boundaryQuery.getSourceJobIds("Perf_SMS_1_HOUR_ReaggregateJob"))
				.thenReturn(sourceJobs);
		Mockito.when(
				boundaryQuery.getSourceJobIds("Perf_SMS_1_DAY_ReaggregateJob"))
				.thenReturn(sourceJobs);
		PowerMockito.whenNew(RuntimePropertyQuery.class).withNoArguments()
				.thenReturn(runtimePropQuery);
		// mock static class
		mockStatic(DBConnectionManager.class);
		mockStatic(DriverManager.class);
		// mockStatic(RetrieveDimensionValues.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		// setting the context
		context.setProperty(JobExecutionContext.LB,
				Long.parseLong("1354307400000"));
		context.setProperty(JobExecutionContext.UB,
				Long.parseLong("1354321800000"));
		context.setProperty(JobExecutionContext.NEXT_LB,
				Long.parseLong("1354311000000"));
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE, "HIVE");
		context.setProperty(ReConnectUtil.HIVE_RETRY_COUNT, 2);
		context.setProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL, 1000L);
		context.setProperty(JobExecutionContext.PERF_JOB_NAME,
				"Perf_SMS_1_DAY_AggregateJob");
		context.setProperty("Perf_SMS_1_DAY_AggregateJob_RETENTIONDAYS", "100");
		context.setProperty("Perf_SMS_1_DAY_AggregateJob_PLEVEL", "DAY");
		PowerMockito.when(dbConnManager.getHiveConnection(Mockito.anyString(),
				Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
				.thenReturn(connection);
		mockStatic(ConnectionManager.class);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(connection);
		PowerMockito
				.when(ConnectionManager
						.getConnection(schedulerConstants.HIVE_DATABASE))
				.thenReturn(connection);
		PowerMockito.when(connection.createStatement()).thenReturn(statement);
		PowerMockito.when(connection.prepareStatement(Mockito.anyString()))
				.thenReturn(statement);
		String query1 = "SELECT requestor_jobname, provider_jobname, alevel, report_time, status, region_id FROM rithomas.reagg_list r "
				+ " WHERE  provider_jobname = 'Perf_SMS_1_HOUR_ReaggregateJob' AND requestor_jobname in ('Perf_SMS_1_15MIN_ReaggregateJob') "
				+ " AND status NOT IN ('Completed', 'Completed_Part') ORDER  BY region_id, report_time";
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(query1, context))
				.thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_HOUR_ReaggregateJob");
		context.setProperty(JobExecutionContext.LONG_CALLS_THRESHOLD_VALUE,
				"val");
		context.setProperty(JobExecutionContext.AVERAGE_MOS_THRESHOLD, "val");
		Object[] resultObj = new Object[] { "2017" };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						QueryConstants.GET_MIN_LOAD_TIME.replace(
								QueryConstants.PROVIDER_JOB,
								context.getProperty(
										JobExecutionContext.JOB_NAME)
										.toString()),
						context))
				.thenReturn(resultObj);
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
		PowerMockito.whenNew(ReaggregationSourceConfiguration.class)
				.withNoArguments()
				.thenReturn(mockReaggregationSourceConfiguration);
		PowerMockito.whenNew(ReaggregationQueryBuilder.class).withNoArguments()
				.thenReturn(mockReaggregationQueryBuilder);
		PowerMockito.whenNew(ArchivedPartitionHelper.class)
				.withArguments(context).thenReturn(helper);
		PowerMockito.mockStatic(PartitionUtil.class);
		PowerMockito.mockStatic(HiveTableQueryUtil.class);
	}

	public static void settingUp(QueryExecutor queryExec,
			UpdateBoundary updateBoundary, UpdateJobStatus updateJobStatus,
			ReaggregationListUtil reaggListUtil, GetJobMetadata getJobMetadata,
			DQICalculator dqiCalculator, HiveRowCountUtil rowCountUtil)
			throws Exception {
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		PowerMockito.whenNew(UpdateBoundary.class).withNoArguments()
				.thenReturn(updateBoundary);
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		PowerMockito.whenNew(ReaggregationListUtil.class).withNoArguments()
				.thenReturn(reaggListUtil);
		PowerMockito.whenNew(GetJobMetadata.class).withNoArguments()
				.thenReturn(getJobMetadata);
		PowerMockito.whenNew(DQICalculator.class).withNoArguments()
				.thenReturn(dqiCalculator);
		PowerMockito.whenNew(HiveRowCountUtil.class).withNoArguments()
				.thenReturn(rowCountUtil);
	}

	@Test
	public void testExecute() throws Exception {
		settingUp(queryExec, updateBoundary, updateJobStatus, reaggListUtil,
				getJobMetadata, dqiCalculator, rowCountUtil);
		assertTrue(reaggManager.execute(context));
	}

	@Test
	public void testExecuteWithAlerts() throws Exception {
		settingUp(queryExec, updateBoundary, updateJobStatus, reaggListUtil,
				getJobMetadata, dqiCalculator, rowCountUtil);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_DAY_ReaggregateJob");
		context.setProperty(JobExecutionContext.TARGET, "PS_SMS_1_DAY");
		context.setProperty(JobExecutionContext.EMAIL_HOST, "host");
		context.setProperty(JobExecutionContext.EMAIL_PORT, "port");
		context.setProperty(JobExecutionContext.SENDER, "sender@project.com");
		context.setProperty(JobExecutionContext.RECEIPIENT,
				"recipient@project.com");
		context.setProperty(JobExecutionContext.SNMP_IP_ADDRESS, "snmp_ip");
		context.setProperty(JobExecutionContext.SNMP_VERSION, "2c");
		context.setProperty(JobExecutionContext.SNMP_COMMUNITY_STRING,
				"public");
		PowerMockito.whenNew(EmailNotificationSender.class).withNoArguments()
				.thenReturn(emailNotifSender);
		PowerMockito.whenNew(ReaggSnmpNotificationSender.class)
				.withNoArguments().thenReturn(reaggSNMPNotifSender);
		String query1 = "SELECT requestor_jobname, provider_jobname, alevel, report_time, status, region_id FROM rithomas.reagg_list r "
				+ " WHERE  provider_jobname = 'Perf_SMS_1_DAY_ReaggregateJob' AND requestor_jobname in ('Perf_SMS_1_15MIN_ReaggregateJob') "
				+ " AND status NOT IN ('Completed', 'Completed_Part') ORDER  BY region_id, report_time";
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(query1, context))
				.thenReturn(null);
		List<String> reportTimeList = new ArrayList<String>();
		reportTimeList.add("2013-08-01 00:00:00");
		reportTimeList.add("2013-08-01 04:00:00");
		Mockito.when(reaggListUtil.getReaggregatedReportTimes(context,
				"Perf_SMS_1_DAY_ReaggregateJob",
				"'Perf_SMS_1_15MIN_ReaggregateJob'"))
				.thenReturn(reportTimeList);
		Object[] resultObj = new Object[] { "2017" };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						QueryConstants.GET_MIN_LOAD_TIME.replace(
								QueryConstants.PROVIDER_JOB,
								context.getProperty(
										JobExecutionContext.JOB_NAME)
										.toString()),
						context))
				.thenReturn(resultObj);
		assertTrue(reaggManager.execute(context));
		String description = (String) context
				.getProperty(JobExecutionContext.DESCRIPTION);
		assertEquals("Re-aggregation completed till 2013-08-01 04:00:00",
				description);
	}

	@Test
	public void testExecuteNoSourcesFoundException() throws Exception {
		settingUp(queryExec, updateBoundary, updateJobStatus, reaggListUtil,
				getJobMetadata, dqiCalculator, rowCountUtil);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_2_DAY_ReaggregateJob");
		context.setProperty(JobExecutionContext.TARGET, "PS_SMS_2_DAY");
		try {
			reaggManager.execute(context);
		} catch (WorkFlowExecutionException ex) {
			assertTrue(ex.getMessage().contains(
					"No source jobs found for the given re-aggregation job!"));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testExecuteAggregationNotDoneException() throws Exception {
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		PowerMockito.whenNew(UpdateBoundary.class).withNoArguments()
				.thenReturn(updateBoundary);
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		PowerMockito.whenNew(ReaggregationListUtil.class).withNoArguments()
				.thenReturn(reaggListUtil);
		PowerMockito.whenNew(GetJobMetadata.class).withNoArguments()
				.thenReturn(getJobMetadata);
		PowerMockito.whenNew(DQICalculator.class).withNoArguments()
				.thenReturn(dqiCalculator);
		PowerMockito.whenNew(HiveRowCountUtil.class).withNoArguments()
				.thenReturn(rowCountUtil);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_DAY_ReaggregateJob");
		context.setProperty(JobExecutionContext.TARGET, "PS_SMS_1_DAY");
		context.setProperty(JobExecutionContext.EMAIL_HOST, "host");
		context.setProperty(JobExecutionContext.EMAIL_PORT, "port");
		context.setProperty(JobExecutionContext.SENDER, "sender@project.com");
		context.setProperty(JobExecutionContext.RECEIPIENT,
				"recipient@project.com");
		context.setProperty(JobExecutionContext.SNMP_VERSION, "2c");
		context.setProperty(JobExecutionContext.SNMP_COMMUNITY_STRING,
				"public");
		PowerMockito.whenNew(EmailNotificationSender.class).withNoArguments()
				.thenReturn(emailNotifSender);
		PowerMockito.whenNew(ReaggSnmpNotificationSender.class)
				.withNoArguments().thenReturn(reaggSNMPNotifSender);
		RuntimeProperty runtimeProp = new RuntimeProperty();
		runtimeProp.setParamname(JobExecutionContext.WEEK_START_DAY);
		runtimeProp.setParamvalue("1");
		Mockito.when(
				runtimePropQuery.retrieve(JobExecutionContext.WEEK_START_DAY))
				.thenReturn(runtimeProp);
		Object[] row1 = new Object[] { "Perf_SMS_1_DAY_ReaggregateJob",
				"Perf_SMS_1_WEEK_ReaggregateJob", "WEEK", "2013-08-05 00:00:00",
				"Initialized", "" };
		Object[] row2 = new Object[] { "Perf_SMS_1_DAY_ReaggregateJob",
				"Perf_SMS_1_WEEK_ReaggregateJob", "WEEK", "2013-08-12 00:00:00",
				"Initialized", null };
		List resultList = new ArrayList();
		resultList.add(row1);
		resultList.add(row2);
		String query1 = "SELECT requestor_jobname, provider_jobname, alevel, report_time, status, region_id FROM rithomas.reagg_list r "
				+ " WHERE  provider_jobname = 'Perf_SMS_1_DAY_ReaggregateJob' AND requestor_jobname in ('Perf_SMS_1_15MIN_ReaggregateJob') "
				+ " AND status NOT IN ('Completed', 'Completed_Part') ORDER  BY region_id, report_time";
		Object[] resultObj = new Object[] { "2017" };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						QueryConstants.GET_MIN_LOAD_TIME.replace(
								QueryConstants.PROVIDER_JOB,
								context.getProperty(
										JobExecutionContext.JOB_NAME)
										.toString()),
						context))
				.thenReturn(resultObj);
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(query1, context))
				.thenReturn(resultList);
		try {
			reaggManager.execute(context);
		} catch (WorkFlowExecutionException ex) {
			assertTrue(ex.getMessage().contains(
					"Normal aggregation is not yet done for the interval: "));
		}
	}

	@Test
	public void testExecuteSnmpAlertException() throws Exception {
		settingUp(queryExec, updateBoundary, updateJobStatus, reaggListUtil,
				getJobMetadata, dqiCalculator, rowCountUtil);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_DAY_ReaggregateJob");
		context.setProperty(JobExecutionContext.TARGET, "PS_SMS_1_DAY");
		context.setProperty(JobExecutionContext.EMAIL_HOST, "host");
		context.setProperty(JobExecutionContext.EMAIL_PORT, "port");
		context.setProperty(JobExecutionContext.SENDER, "sender@project.com");
		context.setProperty(JobExecutionContext.RECEIPIENT,
				"recipient@project.com");
		context.setProperty(JobExecutionContext.SNMP_VERSION, "2c");
		context.setProperty(JobExecutionContext.SNMP_COMMUNITY_STRING,
				"public");
		PowerMockito.whenNew(EmailNotificationSender.class).withNoArguments()
				.thenReturn(emailNotifSender);
		PowerMockito.whenNew(ReaggSnmpNotificationSender.class)
				.withNoArguments().thenReturn(reaggSNMPNotifSender);
		RuntimeProperty runtimeProp = new RuntimeProperty();
		runtimeProp.setParamname(JobExecutionContext.WEEK_START_DAY);
		runtimeProp.setParamvalue("1");
		Mockito.when(
				runtimePropQuery.retrieve(JobExecutionContext.WEEK_START_DAY))
				.thenReturn(runtimeProp);
		String query1 = "SELECT requestor_jobname, provider_jobname, alevel, report_time, status, region_id FROM rithomas.reagg_list r "
				+ " WHERE  provider_jobname = 'Perf_SMS_1_DAY_ReaggregateJob' AND requestor_jobname in ('Perf_SMS_1_15MIN_ReaggregateJob') "
				+ " AND status NOT IN ('Completed', 'Completed_Part') ORDER  BY region_id, report_time";
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(query1, context))
				.thenReturn(null);
		List<String> reportTimeList = new ArrayList<String>();
		reportTimeList.add("2013-08-01 00:00:00");
		reportTimeList.add("2013-08-01 04:00:00");
		Mockito.when(reaggListUtil.getReaggregatedReportTimes(context,
				"Perf_SMS_1_DAY_ReaggregateJob",
				"'Perf_SMS_1_15MIN_ReaggregateJob'"))
				.thenReturn(reportTimeList);
		Object[] resultObj = new Object[] { "2017" };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						QueryConstants.GET_MIN_LOAD_TIME.replace(
								QueryConstants.PROVIDER_JOB,
								context.getProperty(
										JobExecutionContext.JOB_NAME)
										.toString()),
						context))
				.thenReturn(resultObj);
		try {
			reaggManager.execute(context);
		} catch (WorkFlowExecutionException ex) {
			assertTrue(ex.getMessage().contains(
					"SNMP IP address (null) and/or SNMP verion (2c) and/or SNMP community string (public) are not configured for notifying reaggregation. "));
		}
	}

	@Test
	public void testExecuteEmailAlertException() throws Exception {
		settingUp(queryExec, updateBoundary, updateJobStatus, reaggListUtil,
				getJobMetadata, dqiCalculator, rowCountUtil);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_DAY_ReaggregateJob");
		context.setProperty(JobExecutionContext.TARGET, "PS_SMS_1_DAY");
		context.setProperty(JobExecutionContext.EMAIL_HOST, "host");
		context.setProperty(JobExecutionContext.EMAIL_PORT, "port");
		context.setProperty(JobExecutionContext.SNMP_IP_ADDRESS, "snmp_ip");
		context.setProperty(JobExecutionContext.SNMP_VERSION, "2c");
		context.setProperty(JobExecutionContext.SNMP_COMMUNITY_STRING,
				"public");
		PowerMockito.whenNew(EmailNotificationSender.class).withNoArguments()
				.thenReturn(emailNotifSender);
		PowerMockito.whenNew(ReaggSnmpNotificationSender.class)
				.withNoArguments().thenReturn(reaggSNMPNotifSender);
		String query1 = "SELECT requestor_jobname, provider_jobname, alevel, report_time, status, region_id FROM rithomas.reagg_list r "
				+ " WHERE  provider_jobname = 'Perf_SMS_1_DAY_ReaggregateJob' AND requestor_jobname in ('Perf_SMS_1_15MIN_ReaggregateJob') "
				+ " AND status NOT IN ('Completed', 'Completed_Part') ORDER  BY region_id, report_time";
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(query1, context))
				.thenReturn(null);
		List<String> reportTimeList = new ArrayList<String>();
		reportTimeList.add("2013-08-01 00:00:00");
		reportTimeList.add("2013-08-01 04:00:00");
		Mockito.when(reaggListUtil.getReaggregatedReportTimes(context,
				"Perf_SMS_1_DAY_ReaggregateJob",
				"'Perf_SMS_1_15MIN_ReaggregateJob'"))
				.thenReturn(reportTimeList);
		Object[] resultObj = new Object[] { "2017" };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						QueryConstants.GET_MIN_LOAD_TIME.replace(
								QueryConstants.PROVIDER_JOB,
								context.getProperty(
										JobExecutionContext.JOB_NAME)
										.toString()),
						context))
				.thenReturn(resultObj);
		try {
			reaggManager.execute(context);
		} catch (WorkFlowExecutionException ex) {
			assertTrue(ex.getMessage().contains(
					"Email host (host) or email port (port) or sender (null) or recipient (null) not configured."));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testExecuteWithJobExecution() throws Exception {
		settingUp(queryExec, updateBoundary, updateJobStatus, reaggListUtil,
				getJobMetadata, dqiCalculator, rowCountUtil);
		setVaues(context, helper, boundaryQuery, queryExec,
				mockReaggregationQueryBuilder,
				mockReaggregationSourceConfiguration, aggregationUtil, precheck,
				autoTrigger, rowCountUtil, applicationLogger);
		assertTrue(reaggManager.execute(context));
		String lastLb = (String) context
				.getProperty(JobExecutionContext.REAGG_LAST_LB);
		assertEquals("2013-08-12 00:00:00", lastLb);
		Long archivingDateInMill = (Long) context
				.getProperty(JobExecutionContext.ARCHIVING_DATE);
		Long archivingDate = 1557945000000L;
		assertEquals(archivingDate, archivingDateInMill);
		PowerMockito.verifyStatic(VerificationModeFactory.times(1));
	}

	public static void setVaues(WorkFlowContext context,
			ArchivedPartitionHelper helper, BoundaryQuery boundaryQuery,
			QueryExecutor queryExec,
			ReaggregationQueryBuilder mockReaggregationQueryBuilder,
			ReaggregationSourceConfiguration mockReaggregationSourceConfiguration,
			AggregationUtil aggregationUtil, JobTriggerPrecheck precheck,
			AutoTriggerJobs autoTrigger, HiveRowCountUtil rowCountUtil,
			ApplicationLoggerUtilInterface applicationLogger) throws Exception {
		String regionId = "";
		String tableName = "PS_SMS_1_WEEK";
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_WEEK_ReaggregateJob");
		context.setProperty(JobExecutionContext.PERF_JOB_NAME,
				"Perf_SMS_1_WEEK_AggregateJob");
		context.setProperty("Perf_SMS_1_WEEK_AggregateJob_RETENTIONDAYS",
				"100");
		context.setProperty("Perf_SMS_1_WEEK_AggregateJob_PLEVEL", "WEEK");
		context.setProperty(JobExecutionContext.TARGET, tableName);
		context.setProperty(JobExecutionContext.SQL, "dummy");
		context.setProperty(JobExecutionContext.MAX_IMSI_APN_EXPORT, 0L);
		context.setProperty(JobExecutionContext.CUTOFF_VOLUME_2G, "50");
		context.setProperty(JobExecutionContext.CUTOFF_VOLUME_3G, "200");
		context.setProperty(JobExecutionContext.CUTOFF_VOLUME_4G, "500");
		context.setProperty(JobExecutionContext.TYPE_AVAILABLE_HO,
				"'IuPs','3G'");
		context.setProperty(JobExecutionContext.TYPE_AVAILABLE_SGSN,
				"'IuPs','2G_3G','2G','2G_3G','4G','3G','Gb','4G'");
		List<String> sourceJobs = new ArrayList<String>();
		sourceJobs.add("Perf_SMS_1_DAY_ReaggregateJob");
		Mockito.when(
				boundaryQuery.getSourceJobIds("Perf_SMS_1_WEEK_ReaggregateJob"))
				.thenReturn(sourceJobs);
		Object[] row1 = new Object[] { "Perf_SMS_1_DAY_ReaggregateJob",
				"Perf_SMS_1_WEEK_ReaggregateJob", "WEEK", "2013-08-05 00:00:00",
				"Initialized", regionId };
		Object[] row2 = new Object[] { "Perf_SMS_1_DAY_ReaggregateJob",
				"Perf_SMS_1_WEEK_ReaggregateJob", "WEEK", "2013-08-12 00:00:00",
				"Initialized", null };
		List<Object[]> resultSet = new ArrayList<Object[]>();
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQueryMultiple(
								QueryConstants.DYNAMIC_PARAM_QUERY.replace(
										JobExecutionContext.ENTITY_JOB_NAME,
										"Perf_SMS_1_WEEK_ReaggregateJob"),
								context))
				.thenReturn(resultSet);
		List resultList = new ArrayList();
		resultList.add(row1);
		resultList.add(row2);
		String query1 = "select string_agg(requestor_jobname,','), reagg.provider_jobname, reagg.alevel, reagg.report_time, reagg.status, "
				+ "reagg.region_id from (SELECT requestor_jobname, provider_jobname, alevel, report_time, status, region_id FROM rithomas.reagg_list "
				+ "WHERE  provider_jobname = 'Perf_SMS_1_WEEK_ReaggregateJob' AND status NOT IN ('Completed', 'Completed_Part', 'Pending')) reagg "
				+ "left outer join (select report_time, region_id from reagg_list where provider_jobname='Perf_SMS_1_WEEK_ReaggregateJob' and "
				+ "status ='Pending') pending_list on reagg.report_time=pending_list.report_time and nvl(reagg.region_id,'-1')=nvl(pending_list.region_id,'-1') "
				+ "where pending_list.report_time is null group by reagg.provider_jobname, reagg.alevel, reagg.report_time, reagg.status, reagg.region_id "
				+ "ORDER  BY reagg.region_id, reagg.report_time desc";
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(query1, context))
				.thenReturn(resultList).thenReturn(null);
		row1 = new Object[] { "Perf_SMS_1_MONTH_ReaggregateJob", "MONTH",
				"2013-08-01 00:00:00" };
		resultList = new ArrayList();
		resultList.add(row1);
		String dependentJobQuery = "SELECT jd.jobid AS provider_jobid, jp.paramvalue AS alevel, (SELECT bi.maxvalue FROM rithomas.job_prop j, rithomas.boundary bi WHERE "
				+ " j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME' AND  j.paramvalue = bi.jobid AND  bi.maxvalue IS NOT NULL) as maxvalue, "
				+ "(SELECT j.paramvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd "
				+ " WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME') as perf_job_name "
				+ " FROM rithomas.job_dictionary jd, rithomas.job_prop jp, rithomas.boundary b WHERE "
				+ " b.sourcejobid = 'Perf_SMS_1_WEEK_ReaggregateJob' AND b.jobid = jd.jobid AND  jd.id = jp.jobid AND  jp.paramname = 'ALEVEL' AND"
				+ " jd.typeid=(select id from rithomas.job_type_dictionary where jobtypeid='Reaggregation') UNION SELECT b.jobid as provider_jobid, jp.paramvalue as alevel, "
				+ "(SELECT bi.maxvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd, rithomas.boundary bi"
				+ " WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME' AND j.paramvalue = bi.jobid AND bi.maxvalue IS NOT NULL) as maxvalue,"
				+ "(SELECT j.paramvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd "
				+ " WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME') as perf_job_name "
				+ " FROM rithomas.job_dict_dependency c, rithomas.job_list j, rithomas.boundary b, rithomas.job_prop jp, rithomas.job_dictionary jd "
				+ " WHERE j.jobid = 'Perf_SMS_1_WEEK_ReaggregateJob' AND c.source_jobid = j.id AND"
				+ " b.id = c.bound_jobid AND b.jobid = jd.jobid AND jp.jobid = jd.id  AND jp.paramname = 'ALEVEL' AND"
				+ " jd.typeid=(select id from rithomas.job_type_dictionary where jobtypeid='Reaggregation')";
		Mockito.when(queryExec
				.executeMetadatasqlQueryMultiple(dependentJobQuery, context))
				.thenReturn(resultList);
		row1 = new Object[] { "Perf_SMS_1_WEEK_ReaggregateJob",
				"Perf_SMS_1_MONTH_ReaggregateJob", "2013-08-01 00:00:00" };
		resultList = new ArrayList();
		resultList.add(row1);
		String distinctListQuery = "SELECT DISTINCT requestor_jobname, provider_jobname, "
				+ "case when 'MONTH'='HOUR' then date_trunc('hour',report_time) when 'MONTH'='DAY' then date_trunc('day', report_time)"
				+ " when 'MONTH'='WEEK' then date_trunc('week', report_time)  when 'MONTH'='MONTH' then date_trunc('month', report_time) else report_time end AS report_time"
				+ " FROM rithomas.reagg_list WHERE requestor_jobname in ('Perf_SMS_1_DAY_ReaggregateJob') AND provider_jobname = 'Perf_SMS_1_WEEK_ReaggregateJob' AND status <> 'Completed'";
		Mockito.when(queryExec
				.executeMetadatasqlQueryMultiple(distinctListQuery, context))
				.thenReturn(resultList);
		Object[] maxValueList = new Object[] { "2013-08-05 00:00:00" };
		String maxValueQuery1 = "select maxvalue from rithomas.boundary where jobid='Perf_SMS_1_WEEK_AggregateJob' and maxvalue>='2013-08-05 00:00:00'";
		String maxValueQuery2 = "select maxvalue from rithomas.boundary where jobid='Perf_SMS_1_WEEK_AggregateJob' and maxvalue>='2013-08-12 00:00:00'";
		Mockito.when(queryExec.executeMetadatasqlQuery(maxValueQuery1, context))
				.thenReturn(maxValueList);
		Mockito.when(queryExec.executeMetadatasqlQuery(maxValueQuery2, context))
				.thenReturn(maxValueList);
		PowerMockito
				.when(aggregationUtil
						.replaceKPIFormula(Mockito.any(WorkFlowContext.class)))
				.thenReturn(
						(String) context.getProperty(JobExecutionContext.SQL));
		PowerMockito
				.when(CommonAggregationUtil.replaceKPIFormula(
						Mockito.anyString(), Mockito.anyString(),
						Mockito.any(WorkFlowContext.class)))
				.thenReturn(
						(String) context.getProperty(JobExecutionContext.SQL));
		Object[] resultObj = new Object[] { "2017" };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						QueryConstants.GET_MIN_LOAD_TIME.replace(
								QueryConstants.PROVIDER_JOB,
								context.getProperty(
										JobExecutionContext.JOB_NAME)
										.toString()),
						context))
				.thenReturn(resultObj);
		PowerMockito.mockStatic(ApplicationLoggerFactory.class);
		PowerMockito.when(ApplicationLoggerFactory.getApplicationLogger())
				.thenReturn(applicationLogger);
		PowerMockito.when(rowCountUtil.getNumberOfRecordsInserted(context,
				regionId, applicationLogger)).thenReturn((long) 5);
		PowerMockito.whenNew(AutoTriggerJobs.class).withNoArguments()
				.thenReturn(autoTrigger);
		PowerMockito.when(autoTrigger.execute(context)).thenReturn(true);
		PowerMockito.whenNew(JobTriggerPrecheck.class).withNoArguments()
				.thenReturn(precheck);
		PowerMockito.when(precheck.execute(context)).thenReturn(true);
		context.setProperty(JobExecutionContext.DQM_ENABLED, "YES");
		Mockito.when(mockReaggregationSourceConfiguration.setContext(context))
				.thenReturn(mockReaggregationSourceConfiguration);
		Mockito.when(mockReaggregationSourceConfiguration
				.setLowerBound(Mockito.anyLong()))
				.thenReturn(mockReaggregationSourceConfiguration);
		Mockito.when(mockReaggregationSourceConfiguration
				.setNextLb(Mockito.anyLong()))
				.thenReturn(mockReaggregationSourceConfiguration);
		Mockito.when(
				mockReaggregationSourceConfiguration.setSourceJobs(sourceJobs))
				.thenReturn(mockReaggregationSourceConfiguration);
		Mockito.doNothing().when(mockReaggregationQueryBuilder)
				.setConfiguration(mockReaggregationSourceConfiguration);
		Mockito.when(mockReaggregationQueryBuilder.setSql("dummy"))
				.thenReturn(mockReaggregationQueryBuilder);
		Mockito.when(mockReaggregationQueryBuilder.update())
				.thenReturn("dummy");
		Long archivingDate = 1557945000000L;
		Long partitionValue = 1375641000000L;
		Mockito.when(
				helper.getArchivingDaysForJob("Perf_SMS_1_WEEK_ReaggregateJob"))
				.thenReturn(15);
		Mockito.when(helper.getArchivingDate(15)).thenReturn(archivingDate);
		Mockito.when(helper.isPartitionArchived(partitionValue, archivingDate))
				.thenReturn(true);
		PartitionUtil.dropPartitionsBasedOnCondition(context, tableName,
				partitionValue, "=");
	}

	@Test
	public void testFlagInContext() throws Exception {
		settingUp(queryExec, updateBoundary, updateJobStatus, reaggListUtil,
				getJobMetadata, dqiCalculator, rowCountUtil);
		setVaues(context, helper, boundaryQuery, queryExec,
				mockReaggregationQueryBuilder,
				mockReaggregationSourceConfiguration, aggregationUtil, precheck,
				autoTrigger, rowCountUtil, applicationLogger);
		assertTrue(reaggManager.execute(context));
		assertEquals("false", context.getProperty(JobExecutionContext.FLAG));
	}

	@Test
	public void testTimeZoneFalse() throws Exception {
		settingUp(queryExec, updateBoundary, updateJobStatus, reaggListUtil,
				getJobMetadata, dqiCalculator, rowCountUtil);
		setVaues(context, helper, boundaryQuery, queryExec,
				mockReaggregationQueryBuilder,
				mockReaggregationSourceConfiguration, aggregationUtil, precheck,
				autoTrigger, rowCountUtil, applicationLogger);
		mockStatic(TimeZoneUtil.class);
		PowerMockito.when(TimeZoneUtil.isTimeZoneEnabled(context))
				.thenReturn(false);
		assertTrue(reaggManager.execute(context));
		assertEquals("false", context.getProperty(JobExecutionContext.FLAG));
	}
}
