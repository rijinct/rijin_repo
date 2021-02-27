
package com.project.rithomas.jobexecution.tnp;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.common.ApplicabilityCode;
import com.project.rithomas.sdk.model.meta.query.RuntimePropertyQuery;
import com.project.rithomas.sdk.model.performance.PerfIndicatorMeaDuring;
import com.project.rithomas.sdk.model.performance.PerformanceApplicability;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ModelResources.class, KPICalculationJob.class,
		ConnectionManager.class, ReConnectUtil.class, HiveConfigurationProvider.class,
		DBConnectionManager.class })
public class KPICalculationJobTest {

	KPICalculationJob job = new KPICalculationJob();

	JobExecutionContext context = new JobExecutionContext();

	@Mock
	EntityManager entityManager;

	@Mock
	TypedQuery<PerformanceIndicatorSpec> typedQuery;

	@Mock
	TypedQuery<PerformanceIndicatorSpec> typedQueryDevice;

	@Mock
	TypedQuery<PerformanceIndicatorSpec> typedQuerySubscriber;

	@Mock
	Connection connection;

	@Mock
	Statement statement;

	@Mock
	PreparedStatement pstmt;
	@Mock
	DBConnectionManager dbConnManager;
	@Mock
	UpdateBoundary updateBoundary;

	@Mock
	UpdateJobStatus updateJobStatus;

	@Mock
	private HiveRowCountUtil rowCountUtil;

	@Mock
	RuntimePropertyQuery runtimePropertyQuery;
	
	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;

	@Before
	public void setup() throws Exception {
		context.setProperty(ReConnectUtil.HIVE_RETRY_COUNT, 2);
		context.setProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL, 1000L);
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE, "HIVE");
		mockStatic(ModelResources.class);
		mockStatic(ConnectionManager.class);
		mockStatic(HiveConfigurationProvider.class);
		
		when(ModelResources.getEntityManager()).thenReturn(entityManager);
		when(
				entityManager
						.createNamedQuery(
								PerformanceIndicatorSpec.FIND_BY_PERF_SPEC_INTERVAL_GROUPBY,
								PerformanceIndicatorSpec.class)).thenReturn(
				typedQuery);
		when(
				typedQuery.setParameter(PerformanceIndicatorSpec.PERF_SPEC_ID,
						"TEST")).thenReturn(typedQuery);
		when(
				typedQuery.setParameter(
						PerformanceIndicatorSpec.PERF_SPEC_VERSION, "1"))
				.thenReturn(typedQuery);
		when(
				typedQuery.setParameter(PerformanceIndicatorSpec.INTERVAL_ID,
						"15MIN")).thenReturn(typedQuery);
		when(
				typedQuery.setParameter(PerformanceIndicatorSpec.GROUPING_EXPR,
						"CELL_SAC,LAC")).thenReturn(typedQuery);
		when(
				typedQuery.setParameter(PerformanceIndicatorSpec.GROUPING_EXPR,
						"DEVICE_TYPE")).thenReturn(typedQueryDevice);
		when(
				typedQueryDevice.setParameter(
						PerformanceIndicatorSpec.PERF_SPEC_ID, "TEST"))
				.thenReturn(typedQueryDevice);
		when(
				typedQueryDevice.setParameter(
						PerformanceIndicatorSpec.PERF_SPEC_VERSION, "1"))
				.thenReturn(typedQueryDevice);
		when(
				typedQueryDevice.setParameter(
						PerformanceIndicatorSpec.INTERVAL_ID, "15MIN"))
				.thenReturn(typedQueryDevice);
		when(
				typedQuery.setParameter(PerformanceIndicatorSpec.GROUPING_EXPR,
						"SUBCRIBER_GROUP")).thenReturn(typedQuerySubscriber);
		when(
				typedQuerySubscriber.setParameter(
						PerformanceIndicatorSpec.PERF_SPEC_ID, "TEST"))
				.thenReturn(typedQuerySubscriber);
		when(
				typedQuerySubscriber.setParameter(
						PerformanceIndicatorSpec.PERF_SPEC_VERSION, "1"))
				.thenReturn(typedQuerySubscriber);
		when(
				typedQuerySubscriber.setParameter(
						PerformanceIndicatorSpec.INTERVAL_ID, "15MIN"))
				.thenReturn(typedQuerySubscriber);
		List<PerformanceIndicatorSpec> kpiList1 = getKPIList();
		when(typedQuery.getResultList()).thenReturn(kpiList1);
		when(typedQuerySubscriber.getResultList())
				.thenReturn(getKPIListInvalidAppl());
		mockStatic(DBConnectionManager.class);
		mockStatic(ConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(connection);
		when(connection.createStatement()).thenReturn(statement);
		when(connection.prepareStatement(Mockito.anyString()))
				.thenReturn(pstmt);
		whenNew(UpdateBoundary.class).withNoArguments()
				.thenReturn(updateBoundary);
		whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		whenNew(HiveRowCountUtil.class).withNoArguments()
				.thenReturn(rowCountUtil);
		whenNew(RuntimePropertyQuery.class).withNoArguments()
				.thenReturn(runtimePropertyQuery);
		when(runtimePropertyQuery.retrieve("RETRY_ALL_HIVE_EXCEPTIONS", "true"))
				.thenReturn("true");
		List<String> hints = new ArrayList<String>();
		hints.add("CREATE temporary function test as 'com.project.rithomas.udf.Test'");
		hints.add("set mapred.reduce.tasks=4");
		PowerMockito.when(HiveConfigurationProvider.getInstance()).thenReturn(mockHiveConfigurationProvider);
		when(mockHiveConfigurationProvider.getQueryHints("PS_TNP_TEST_1_15MIN_10_KPIJob", schedulerConstants.HIVE_DATABASE))
				.thenReturn(hints);
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
	}

	private List<PerformanceIndicatorSpec> getKPIList() {
		List<PerformanceIndicatorSpec> kpis = new ArrayList<PerformanceIndicatorSpec>();
		kpis.add(getKPI("CALL_DROP_VOICE", "CALL_DROP", "00:00", "23:59", true));
		kpis.add(getKPI("NUMBER_OF_CALLS_VOICE", "NUMBER_OF_CALLS", "00:00",
				"23:59", true));
		return kpis;
	}

	private List<PerformanceIndicatorSpec> getKPIListInvalidAppl() {
		List<PerformanceIndicatorSpec> kpis = new ArrayList<PerformanceIndicatorSpec>();
		PerformanceIndicatorSpec kpi = getKPI("NUMBER_OF_CALLS_VOICE",
				"NUMBER_OF_CALLS", "00:00", "23:59", false);
		kpis.add(kpi);
		return kpis;
	}

	private PerformanceIndicatorSpec getKPI(String id, String name,
			String startTime, String endTime, boolean isValidApplicability) {
		PerformanceIndicatorSpec kpi1 = new PerformanceIndicatorSpec();
		kpi1.setIndicatorspecId(id);
		kpi1.setPiSpecName(name);
		List<PerfIndicatorMeaDuring> kpiMeasDuringList = new ArrayList<PerfIndicatorMeaDuring>();
		PerfIndicatorMeaDuring perfIndicatorMeaDuring = new PerfIndicatorMeaDuring();
		PerformanceApplicability perfApplicability = getPerformanceApplicability(
				startTime, endTime, isValidApplicability);
		perfIndicatorMeaDuring.setPerfapplicabilityid(perfApplicability);
		kpiMeasDuringList.add(perfIndicatorMeaDuring);
		kpi1.setPerfIndicatorsMeaDuring(kpiMeasDuringList);
		return kpi1;
	}

	private PerformanceApplicability getPerformanceApplicability(
			String startTime, String endTime, boolean isValidApplicability) {
		PerformanceApplicability perfApplicability = new PerformanceApplicability();
		perfApplicability.setApplicabilityCode(ApplicabilityCode.APPLICABLE
				.name());
		if (isValidApplicability) {
			perfApplicability.setDays("2-6");
		} else {
			perfApplicability.setDays("Invalid days");
		}
		perfApplicability.setStartTime(startTime);
		perfApplicability.setEndTime(endTime);
		perfApplicability.setPerfapplicabilityid("TEST");
		return perfApplicability;
	}

	@Test
	public void testExecute() {
		try {
			setContext();
			context.setProperty(JobExecutionContext.JOB_DESCRIPTION,
					"CELL_SAC,LAC");
			job.execute(context);
		} catch (WorkFlowExecutionException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecuteDeviceTypeKPIs() {
		try {
			setContext();
			context.setProperty(JobExecutionContext.JOB_DESCRIPTION,
					"DEVICE_TYPE");
			job.execute(context);
		} catch (WorkFlowExecutionException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecuteInvalidApplicability() {
		try {
			setContext();
			context.setProperty(JobExecutionContext.JOB_DESCRIPTION,
					"SUBCRIBER_GROUP");
			job.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertTrue(e
					.getMessage()
					.contains(
							"Invalid applicability exception while calculating KPI: Invalid day 'Invalid days'"));
		}
	}

	@Test
	public void testExecuteSQLException() throws SQLException {
		try {
			setContext();
			when(connection.createStatement()).thenThrow(
					new SQLException("Test excpetion"));
			context.setProperty(JobExecutionContext.JOB_DESCRIPTION,
					"CELL_SAC,LAC");
			job.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertTrue(e.getMessage().contains("Test excpetion"));
		}
	}

	@Test
	public void testExecuteKPIsTimeZone() {
		try {
			setContextForTimeZone();
			context.setProperty(JobExecutionContext.JOB_DESCRIPTION,
					"CELL_SAC,LAC");
			job.execute(context);
		} catch (WorkFlowExecutionException e) {
			e.printStackTrace();
		}
	}

	private void setContextForTimeZone() {
		setContext();
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "Yes");
		List<String> timeZoneRegions = new ArrayList<String>();
		timeZoneRegions.add("Region1");
		timeZoneRegions.add("Region2");
		context.setProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS,
				timeZoneRegions);
		Map<String, Long> regionUbMap = new HashMap<String, Long>();
		regionUbMap.put("Region1", 1424055600000L);
		regionUbMap.put("Region2", 1424059200000L);
		context.setProperty(JobExecutionContext.MAX_REPORT_TIME_FOR_REGION,
				regionUbMap);
		Map<String, Long> regionLbMap = new HashMap<String, Long>();
		regionLbMap.put("Region1", 1424053800000L);
		regionLbMap.put("Region2", 1424057400000L);
		context.setProperty(JobExecutionContext.LEAST_REPORT_TIME_FOR_REGION,
				regionLbMap);
		Map<String, Long> regionOffsetMap = new HashMap<String, Long>();
		regionOffsetMap.put("Region1", 0L);
		regionOffsetMap.put("Region2", 3600000L);
		context.setProperty(JobExecutionContext.REGION_TIMEZONE_OFFSET_MAP,
				regionOffsetMap);
	}

	private void setContext() {
		context.setProperty(JobExecutionContext.JOB_NAME,
				"PS_TNP_TEST_1_15MIN_10_KPIJob");
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "No");
		context.setProperty(JobExecutionContext.LB,
				Long.valueOf("1424053800000"));
		context.setProperty(JobExecutionContext.UB,
				Long.valueOf("1424055600000"));
		context.setProperty(JobExecutionContext.NEXT_LB,
				Long.valueOf("1424054700000"));
		context.setProperty(JobExecutionContext.PLEVEL, "15MIN");
		context.setProperty(
				JobExecutionContext.SQL,
