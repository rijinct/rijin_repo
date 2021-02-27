
package com.project.rithomas.jobexecution.tnp;

import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Timer;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.ApplicationIdLoggerHiveServer2Util;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.query.RuntimePropertyQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ ThresholdCalculationJob.class, WorkFlowContext.class,
		UpdateBoundary.class, GregorianCalendar.class, ReConnectUtil.class,
		DateFunctionTransformation.class, UpdateJobStatus.class,
		ConnectionManager.class, Connection.class, Statement.class,
		Date.class, Calendar.class, GetDBResource.class, HiveConfigurationProvider.class, DBConnectionManager.class })
public class ThresholdCalculationJobTest {

	private ThresholdCalculationJob mockThresholdCalculationManager;

	@Mock
	private WorkFlowContext mockWorkFlowContext;

	@Mock
	private UpdateBoundary mockUpdateBoundary;

	@Mock
	private GregorianCalendar mockcalendar, mockCalendarBound,
			mockCalendarTemp;

	@Mock
	private UpdateJobStatus mockUpdateJobStatus;

	@Mock
	private DateFunctionTransformation mockDateFunctionTransformation;

	@Mock
	private Connection mockConnection;

	@Mock
	private Statement mockSt;

	@Mock
	private PreparedStatement mockPst;
	@Mock
	private HiveRowCountUtil rowCountUtil;

	@Mock
	private Timer mockConnTimer;

	@Mock
	RuntimePropertyQuery runtimePropertyQuery;
	
	@Mock
	GetDBResource getDBResource;
	
	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;
	
	@Mock
	DBConnectionManager dbConnManager;


	private Long delay;

	private Long mockLB;

	private Long mockUB;

	private Long mockNextLB;

	private Long mockSystemTime;

	private Long mockNextLBBreakLoop;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		mockThresholdCalculationManager = new ThresholdCalculationJob();
		delay = new Long(10000000);
		mockLB = new Long(10000000);
		mockUB = new Long(12000000);
		mockNextLB = new Long(11500000);
		mockSystemTime = new Long(12500000);
		mockNextLBBreakLoop = new Long(12000001);

		
		mockStatic(GetDBResource.class);
		mockStatic(HiveConfigurationProvider.class);

		PowerMockito.whenNew(UpdateBoundary.class).withNoArguments()
				.thenReturn(mockUpdateBoundary);
		// UpdateJobStatus
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(mockUpdateJobStatus);
		PowerMockito.whenNew(GregorianCalendar.class).withNoArguments()
				.thenReturn(mockcalendar).thenReturn(mockCalendarBound)
				.thenReturn(mockCalendarTemp);
		PowerMockito.mockStatic(DateFunctionTransformation.class);
		PowerMockito.whenNew(HiveRowCountUtil.class).withNoArguments()
				.thenReturn(rowCountUtil);
		PowerMockito.whenNew(RuntimePropertyQuery.class).withNoArguments()
				.thenReturn(runtimePropertyQuery);
		PowerMockito.when(
				runtimePropertyQuery.retrieve("RETRY_ALL_HIVE_EXCEPTIONS",
						"true")).thenReturn("true");
		PowerMockito.when(GetDBResource.getInstance())
		.thenReturn(getDBResource);
		Mockito.when(DateFunctionTransformation.getInstance()).thenReturn(
				mockDateFunctionTransformation);
		Mockito.when(
				mockWorkFlowContext.getProperty(JobExecutionContext.JOB_NAME))
				.thenReturn("mockJobId");
		Mockito.when(
				mockWorkFlowContext
						.getProperty(JobExecutionContext.QUERY_TIMEOUT))
				.thenReturn(delay);
		Mockito.when(mockWorkFlowContext
				.getProperty(JobExecutionContext.EXECUTION_ENGINE))
				.thenReturn(schedulerConstants.HIVE_DATABASE);
		Mockito.when(mockWorkFlowContext.getProperty(JobExecutionContext.SQL))
				.thenReturn("mockSQL");
		Mockito.when(mockWorkFlowContext
				.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL))
				.thenReturn(false);
		PowerMockito.mockStatic(ConnectionManager.class);
		mockStatic(DBConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(mockConnection);
		Mockito.when(mockConnection.createStatement()).thenReturn(mockSt);
		Mockito.when(mockConnection.prepareStatement(Mockito.anyString()))
				.thenReturn(mockPst);
		PowerMockito.when(HiveConfigurationProvider.getInstance()).thenReturn(mockHiveConfigurationProvider);
		Mockito.when(mockHiveConfigurationProvider.getQueryHints("mockJobId", schedulerConstants.HIVE_DATABASE)).thenReturn(
				getHiveUdfList());
		Mockito.when(mockWorkFlowContext.getProperty(JobExecutionContext.LB))
				.thenReturn(mockLB);
		Mockito.when(mockWorkFlowContext.getProperty(JobExecutionContext.UB))
				.thenReturn(mockUB);
		Mockito.when(
				mockWorkFlowContext.getProperty(ReConnectUtil.HIVE_RETRY_COUNT))
				.thenReturn(2);
		Mockito.when(
				mockWorkFlowContext
						.getProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL))
				.thenReturn(1000L);
		Mockito.when(
				mockWorkFlowContext.getProperty(JobExecutionContext.NEXT_LB))
				.thenReturn(mockNextLB);
		Mockito.when(mockDateFunctionTransformation.getSystemTime())
				.thenReturn(mockSystemTime);
		PowerMockito.whenNew(Timer.class).withNoArguments()
				.thenReturn(mockConnTimer);
		Mockito.when(
				mockWorkFlowContext.getProperty(JobExecutionContext.TARGET))
				.thenReturn("mockTargetTable");
		Mockito.when(
				mockDateFunctionTransformation
						.getFormattedDate(mockCalendarBound.getTime()))
				.thenReturn("mockSql");
		Mockito.when(mockCalendarTemp.getTimeInMillis()).thenReturn(mockNextLB)
				.thenReturn(mockNextLBBreakLoop);
		Mockito.when(
				mockWorkFlowContext.getProperty(JobExecutionContext.PLEVEL))
				.thenReturn("PLEVEL");
		Mockito.when(
				mockDateFunctionTransformation.getNextBound(mockNextLB,
						"PLEVEL")).thenReturn(mockCalendarTemp);
		Mockito.when(
				mockWorkFlowContext
						.getProperty(JobExecutionContext.APPLICATION_ID_LOGGER))
				.thenReturn(ApplicationIdLoggerHiveServer2Util.getInstance());		
		Mockito.when(getDBResource.getHdfsConfiguration()).thenReturn(
				 new Configuration());
		
	}

	@Test
	public void testExecute() throws Exception {
		mockThresholdCalculationManager.execute(mockWorkFlowContext);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testExecuteWithSQLException() throws Exception {
		Mockito.when(mockConnection.createStatement()).thenThrow(
				new SQLException(""));
		mockThresholdCalculationManager.execute(mockWorkFlowContext);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testExecuteWithInterruptedTrue() throws Exception {
		Mockito.when(
				mockWorkFlowContext
						.getProperty(JobExecutionContext.INTERRUPTED))
				.thenReturn(true);
		mockThresholdCalculationManager.abort(mockWorkFlowContext);
		mockThresholdCalculationManager.execute(mockWorkFlowContext);
	}

	private List<String> getHiveUdfList() {
		List<String> list = new ArrayList<String>();
		list.add("DELETE");
		return list;
	}

	@Test
	public void testGetSqlToExecute() {
	}

	@Test
	public void testAbortWorkFlowContext() {
		Mockito.when(
				mockWorkFlowContext
						.getProperty(JobExecutionContext.INTERRUPTED))
				.thenReturn(false);
		mockThresholdCalculationManager.abort(mockWorkFlowContext);
	}
}
