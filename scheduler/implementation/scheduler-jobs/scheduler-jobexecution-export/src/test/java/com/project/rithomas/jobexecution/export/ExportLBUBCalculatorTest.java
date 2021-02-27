
package com.project.rithomas.jobexecution.export;

import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.DbConfigurator;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.HiveJobRunner;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.LBUBUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { ExportLBUBCalculator.class, GetDBResource.class,
		DBConnectionManager.class, DbConfigurator.class })
public class ExportLBUBCalculatorTest {

	WorkFlowContext context = new JobExecutionContext();

	ExportLBUBCalculator exportLBUBCalculator = null;

	@Mock
	BoundaryQuery boundQuery;

	@Mock
	Boundary boundaryMock;

	@Mock
	LBUBUtil lbubUtilMock;

	Boundary boundary = new Boundary();

	Boundary srcBoundary = new Boundary();

	Boundary usBoundary = new Boundary();

	List<Boundary> boundaryList = new ArrayList<Boundary>();

	List<Boundary> srcBoundaryList = new ArrayList<Boundary>();

	List<Boundary> usBoundaryList = new ArrayList<Boundary>();

	Calendar calendar = new GregorianCalendar();

	Calendar boundCalendar = new GregorianCalendar();

	@Mock

	Statement statement;

	@Mock
	ResultSet result;

	@Mock
	Connection connection;

	@Mock
	Calendar calendarMock;
	@Mock
	DBConnectionManager dbConnManager;
	@Mock
	HiveJobRunner hiveJobRunner;
	@Mock
	DbConfigurator dbConfigurator;

	@Before
	public void setUp() throws Exception {
		exportLBUBCalculator = new ExportLBUBCalculator();
		// setting parameters in context
		context.setProperty(JobExecutionContext.SOURCERETENTIONDAYS, "5");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE, "HIVE");
		List<String> sourceJobIdList = new ArrayList<String>();
		sourceJobIdList.add("Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		// mocking new object call
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(boundQuery);
		PowerMockito.whenNew(Calendar.class).withNoArguments()
				.thenReturn(calendarMock);
		PowerMockito.whenNew(LBUBUtil.class).withNoArguments()
				.thenReturn(lbubUtilMock);
		mockStatic(DBConnectionManager.class);
		//mockStatic(ConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(connection);
		// mock static class
		mockStatic(GetDBResource.class);
		PowerMockito.when(connection.createStatement()).thenReturn(statement);
		PowerMockito.when(statement.executeQuery(Mockito.anyString()))
				.thenReturn(result);
		PowerMockito.when(result.next()).thenReturn(true).thenReturn(true)
				.thenReturn(false);
		PowerMockito.when(result.getFetchSize()).thenReturn(1);
		PowerMockito.when(result.getTimestamp(1)).thenReturn(
				new Timestamp(calendar.getTimeInMillis()));
		context.setProperty(JobExecutionContext.SOURCE_TABLE_NAME, "US_SMS_1");
		context.setProperty(JobExecutionContext.PARTITION_COLUMN, "REPORT_TIME");
		context.setProperty(JobExecutionContext.INTERVAL, "15MIN");
		// setting source Boundary values
		srcBoundary.setSourceJobId("Usage_SMS_1_LoadJob");
		srcBoundary.setSourcePartColumn("REPORT_TIME");
		srcBoundary.setSourceJobType("Loading");
		srcBoundary.setJobId("Exp_SMS_EXPORT_1_RAW_ExportJob");
		srcBoundary.setId(1);
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
	}

	@Test
	public void testExecute() throws WorkFlowExecutionException {
		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
		context.setProperty(JobExecutionContext.MAXVALUE, timeStamp);
		exportLBUBCalculator.execute(context);
	}

	@Test
	public void testExecuteWithMaxValueNull() throws WorkFlowExecutionException {
		context.setProperty(JobExecutionContext.MAXVALUE, null);
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Exp_APBHLR_HOUR_1_AggregateJob");
		context.setProperty("DELAYTIME", "5");
		PowerMockito
				.when(dbConfigurator.shouldConnectToCustomDbUrl(
						Mockito.anyString(), Mockito.anyString()))
				.thenReturn(true);
		exportLBUBCalculator.execute(context);
	}
}
