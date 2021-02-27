package com.project.rithomas.jobexecution.aggregation;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.etl.common.PlevelValues;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.HiveJobRunner;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

@Ignore
@PrepareForTest(value = { SubPartitionUtil.class,
		ConnectionManager.class, DBConnectionManager.class,
		HiveJobRunner.class })
@RunWith(PowerMockRunner.class)
public class SubPartitionUtilTest {

	SubPartitionUtil util = new SubPartitionUtil();
	
	@Mock
	QueryExecutor executor;
	
	WorkFlowContext context;
	
	@Mock
	schedulerJobRunner jobRunner;
	
	@Mock 
	ResultSet rs;

	@Before
	public void setUp() throws Exception {
		context = new GeneratorWorkFlowContext();
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(executor);
	}
	
	@Test
	public void testStoreParitionInfoDisabled()
			throws SQLException, JobExecutionException {
		Long lb = 1535353500000L;
		context.setProperty(JobExecutionContext.TARGET, "PS_SMS_SEGG_1_DAY");
		util.storeParitionInfo(context, null, "Default", lb);
		context.setProperty(JobExecutionContext.LOAD_PARTITION_INFO, "true");
		util.storeParitionInfo(context, null, "Default", lb);
	}
	
	@Test
	public void testStoreParitionInfoEnabled()
			throws Exception {
		Mockito.when(jobRunner.runQuery(
				"select technology,region from PS_SMS_SEGG_1_DAY where dt='1535311800000' and tz='Default' group by technology,region",
				null, context)).thenReturn(rs);
		Mockito.when(rs.next()).thenReturn(true, true, false);
		Mockito.when(rs.getString(1)).thenReturn("2G", "3G");
		Mockito.when(rs.getString(2)).thenReturn("TamilNadu", "Karnataka");
		Long lb = 1535311800000L;
		Long dt = DateFunctionTransformation.getInstance().getTrunc(lb,
				PlevelValues.DAY.name(), null, TimeZoneUtil.getZoneId(context, "Default"));
		context.setProperty(JobExecutionContext.TARGET, "PS_SMS_SEGG_1_DAY");
		context.setProperty(JobExecutionContext.LOAD_PARTITION_INFO, "true");
		context.setProperty(JobExecutionContext.PART_INFO_COLUMNS, "technology,region");
		util.storeParitionInfo(context, jobRunner, "Default", lb);
		Mockito.verify(executor, Mockito.times(1)).executePostgresqlUpdate("delete from sub_partition_values where table_name='PS_SMS_SEGG_1_DAY' and dt=1535311800000 and tz='Default'", context);
		Mockito.verify(executor, Mockito.times(1)).executePostgresqlUpdate(
				context);
	}
}
