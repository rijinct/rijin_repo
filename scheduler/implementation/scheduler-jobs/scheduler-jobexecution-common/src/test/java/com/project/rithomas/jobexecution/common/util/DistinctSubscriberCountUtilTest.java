
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.util.UsageSpecificationGeneratorUtil;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { UsageSpecificationGeneratorUtil.class,
		HiveTableQueryUtil.class, ConnectionManager.class })
public class DistinctSubscriberCountUtilTest {

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	Connection mockConnection;

	@Mock
	PreparedStatement mockStatement;

	@Mock
	ResultSet mockResultSet;

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetSourceTableNames() throws Exception {
		mockStatic(ConnectionManager.class);
		PowerMockito.when(
				ConnectionManager
						.getConnection(schedulerConstants.HIVE_DATABASE))
				.thenReturn(mockConnection);
		Mockito.when(mockConnection.createStatement())
				.thenReturn(mockStatement);
		Mockito.when(mockStatement.executeQuery(Mockito.anyString()))
				.thenReturn(mockResultSet);
		// .thenThrow(new SQLException("SQL Exception."));
		Mockito.when(mockResultSet.next()).thenReturn(true, false);
		Mockito.when(mockResultSet.getString(1)).thenReturn("us_sms_1");
		List<String> tables = new ArrayList<String>();
		tables.add("us_sms_1");
		assertEquals(tables,
				DistinctSubscriberCountUtil.getSourceTableNames(context));
	}

	@Test
	public void testGetSourceJobIds() {
		List<String> sourceTables = new ArrayList<String>();
		sourceTables.add("us_sms_1");
		sourceTables.add("us_gtp_1");
		List<String> smsIDVersion = new ArrayList<String>();
		smsIDVersion.add("SMS");
		smsIDVersion.add("1");
		List<String> gtpIDVersion = new ArrayList<String>();
		gtpIDVersion.add("GTP");
		gtpIDVersion.add("1");
		mockStatic(HiveTableQueryUtil.class);
		PowerMockito.when(
				HiveTableQueryUtil.getSpecIDVerFromTabName("us_sms_1"))
				.thenReturn(smsIDVersion);
		PowerMockito.when(
				HiveTableQueryUtil.getSpecIDVerFromTabName("us_gtp_1"))
				.thenReturn(gtpIDVersion);
		mockStatic(UsageSpecificationGeneratorUtil.class);
		PowerMockito
				.when(UsageSpecificationGeneratorUtil.getUsageSpecJobName(
						"SMS", "1")).thenReturn("Usage_SMS_1_LoadJob");
		PowerMockito
				.when(UsageSpecificationGeneratorUtil.getUsageSpecJobName(
						"GTP", "1")).thenReturn("Usage_GTP_1_LoadJob");
		List<String> jobNames = new ArrayList<String>();
		jobNames.add("Usage_SMS_1_LoadJob");
		jobNames.add("Usage_GTP_1_LoadJob");
		assertEquals(jobNames,
				DistinctSubscriberCountUtil.getSourceJobIds(sourceTables));
	}
}
