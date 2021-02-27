
package com.project.rithomas.jobexecution.common.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@PrepareForTest(value = { MultipleQueryGeneratorBasedOnSubPartition.class,
		ConnectionManager.class, DBConnectionManager.class,
		HiveJobRunner.class })
@RunWith(PowerMockRunner.class)
public class MultipleQueryGeneratorBasedOnSubPartitionTest {

	private static final String SPLIT_STRING = "insert overwrite table";

	@Mock
	QueryExecutor executor;

	@Mock
	Connection con;

	@Mock
	Statement st;

	@Mock
	DBConnectionManager dbConnManager;

	@Test
	public void testGenerateQuery() throws Exception {
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(executor);
		ResultSet result1 = PowerMockito.mock(ResultSet.class);
		ResultSet result2 = PowerMockito.mock(ResultSet.class);
		List<Object[]> resultSet1 = new ArrayList<Object[]>();
		Object[] objectArray1 = { "technology", "es_technology_1" };
		resultSet1.add(objectArray1);
		List<Object[]> resultSet2 = new ArrayList<Object[]>();
		Object[] objectArray2 = { "region", "es_region_1" };
		resultSet2.add(objectArray2);
		WorkFlowContext context = new GeneratorWorkFlowContext();
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE,
				schedulerConstants.HIVE_DATABASE);
		PowerMockito.when(executor.executeMetadatasqlQueryMultiple(
								"select column_name,table_name from rithomas.sub_part_column_info where sub_partition_column='technology'",
								context)).thenReturn(resultSet1);
		PowerMockito.when(executor.executeMetadatasqlQueryMultiple(
								"select column_name,table_name from rithomas.sub_part_column_info where sub_partition_column='region'",
								context)).thenReturn(resultSet2);
		PowerMockito.mockStatic(ConnectionManager.class);
		PowerMockito.mockStatic(DBConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(con);
		PowerMockito.when(con.createStatement()).thenReturn(st);
		PowerMockito
				.when(st.executeQuery("select technology from es_technology_1"))
				.thenReturn(result1);
		PowerMockito.when(st.executeQuery("select region from es_region_1"))
				.thenReturn(result2);
		PowerMockito.when(result1.next()).thenReturn(true).thenReturn(true)
				.thenReturn(false);
		PowerMockito.when(result1.getString(1)).thenReturn("2g")
				.thenReturn("3g");
		PowerMockito.when(result2.next()).thenReturn(true).thenReturn(true)
				.thenReturn(false);
		PowerMockito.when(result2.getString(1)).thenReturn("12")
				.thenReturn("13");
		String query = "from PS_VOICE_SEGG_1_1_DAY "
				+ "$SubPartitionValues:{ obj |"
