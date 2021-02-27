
package com.project.rithomas.jobexecution.entity;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;

import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.ArrayListMultimap;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.etl.common.utils.ETLCommonUtils;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.common.CharacteristicSpecification;
import com.project.rithomas.sdk.model.corelation.EntitySpecification;
import com.project.rithomas.sdk.model.corelation.EntitySpecificationCharacteristicUse;
import com.project.rithomas.sdk.model.meta.GenericDataType;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HiveToPostgresInsert.class, ConnectionManager.class,
		GetDBResource.class, DBConnectionManager.class, ETLCommonUtils.class,
		UpdateJobStatus.class })
public class HiveToPostgresInsertTest {

	HiveToPostgresInsert hiveToPostgresInsert = null;

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	Connection mockHiveConnection;

	@Mock
	Connection mockPostgresConnection;

	@Mock
	Session mockSession;

	@Mock
	Statement mockHiveStatement;

	@Mock
	Statement mockPostgresStatement;

	@Mock
	DBConnectionManager mockConnectionManager;

	@Mock
	ResultSet mockResultSet;

	@Before
	public void setUp() throws Exception {
		hiveToPostgresInsert = new HiveToPostgresInsert();
		mockStatic(ConnectionManager.class);
		mockHiveConnection = Mockito.mock(Connection.class);
		mockHiveStatement = Mockito.mock(Statement.class);
		mockResultSet = Mockito.mock(ResultSet.class);
		PowerMockito
				.when(ConnectionManager
						.getConnection(schedulerConstants.HIVE_DATABASE))
				.thenReturn(mockHiveConnection);
		Mockito.when(mockHiveConnection.createStatement())
				.thenReturn(mockHiveStatement);
		mockStatic(DBConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(mockConnectionManager);
		String postgresDriver = "org.postgresql.Driver";
		String postgresUrl = "jdbc:postgresql://localhost/sai";
		String postgresUserName = "rithomas";
		String postgresPassword = "rithomas";
		mockStatic(GetDBResource.class);
		PowerMockito.when(GetDBResource.getPostgreDriver())
				.thenReturn(postgresDriver);
		PowerMockito.when(GetDBResource.getPostgresUrl())
				.thenReturn(postgresUrl);
		PowerMockito.when(GetDBResource.getPostgreUserName())
				.thenReturn(postgresUserName);
		PowerMockito.when(GetDBResource.getPostgrePassword())
				.thenReturn(postgresPassword);
		Mockito.when(mockConnectionManager.getPostgresConnection(postgresDriver,
				postgresUrl, postgresUserName, postgresPassword))
				.thenReturn(mockPostgresConnection);
		Mockito.when(mockPostgresConnection.createStatement())
				.thenReturn(mockPostgresStatement);
		context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, BigInteger.ONE);
		QueryExecutor mockQueryExecutor = mock(QueryExecutor.class);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(mockQueryExecutor);
	}

	@Test
	public void testExecuteWithPostgresLoadingEnabled() throws Exception {
		context.setProperty(JobExecutionContext.POSTGRES_LOADING_ENABLED,
				"YES");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_DATA_SRC_RGN_INFO_1_CorrelationJob");
		context.setProperty(JobExecutionContext.TARGET,
				"ES_DATA_SRC_RGN_INFO_1");
		context.setProperty(
				JobExecutionContext.ADAP_MAP_IMPORT_DIR_AND_WORK_FILE_PATH_LIST,
				ArrayListMultimap.create());
		EntitySpecification entitySpecification = new EntitySpecification();
		entitySpecification.setSpecId("DATA_SRC_RGN_INFO");
		entitySpecification.setVersion("1");
		mockStatic(ETLCommonUtils.class);
		PowerMockito
				.when(ETLCommonUtils.getEntitySpecFromJobName(
						"Entity_DATA_SRC_RGN_INFO_1_CorrelationJob"))
				.thenReturn(entitySpecification);
		PowerMockito
				.when(mockHiveStatement
						.executeQuery("select * from ES_DATA_SRC_RGN_INFO_1"))
				.thenReturn(mockResultSet);
		CharacteristicSpecification charSpec1 = new CharacteristicSpecification();
		charSpec1.setSpecCharId("TMZ_DATA_SOURCE_ID");
		charSpec1.setName("DATA_SOURCE_ID");
		charSpec1.setValueType(GenericDataType.STRING);
		charSpec1.setValueTypePrecision("30");
		CharacteristicSpecification charSpec2 = new CharacteristicSpecification();
		charSpec2.setSpecCharId("TMZ_REGION_ID");
		charSpec2.setName("REGION_ID");
		charSpec2.setValueType(GenericDataType.STRING);
		charSpec2.setValueTypePrecision("200");
		EntitySpecificationCharacteristicUse entitySpecCharUse1 = new EntitySpecificationCharacteristicUse();
		entitySpecCharUse1.setSpecChar(charSpec1);
		entitySpecCharUse1.setSequenceNumber(1);
		EntitySpecificationCharacteristicUse entitySpecCharUse2 = new EntitySpecificationCharacteristicUse();
		entitySpecCharUse2.setSpecChar(charSpec2);
		entitySpecCharUse2.setSequenceNumber(2);
		Collection<EntitySpecificationCharacteristicUse> entitySpecCharUseList = new ArrayList<EntitySpecificationCharacteristicUse>();
		entitySpecCharUseList.add(entitySpecCharUse1);
		entitySpecCharUseList.add(entitySpecCharUse2);
		entitySpecification
				.setEntitySpecificationCharacteristicUse(entitySpecCharUseList);
		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
		Mockito.when(mockResultSet.getString(1)).thenReturn("60");
		Mockito.when(mockResultSet.getString(2)).thenReturn("RGN1");
		String sql = "select * from ES_DATA_SRC_RGN_INFO_1";
		runMockedHiveQuery(sql);
		sql = "delete from saidata.ES_DATA_SRC_RGN_INFO_1";
		runMockedPostgresQuery(sql);
		sql = "insert into saidata.ES_DATA_SRC_RGN_INFO_1 (DATA_SOURCE_ID,REGION_ID) values ('60','RGN1')";
		runMockedPostgresQuery(sql);
		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
		boolean success = hiveToPostgresInsert.execute(context);
		assertTrue(success);
	}

	private void runMockedHiveQuery(String sql) throws Exception, SQLException {
		PreparedStatement query = Mockito.mock(PreparedStatement.class);
		PowerMockito.when(mockHiveConnection.prepareStatement(sql))
				.thenReturn(query);
		PowerMockito.when(query.executeQuery()).thenReturn(mockResultSet);
		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
	}

	private void runMockedPostgresQuery(String sql)
			throws Exception, SQLException {
		PreparedStatement query = Mockito.mock(PreparedStatement.class);
		PowerMockito.when(mockPostgresConnection.prepareStatement(sql))
				.thenReturn(query);
	}

	@Test
	public void testClassNotFoundException()
			throws ClassNotFoundException, SQLException {
		try {
			Mockito.when(mockConnectionManager.getPostgresConnection(
					Mockito.anyString(), Mockito.anyString(),
					Mockito.anyString(), Mockito.anyString()))
					.thenThrow(new ClassNotFoundException(
							"ClassNotFoundException"));
			hiveToPostgresInsert.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertTrue(e.getMessage().contains("ClassNotFoundException"));
		}
	}
}
