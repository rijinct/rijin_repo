
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.ca4ci.pm.client.PasswordManagementClient;
import com.rijin.ca4ci.pm.helper.DbType;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.model.meta.GeneratorProperty;
import com.project.rithomas.sdk.model.meta.query.GeneratorPropertyQuery;
import com.project.rithomas.sdk.model.utils.SDKEncryptDecryptUtils;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { GetDBResource.class, SDKEncryptDecryptUtils.class, PasswordManagementClient.class})
public class GetDBResourceTest {

	GeneratorPropertyQuery mockGeneratorPropertyQuery = mock(GeneratorPropertyQuery.class);

	private static String postgresUrlKey = "POSTGRES_URL_TEMPLATE";

	private static String postgresDriverKey = "POSTGRES_DRIVER";

	GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();
	
	@Mock
	PasswordManagementClient mockPasswordManagementClient;

	@Before
	public void setUp() throws Exception {
		PowerMockito.whenNew(GeneratorPropertyQuery.class).withNoArguments()
				.thenReturn(mockGeneratorPropertyQuery);
		PowerMockito.when(
				mockGeneratorPropertyQuery.retrieve(Mockito.anyString()))
				.thenReturn(null);

		String filePath = this.getClass().getClassLoader()
				.getResource("persistence-config.properties").getPath();
		if (filePath != null && filePath.contains("%20")) {
			filePath = filePath.replace("%20", " ");
		}
		FileInputStream fis = new FileInputStream(filePath);
		String clientPropFile = this.getClass().getClassLoader()
				.getResource("client.properties").getPath();
		if (clientPropFile != null && clientPropFile.contains("%20")) {
			clientPropFile = clientPropFile.replace("%20", " ");
		}
		FileInputStream clientPropFis = new FileInputStream(new File(
				clientPropFile));
		PowerMockito
				.whenNew(FileInputStream.class)
				.withArguments(
						SDKSystemEnvironment.getSDKApplicationConfDirectory()
								+ "persistence-config.properties")
				.thenReturn(fis);
		PowerMockito
				.whenNew(FileInputStream.class)
				.withArguments(
						SDKSystemEnvironment.getschedulerConfDirectory()
								+ "client.properties")
				.thenReturn(clientPropFis);
		PowerMockito.mockStatic(SDKEncryptDecryptUtils.class);
		Mockito.when(
				SDKEncryptDecryptUtils
						.decryptPassword("dxy6b/yiOcsaCLHQ1kNesQ=="))
				.thenReturn("saiws");
		
		PowerMockito.whenNew(PasswordManagementClient.class).withNoArguments().thenReturn(mockPasswordManagementClient);
		Mockito.when(mockPasswordManagementClient.readPassword(DbType.POSTGRES, "sai", "rithomas")).thenReturn("abc");
	}

	// @Test
	public void testGetInstance() {
		GeneratorProperty generatorPropertyPostgresUrl = new GeneratorProperty();
		generatorPropertyPostgresUrl
				.setParamvalue("jdbc:postgresql://localhost/sai");
		PowerMockito.when(mockGeneratorPropertyQuery.retrieve(postgresUrlKey))
				.thenReturn(generatorPropertyPostgresUrl);
		GeneratorProperty generatorPropertyPostgresDriver = new GeneratorProperty();
		generatorPropertyPostgresDriver.setParamvalue("org.postgresql.Driver");
		PowerMockito.when(
				mockGeneratorPropertyQuery.retrieve(postgresDriverKey))
				.thenReturn(generatorPropertyPostgresDriver);
		assertNotNull(GetDBResource.getInstance());
	}

	// @Test
	public void testRetrievePostgresObjectProperties() throws JobExecutionException {
		GetDBResource.getInstance().retrievePostgresObjectProperties(context);
		assertEquals(
				(String) context.getProperty(JobExecutionContext.POSTGRESURL),
				"jdbc:postgresql://10.58.127.74/sai");
		assertEquals(
				(String) context
						.getProperty(JobExecutionContext.POSTGRESDRIVER),
				"org.postgresql.Driver");
		assertEquals(
				(String) context.getProperty(JobExecutionContext.USERNAME),
				"rithomas");
		assertEquals(
				(String) context.getProperty(JobExecutionContext.PASSWORD),
				"rithomas");
	}

	// @Test
	public void testRetrievePostgresObjectPropertiesIOException()
			throws Exception {
		PowerMockito
				.whenNew(FileInputStream.class)
				.withArguments(
						SDKSystemEnvironment.getSDKApplicationConfDirectory()
								+ "persistence-config.properties")
				.thenThrow(new IOException());
		GetDBResource.getInstance().retrievePropertiesFromConfig();
	}

	// @Test
	// public void testRetrieveHiveObjectProperties() {
	//
	// context.setProperty(JobExecutionContext.HIVE_PORT, "50030");
	// context.setProperty(JobExecutionContext.HIVE_HOST, "10.58.127.74");
	// context.setProperty(JobExecutionContext.HIVE_DBNAME, "saidata");
	//
	// GetDBResource.getInstance().retrieveHiveObjectProperties(context);
	//
	// assertEquals((String) context.getProperty(JobExecutionContext.HIVEURL),
	// "jdbc:hive2://10.58.127.74:50030/saidata");
	// assertEquals(
	// (String) context.getProperty(JobExecutionContext.HIVEDRIVER),
	// "org.apache.hadoop.hive.jdbc.HiveDriver");
	// }
	// @Test
	public void testGetHdfsConfiguration() {
		assertNotNull(GetDBResource.getInstance().getHdfsConfiguration());
	}

	@Test
	public void testSetGetPostgresUrl() {
		GetDBResource.setPostgresUrl("jdbc:postgresql://localhost/saitest");
		assertEquals("jdbc:postgresql://localhost/saitest",
				GetDBResource.getPostgresUrl());
	}
		
	// @Test
	public void testGetPostgresDriver() {
		assertEquals("org.postgresql.Driver", GetDBResource.getPostgreDriver());
	}


	@Test
	public void testGetPostgreUserName() {
		assertEquals("rithomas", GetDBResource.getPostgreUserName());
	}

	@Test
	public void testGetPostgrePassword() throws JobExecutionException {
		assertEquals("abc", GetDBResource.getPostgrePassword());
	}

	@Test
	public void testGetPostgreCachePassword() {
		assertEquals("", GetDBResource.getPostgresCachePassword());
	}

	@Test
	public void testGetPostgreCacheUserName() {
		assertEquals("saiws", GetDBResource.getPostgresCacheUser());
	}
}