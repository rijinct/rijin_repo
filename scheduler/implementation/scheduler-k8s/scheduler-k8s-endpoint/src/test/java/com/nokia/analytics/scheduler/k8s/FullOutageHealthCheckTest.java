
package com.rijin.analytics.scheduler.k8s;

import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.map.HashedMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.boot.actuate.health.Health;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.QueryConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HiveConfigurationProvider.class, ConnectionManager.class,
		Health.class, HealthCheckUtils.class, System.class,
		FullOutageHealthCheck.class })
public class FullOutageHealthCheckTest {

	List boundary = null;

	List status = null;

	DateTimeFormatter formatter = DateTimeFormatter
			.ofPattern("yyyy-MM-dd HH:mm:ss");

	FullOutageHealthCheck check;

	@Before
	public void setUp() throws Exception {
		boundary = new ArrayList();
		status = new ArrayList();
		FullOutageHealthCheck.restart = false;
		mockStatic(HealthCheckUtils.class);
		PowerMockito.mockStatic(System.class);
		PowerMockito.when(System.getenv("HIVE_HDFS_ERROR_DESCRIPTIONS"))
				.thenReturn(
						"Could not open client transport,UndeclaredThrowableException,Connection refused");
		PowerMockito
				.when(System.getenv("FOLDER_MODIFICATION_MINUTES_THRESHOLD"))
				.thenReturn("15");
		PowerMockito
				.when(System
						.getenv("FULL_OUTAGE_HEALTH_CHECK_MINUTES_INTERVAL"))
				.thenReturn("60");
	}

	@Test
	public void intialHealthCheckTest() throws JobExecutionException {
		FullOutageHealthCheck.startTime = null;
		check = new FullOutageHealthCheck();
		check.health();
		Assert.assertNotNull(FullOutageHealthCheck.startTime);
		Assert.assertEquals(false, FullOutageHealthCheck.restart);
	}

	@Test
	public void exceptionHealthCheckTest() throws JobExecutionException {
		PowerMockito
				.when(System
						.getenv("FULL_OUTAGE_HEALTH_CHECK_MINUTES_INTERVAL"))
				.thenReturn(null);
		FullOutageHealthCheck.startTime = LocalDateTime.now().minusMinutes(30);
		check = new FullOutageHealthCheck();
		check.health();
		Assert.assertNotNull(FullOutageHealthCheck.startTime);
		Assert.assertEquals(false, FullOutageHealthCheck.restart);
	}

	@Test
	public void checkBeforeIntervalTest() throws JobExecutionException {
		FullOutageHealthCheck.startTime = LocalDateTime.now().minusMinutes(30);
		check = new FullOutageHealthCheck();
		check.health();
		Assert.assertEquals(false, FullOutageHealthCheck.restart);
	}

	@Test
	public void checkAfterIntervalWithUpdatedBoundaryTest() throws Exception {
		String voice[] = { "Usage_VOICE_1_LoadJob", "2020-08-10 22:50:00" };
		boundary.add(voice);
		PowerMockito
				.when(HealthCheckUtils.executeMultiQuery(
						QueryConstants.USAGE_JOB_BOUNDARY_QUERY))
				.thenReturn(boundary);
		FullOutageHealthCheck.startTime = LocalDateTime.now().minusMinutes(60);
		FullOutageHealthCheck.boundaryMap = new HashedMap();
		FullOutageHealthCheck.boundaryMap.put("Usage_VOICE_1_LoadJob",
				"2020-08-10 21:50:00");
		check = new FullOutageHealthCheck();
		check.health();
		Assert.assertEquals(false, FullOutageHealthCheck.restart);
	}

	@Test
	public void checkAfterIntervalWithNewUpdatedBoundaryTest()
			throws Exception {
		String voice[] = { "Usage_VOICE_1_LoadJob", "2020-08-10 22:50:00" };
		boundary.add(voice);
		PowerMockito
				.when(HealthCheckUtils.executeMultiQuery(
						QueryConstants.USAGE_JOB_BOUNDARY_QUERY))
				.thenReturn(boundary);
		FullOutageHealthCheck.startTime = LocalDateTime.now().minusMinutes(60);
		FullOutageHealthCheck.boundaryMap = new HashedMap();
		FullOutageHealthCheck.boundaryMap.put("Usage_VOICE_1_LoadJob",
				"2020-08-10 21:50:00");
		FullOutageHealthCheck.boundaryMap.put("Usage_SMS_1_LoadJob",
				"2020-08-10 21:50:00");
		check = new FullOutageHealthCheck();
		check.health();
		Assert.assertEquals(false, FullOutageHealthCheck.restart);
	}

	@Test
	public void checkWithNoBoundaryUpdateAndUsageJobRunningAndProcessingTest()
			throws Exception {
		String voice[] = { "Usage_VOICE_1_LoadJob", "2020-08-10 24:50:00" };
		String voiceStatus[] = { "Usage_VOICE_1_LoadJob", "C", "" };
		boundary.add(voice);
		status.add(voiceStatus);
		PowerMockito
				.when(HealthCheckUtils.executeMultiQuery(
						QueryConstants.USAGE_JOB_BOUNDARY_QUERY))
				.thenReturn(boundary);
		PowerMockito
				.when(HealthCheckUtils
						.getFolderModifiedTimestamp(Mockito.any()))
				.thenReturn(LocalDateTime.now().format(formatter));
		PowerMockito
				.when(HealthCheckUtils
						.executeMultiQuery(QueryConstants.USAGE_STATUS_QUERY))
				.thenReturn(status);
		FullOutageHealthCheck.startTime = LocalDateTime.now().minusMinutes(60);
		FullOutageHealthCheck.boundaryMap = new HashedMap();
		FullOutageHealthCheck.boundaryMap.put("Usage_VOICE_1_LoadJob",
				"2020-08-10 24:50:00");
		FullOutageHealthCheck.usageImportDirMap.put("Usage_VOICE_1_LoadJob",
				"/rithomas/us/import/VOICE_1/VOICE_1");
		check = new FullOutageHealthCheck();
		check.health();
		Assert.assertEquals(false, FullOutageHealthCheck.restart);
	}

	@Test
	public void checkWithNoBoundaryUpdateAndUsageJobRunningAndNotProcessingTest()
			throws Exception {
		String voice[] = { "Usage_VOICE_1_LoadJob", "2020-08-10 24:50:00" };
		String voiceStatus[] = { "Usage_VOICE_1_LoadJob", "C", "" };
		boundary.add(voice);
		status.add(voiceStatus);
		PowerMockito
				.when(HealthCheckUtils.executeMultiQuery(
						QueryConstants.USAGE_JOB_BOUNDARY_QUERY))
				.thenReturn(boundary);
		String importDir = "/rithomas/us/import/VOICE_1/VOICE_1";
		PowerMockito
				.when(HealthCheckUtils.getFolderModifiedTimestamp(importDir))
				.thenReturn(LocalDateTime.now().format(formatter));
		String workDir = "/rithomas/us/work/VOICE_1/VOICE_1";
		PowerMockito.when(HealthCheckUtils.getFolderModifiedTimestamp(workDir))
				.thenReturn(
						LocalDateTime.now().minusMinutes(30).format(formatter));
		PowerMockito
				.when(HealthCheckUtils
						.executeMultiQuery(QueryConstants.USAGE_STATUS_QUERY))
				.thenReturn(status);
		FullOutageHealthCheck.startTime = LocalDateTime.now().minusMinutes(60);
		FullOutageHealthCheck.boundaryMap = new HashedMap();
		FullOutageHealthCheck.boundaryMap.put("Usage_VOICE_1_LoadJob",
				"2020-08-10 24:50:00");
		FullOutageHealthCheck.usageImportDirMap.put("Usage_VOICE_1_LoadJob",
				"/rithomas/us/import/VOICE_1/VOICE_1");
		check = new FullOutageHealthCheck();
		check.health();
		Assert.assertEquals(true, FullOutageHealthCheck.restart);
	}

	@Test
	public void checkWithNoBoundaryUpdateAndUsageJobsErrorAndWithHdfsExceptionTest()
			throws Exception {
		String voice[] = { "Usage_VOICE_1_LoadJob", "2020-08-10 24:50:00" };
		String voiceStatus[] = { "Usage_VOICE_1_LoadJob", "E",
				";Could not open client transport with JDBC Uri: jdbc:hive2://40.40.40.108:8071/project;principal=hive/40.40.40.108@RIJIN.COM;retries=3: null" };
		boundary.add(voice);
		status.add(voiceStatus);
		PowerMockito
				.when(HealthCheckUtils.executeMultiQuery(
						QueryConstants.USAGE_JOB_BOUNDARY_QUERY))
				.thenReturn(boundary);
		PowerMockito
				.when(HealthCheckUtils
						.getFolderModifiedTimestamp(Mockito.any()))
				.thenReturn(LocalDateTime.now().format(formatter));
		PowerMockito
				.when(HealthCheckUtils
						.executeMultiQuery(QueryConstants.USAGE_STATUS_QUERY))
				.thenReturn(status);
		FullOutageHealthCheck.startTime = LocalDateTime.now().minusMinutes(60);
		FullOutageHealthCheck.boundaryMap = new HashedMap();
		FullOutageHealthCheck.boundaryMap.put("Usage_VOICE_1_LoadJob",
				"2020-08-10 24:50:00");
		FullOutageHealthCheck.usageImportDirMap.put("Usage_VOICE_1_LoadJob",
				"/rithomas/us/import/VOICE_1/VOICE_1");
		check = new FullOutageHealthCheck();
		check.health();
		Assert.assertEquals(false, FullOutageHealthCheck.restart);
	}

	@Test
	public void checkWithNoBoundaryUpdateAndUsageJobsErrorAndWithoutHdfsExceptionTest()
			throws Exception {
		String voice[] = { "Usage_VOICE_1_LoadJob", "2020-08-10 24:50:00" };
		String voiceStatus[] = { "Usage_VOICE_1_LoadJob", "E",
				"JobWorkflowException" };
		boundary.add(voice);
		status.add(voiceStatus);
		PowerMockito
				.when(HealthCheckUtils.executeMultiQuery(
						QueryConstants.USAGE_JOB_BOUNDARY_QUERY))
				.thenReturn(boundary);
		PowerMockito
				.when(HealthCheckUtils
						.getFolderModifiedTimestamp(Mockito.any()))
				.thenReturn(LocalDateTime.now().format(formatter));
		PowerMockito
				.when(HealthCheckUtils
						.executeMultiQuery(QueryConstants.USAGE_STATUS_QUERY))
				.thenReturn(status);
		FullOutageHealthCheck.startTime = LocalDateTime.now().minusMinutes(60);
		FullOutageHealthCheck.boundaryMap = new HashedMap();
		FullOutageHealthCheck.boundaryMap.put("Usage_VOICE_1_LoadJob",
				"2020-08-10 24:50:00");
		FullOutageHealthCheck.usageImportDirMap.put("Usage_VOICE_1_LoadJob",
				"/rithomas/us/import/VOICE_1/VOICE_1");
		check = new FullOutageHealthCheck();
		check.health();
		Assert.assertEquals(true, FullOutageHealthCheck.restart);
	}
}