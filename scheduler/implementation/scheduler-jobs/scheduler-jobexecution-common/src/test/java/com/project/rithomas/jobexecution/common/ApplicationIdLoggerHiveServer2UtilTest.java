
package com.project.rithomas.jobexecution.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hive.jdbc.HiveStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { ApplicationIdLoggerHiveServer2Util.class, DBConnectionManager.class, Connection.class,
		PreparedStatement.class, ApplicationLoggerUtilInterface.class,
		HiveStatement.class })
public class ApplicationIdLoggerHiveServer2UtilTest {


	@Mock
	DBConnectionManager dbConnManager;

	@Mock
	Connection connection;

	@Mock
	HiveStatement statement;

	@Before
	public void setup() throws Exception {
		List<String> logStatements = new ArrayList<String>();
		logStatements
				.add("INFO  : Submitting tokens for job: job_1485145997849_0013");
		logStatements
				.add("INFO  : Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:projectcluster, Ident: (token for hive: HDFS_DELEGATION_TOKEN owner=hive/114.114.114.24@RIJIN.COM, renewer=yarn,; realUser=, issueDate=1485153006227, maxDate=1485757806227, sequenceNumber=13, masterKeyId=2");
		logStatements
				.add("INFO  : The url to track the job: http://hactive.rijin.com:8088/proxy/application_1485145997849_0013/");
		logStatements
				.add("INFO  : Starting Job = job_1485145997849_0013, Tracking URL = http://hactive.rijin.com:8088/proxy/application_1485145997849_0013/");
		logStatements.add(
				"INFO  : Hadoop job information for Stage-9: number of mappers: 2; number of reducers: 1");
		logStatements
				.add("INFO  : Kill Command = /opt/cloudera/parcels/CDH-5.7.4-1.cdh5.7.4.p0.2/lib/hadoop/bin/hadoop job  -kill job_1485145997849_0013");
		logStatements.add(
				"INFO  : Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0");
		logStatements.add(
				"INFO  : Submitting tokens for job: job_1485145997849_0014");
		logStatements.add(
				"INFO  : Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 7.86 sec   HDFS Read: 532149 HDFS Write: 3614 SUCCESS");
		logStatements.add(
				"INFO  : Stage-Stage-9: Map: 1  Reduce: 1   Cumulative CPU: 8.26 sec   HDFS Read: 549382 HDFS Write: 138 SUCCESS");
		logStatements.add(
				"INFO  : Stage-Stage-2: Map: 2   Cumulative CPU: 6.47 sec   HDFS Read: 78558 HDFS Write: 30467 SUCCESS");
		logStatements.add(
				"INFO  : Total MapReduce CPU Time Spent: 25 seconds 460 msec");
		logStatements.add(
				"INFO  : Completed executing command(queryId=hive_20190916103232_97d9573e-a419-41d4-a4df-4432a61ec1eb); Time taken: 92.653 seconds");
		logStatements.add(
				"INFO  : Partition project.ps_voice_segg_1_hour{dt=1568572200000, tz=tz_zone-1 2} stats: [numFiles=1, numRows=13, totalSize=26869, rawDataSize=1703]");
		logStatements.add(
				"INFO  : Compiling command(queryId=hive_20190927143535_42ba5126-2825-4746-862b-f80ac099441c): INSERT overwrite table PS_VOICE_SEGG_1_HOUR partition(dt='1568572200000'  , tz='Default' )");
		PowerMockito.when(statement.hasMoreLogs()).thenReturn(true)
				.thenReturn(false);
		PowerMockito.when(statement.getQueryLog(true, 0)).thenReturn(
				logStatements);

	}

	@Test
	public void testStartApplicationIdLog() throws WorkFlowExecutionException {
		TestLogger logger = TestLoggerFactory
				.getTestLogger(ApplicationIdLoggerHiveServer2Util.class
						.getName());
		ApplicationLoggerUtilInterface applicationlogger = ApplicationLoggerFactory
				.getApplicationLogger();
		Thread thread = applicationlogger.startApplicationIdLog(
				Thread.currentThread(), statement, "Perf", "100");
		applicationlogger.stopApplicationIdLog(thread);
		logger.getLoggingEvents().contains("application_1485145997849_0013");
	}
}
