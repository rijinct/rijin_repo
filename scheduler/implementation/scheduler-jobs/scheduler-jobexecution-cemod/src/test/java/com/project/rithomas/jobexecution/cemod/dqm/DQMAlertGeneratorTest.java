
package com.project.rithomas.jobexecution.project.dqm;

import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfiguration;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.etl.notifier.email.EmailNotificationSender;
import com.project.rithomas.etl.properties.runtime.ETLRuntimePropRetriever;
import com.project.rithomas.etl.threshold.ETLThresholdRetriever;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DriverManager.class, ConnectionManager.class,
		DQMAlertGenerator.class, HiveConfigurationProvider.class })
public class DQMAlertGeneratorTest {

	DQMAlertGenerator dqmAlertGenerator = new DQMAlertGenerator();

	WorkFlowContext context = new JobExecutionContext();

	Map<String, Object> notificationData = new HashMap<String, Object>();;

	@Mock
	UpdateJobStatus updateJobStatus;

	@Mock
	ETLThresholdRetriever etlThresholdRetriever;

	@Mock
	ETLRuntimePropRetriever etlRuntimePropRetriever;

	@Mock
	DQMSNMPNotificationSender dqmsnmpNotificationSender;

	@Mock
	EmailNotificationSender emailNotificationSender;

	@Mock
	ResultSet result;

	@Mock
	Connection connection;

	@Mock
	PreparedStatement statement;

	@Mock
	Session session;

	@Mock
	private HiveConfigurationProvider mockHiveConfigurationProvider;

	@Mock
	private HiveConfiguration mockHiveConfiguration;

	@Before
	public void setUp() throws Exception {
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		mockStatic(DriverManager.class);
		mockStatic(ConnectionManager.class);
		mockStatic(HiveConfigurationProvider.class);
		// mockStatic(RetrieveDimensionValues.class);
		PowerMockito.when(ConnectionManager.getConnection(Mockito.anyString()))
				.thenReturn(connection);
		PowerMockito.when(connection.createStatement()).thenReturn(statement);
		Mockito.when(statement.executeQuery(Mockito.anyString()))
				.thenReturn(result);
		Mockito.when(result.next()).thenReturn(true, true, false, true, true,
				false, true, true, false, true, true, false, true, true, false,
				true, true, false);
		Mockito.when(result.getInt(2)).thenReturn(5, 10, 5, 10, 5, 10, 20);
		Mockito.when(result.getInt(3)).thenReturn(100, 20, 10, 200, 35);
		Mockito.when(result.getString(1)).thenReturn("SMS_1", "VOICE_1",
				"SMS_1", "VOICE_1", "SMS_1", "VOICE_1");
		PowerMockito.whenNew(EmailNotificationSender.class).withNoArguments()
				.thenReturn(emailNotificationSender);
		PowerMockito.whenNew(DQMSNMPNotificationSender.class).withNoArguments()
				.thenReturn(dqmsnmpNotificationSender);
		PowerMockito.whenNew(ETLRuntimePropRetriever.class).withNoArguments()
				.thenReturn(etlRuntimePropRetriever);
		Mockito.when(etlRuntimePropRetriever.retrieveSnmpEnabled())
				.thenReturn("YES");
		Mockito.when(etlRuntimePropRetriever.retrieveEmailEnabled())
				.thenReturn("YES");
		PowerMockito.whenNew(ETLThresholdRetriever.class).withNoArguments()
				.thenReturn(etlThresholdRetriever);
		Mockito.when(etlThresholdRetriever.retrieveDuplicateRecordsThreshold())
				.thenReturn(20);
		Mockito.when(etlThresholdRetriever.retrieveRejectedRecordsThreshold())
				.thenReturn(25);
		Mockito.when(etlThresholdRetriever.retrieveComparatorForRejectedRec())
				.thenReturn(">", "=");
		Mockito.when(etlThresholdRetriever.retrieveComparatorForDuplicateRec())
				.thenReturn("=", ">");
		context.setProperty(JobExecutionContext.DQM_SCAN_INTERVAL, "10MIN");
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		Mockito.when(mockHiveConfigurationProvider.getConfiguration())
				.thenReturn(mockHiveConfiguration);
		Mockito.when(mockHiveConfiguration.getDbDriver())
				.thenReturn("dbDriver");
		Mockito.when(mockHiveConfiguration.getDbUrl()).thenReturn("dbUrl");
		Mockito.when(mockHiveConfiguration.getHiveUserName())
				.thenReturn("hiveUserName");
	}

	@Test
	public void testExecute() throws WorkFlowExecutionException {
		dqmAlertGenerator.execute(context);
		dqmAlertGenerator.execute(context);
	}
}
