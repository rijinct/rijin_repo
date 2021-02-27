
package com.project.rithomas.jobexecution.project;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
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

import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DistinctSubscriberCountUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.common.CharacteristicSpecification;
import com.project.rithomas.sdk.model.usage.UsageSpecification;
import com.project.rithomas.sdk.model.usage.UsageSpecificationCharacteristicUse;
import com.project.rithomas.sdk.model.usage.query.UsageSpecificationQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.CodeGeneratorException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { GetSubscriberCountDetails.class,
		DistinctSubscriberCountUtil.class, ConnectionManager.class })
public class GetSubscriberCountDetailsTest {

	GetSubscriberCountDetails getSubscriberCountDetails = null;

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	Connection mockConnection;

	@Mock
	PreparedStatement mockStatement;

	@Before
	public void setUp() throws Exception {
		getSubscriberCountDetails = new GetSubscriberCountDetails();
		List<String> tableNames = new ArrayList<String>();
		tableNames.add("us_sms_1");
		mockStatic(DistinctSubscriberCountUtil.class);
		PowerMockito.when(
				DistinctSubscriberCountUtil.getSourceTableNames(context))
				.thenReturn(tableNames);
		UsageSpecificationQuery mockUsageSpecificationQuery = mock(UsageSpecificationQuery.class);
		PowerMockito.whenNew(UsageSpecificationQuery.class).withNoArguments()
				.thenReturn(mockUsageSpecificationQuery);
		UsageSpecification usageSpecification = new UsageSpecification();
		usageSpecification.setSpecId("SMS");
		usageSpecification.setVersion("1");
		CharacteristicSpecification characteristicSpecification = new CharacteristicSpecification();
		characteristicSpecification.setSpecCharId("SMS_IMSI");
		characteristicSpecification.setName("SMS_IMSI");
		UsageSpecificationCharacteristicUse usageSpecCharUse = new UsageSpecificationCharacteristicUse();
		usageSpecCharUse.setSpecChar(characteristicSpecification);
		Collection<UsageSpecificationCharacteristicUse> usageSpecificationCharacteristicUses = new ArrayList<UsageSpecificationCharacteristicUse>();
		usageSpecificationCharacteristicUses.add(usageSpecCharUse);
		usageSpecification
				.setUsageSpecsCharUse(usageSpecificationCharacteristicUses);
		Mockito.when(
				mockUsageSpecificationQuery.retrieve(Mockito.anyString(),
						Mockito.anyString())).thenReturn(usageSpecification);
		mockStatic(ConnectionManager.class);
		PowerMockito.when(
				ConnectionManager
						.getConnection(schedulerConstants.HIVE_DATABASE))
				.thenReturn(mockConnection);
		Mockito.when(mockConnection.createStatement())
				.thenReturn(mockStatement);
		context.setProperty(JobExecutionContext.LB,
				Long.parseLong("1390780800000"));
		context.setProperty(JobExecutionContext.UB,
				Long.parseLong("1390867200000"));
		context.setProperty(JobExecutionContext.NEXT_LB,
				Long.parseLong("1390867200000"));
				+ "select imsi_cnt from ("
				+ "$SOURCE_PERF_SPEC_PARAMS:{ obj | $if(obj.comma)$ UNION ALL $endif$"
