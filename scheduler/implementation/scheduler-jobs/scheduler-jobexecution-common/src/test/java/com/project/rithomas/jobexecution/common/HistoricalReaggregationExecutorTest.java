
package com.project.rithomas.jobexecution.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.hibernate.SessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.MetadataHibernateUtil;
import com.project.rithomas.jobexecution.common.util.PopulateReaggParamConfig;
import com.project.rithomas.jobexecution.common.util.HistoricalReaggregationExecutor;
import com.project.rithomas.jobexecution.common.util.PopulateReaggregationList;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.JobProperty;
import com.project.rithomas.sdk.model.meta.JobPropertyPK;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.others.JobProperties;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor(value = {
		"com.project.rithomas.jobexecution.common.util.MetadataHibernateUtil" })
@PrepareForTest(value = { HistoricalReaggregationExecutor.class,
		FileReader.class, BufferedReader.class, ModelResources.class,
		GetDBResource.class, PopulateReaggregationList.class,
		MetadataHibernateUtil.class })
public class HistoricalReaggregationExecutorTest {

	PopulateReaggParamConfig populateReaggParamConfig = new PopulateReaggParamConfig();

	@InjectMocks
	HistoricalReaggregationExecutor historicalReaggregationExecutor = new HistoricalReaggregationExecutor();

	JobDictionary jobDictionary = new JobDictionary();

	JobProperty jobProp = new JobProperty();

	List<JobProperty> jobPropList = new ArrayList<JobProperty>();

	JobProperties jobProperties = new JobProperties();

	JobTypeDictionary jobTypeDictionary = new JobTypeDictionary();

	JobPropertyPK jobPropertyPK = new JobPropertyPK();

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	HistoricalReaggregationExecutor historicalReaggregationExecutorMock;

	@Mock
	FileReader fileReaderMock;

	@Mock
	EntityManager entityManagerMock;

	@Mock
	private TypedQuery typedQueryMock;

	@Mock
	private GetDBResource mockGetDBResource;

	@Mock
	JobDictionary jobDictionaryMock;

	@Mock
	PopulateReaggregationList populateReaggregationListMock;

	@Mock
	SessionFactory mockSessionFactory;

	@Mock
	QueryExecutor queryExecMock;

	@Mock
	PopulateReaggParamConfig populateReaggParamConfigMock;

	PopulateReaggregationList populateReaggregationList = new PopulateReaggregationList();

	// HistoricalReaggregationExecutor historicalReaggregationExecutor = new
	// HistoricalReaggregationExecutor();
	@Before
	public void setUp() throws Exception {
		PowerMockito.mockStatic(ModelResources.class);
		PowerMockito.mockStatic(GetDBResource.class);
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(mockGetDBResource);
		PowerMockito.mockStatic(MetadataHibernateUtil.class);
		PowerMockito.when(MetadataHibernateUtil.getSessionFactory())
				.thenReturn(mockSessionFactory);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExecMock);
		FileReader fileReader = Mockito.mock(FileReader.class);
		BufferedReader bufferedReader = Mockito.mock(BufferedReader.class);
		PowerMockito.whenNew(FileReader.class).withAnyArguments()
				.thenReturn(fileReader);
		PowerMockito.whenNew(BufferedReader.class).withArguments(fileReader)
				.thenReturn(bufferedReader);
		PowerMockito.whenNew(PopulateReaggregationList.class).withNoArguments()
				.thenReturn(populateReaggregationListMock);
		PowerMockito.whenNew(HistoricalReaggregationExecutor.class)
				.withNoArguments()
				.thenReturn(historicalReaggregationExecutorMock);
		PowerMockito.whenNew(PopulateReaggParamConfig.class).withNoArguments()
				.thenReturn(populateReaggParamConfigMock);
		PowerMockito.when(bufferedReader.readLine())
				.thenReturn("Perf_SMS_SEGG_1_HOUR_AggregateJob,SMS_SEGG")
				.thenReturn("Perf_SMS_L2_1_HOUR_AggregateJob,CQI_SMS:CQI")
				.thenReturn(null);
		PowerMockito.when(ModelResources.getEntityManager())
				.thenReturn(entityManagerMock);
		PowerMockito.when(entityManagerMock
				.createNamedQuery(Mockito.anyString(), (Class) Mockito.any()))
				.thenReturn(typedQueryMock);
		PowerMockito.when(typedQueryMock.setParameter(Mockito.anyString(),
				Mockito.anyString())).thenReturn(typedQueryMock);
	}

	@Test
	public void testWithMultipleGroupsAsParam() throws Exception {
		String enableGroup = "VOICE_SEGG,SMS_SEGG";
		String disableGroup = "CQI_SMS";
		String fileName = "JobGroupMapping.csv";
		File file = FileSystems.getDefault().getPath(fileName).normalize()
				.toFile();
		String startTime = "2019-03-31 23:00:00";
		String endTime = "2019-04-01 01:00:00";
		String regionId = "";
		String[] arguments = { "-e", enableGroup, "-d", disableGroup, "-start",
				"2019-03-31 23:00:00", "-end", "2019-04-01 01:00:00", "-r", "",
				"-f", fileName };
		jobTypeDictionary.setJobtypeid("Aggregation");
		jobProperties.setJobProperty(jobPropList);
		jobDictionaryMock.setTypeid(jobTypeDictionary);
		jobDictionaryMock.setId(1);
		jobDictionaryMock.setJobProperties(jobProperties);
		jobDictionaryMock.setJobid("L2");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		jobDictionary.setJobProperties(jobProperties);
		jobDictionary.setJobid("Perf_SMS_L2_1_HOUR_ReaggregateJob");
		jobPropertyPK.setParamname("ALEVEL");
		jobPropertyPK.setJobid(1);
		jobProp.setParamvalue("HOUR");
		jobProp.setParamName("ALEVEL");
		jobProp.setJobPropPK(jobPropertyPK);
		jobProp.setJobDictionary(jobDictionary);
		jobProp.setJobId(1);
		jobPropList.add(jobProp);
		PowerMockito.when(typedQueryMock.getResultList())
				.thenReturn(jobPropList);
		PowerMockito.mockStatic(PopulateReaggregationList.class);
		PowerMockito.when(
				populateReaggregationListMock.validateInput(startTime, endTime))
				.thenReturn(true);
		PowerMockito
				.suppress(PowerMockito.methods(PopulateReaggregationList.class,
						"populateReaggListAndTriggerDependentJob"));
		HistoricalReaggregationExecutor.main(arguments);
		Mockito.verify(historicalReaggregationExecutorMock)
				.populateReaggListFromGroup(populateReaggParamConfigMock, file);
	}

	@Test
	public void testWithDisableParameter() throws Exception {
		String enableGroup = "";
		String disableGroup = "CQI_SMS";
		String fileName = "JobGroupMapping.csv";
		File file = FileSystems.getDefault().getPath(fileName).normalize()
				.toFile();
		String startTime = "2019-03-31 23:00:00";
		String endTime = "2019-04-01 01:00:00";
		String regionId = "";
		String[] arguments = { "-e", "", "-d", disableGroup, "-start",
				"2019-03-31 23:00:00", "-end", "2019-04-01 01:00:00", "-r",
				regionId, "-f", fileName };
		jobTypeDictionary.setJobtypeid("Aggregation");
		jobProperties.setJobProperty(jobPropList);
		jobDictionaryMock.setTypeid(jobTypeDictionary);
		jobDictionaryMock.setId(1);
		jobDictionaryMock.setJobProperties(jobProperties);
		jobDictionaryMock.setJobid("L2");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		jobDictionary.setJobProperties(jobProperties);
		jobDictionary.setJobid("Perf_SMS_L2_1_HOUR_ReaggregateJob");
		jobPropertyPK.setParamname("ALEVEL");
		jobPropertyPK.setJobid(1);
		jobProp.setParamvalue("HOUR");
		jobProp.setParamName("ALEVEL");
		jobProp.setJobPropPK(jobPropertyPK);
		jobProp.setJobDictionary(jobDictionary);
		jobProp.setJobId(1);
		jobPropList.add(jobProp);
		PowerMockito.when(typedQueryMock.getResultList())
				.thenReturn(jobPropList);
		PowerMockito.mockStatic(PopulateReaggregationList.class);
		PowerMockito.when(
				populateReaggregationListMock.validateInput(startTime, endTime))
				.thenReturn(true);
		PowerMockito
				.suppress(PowerMockito.methods(PopulateReaggregationList.class,
						"populateReaggListAndTriggerDependentJob"));
		HistoricalReaggregationExecutor.main(arguments);
		Mockito.verify(historicalReaggregationExecutorMock)
				.populateReaggListFromGroup(populateReaggParamConfigMock, file);
	}

	@Test
	public void testWithEnableParameter() throws Exception {
		String enableGroup = "VOICE_SEGG,SMS_SEGG";
		String disableGroups = "";
		String fileName = "JobGroupMapping.csv";
		File file = FileSystems.getDefault().getPath(fileName).normalize()
				.toFile();
		String startTime = "2019-03-31 23:00:00";
		String endTime = "2019-04-01 01:00:00";
		String regionId = "";
		String[] arguments = { "-e", enableGroup, "-d", "", "-start",
				"2019-03-31 23:00:00", "-end", "2019-04-01 01:00:00", "-r",
				regionId, "-f", fileName };
		jobTypeDictionary.setJobtypeid("Aggregation");
		jobProperties.setJobProperty(jobPropList);
		jobDictionaryMock.setTypeid(jobTypeDictionary);
		jobDictionaryMock.setId(1);
		jobDictionaryMock.setJobProperties(jobProperties);
		jobDictionaryMock.setJobid("L2");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		jobDictionary.setJobProperties(jobProperties);
		jobDictionary.setJobid("Perf_SMS_L2_1_HOUR_ReaggregateJob");
		jobPropertyPK.setParamname("ALEVEL");
		jobPropertyPK.setJobid(1);
		jobProp.setParamvalue("HOUR");
		jobProp.setParamName("ALEVEL");
		jobProp.setJobPropPK(jobPropertyPK);
		jobProp.setJobDictionary(jobDictionary);
		jobProp.setJobId(1);
		jobPropList.add(jobProp);
		PowerMockito.when(typedQueryMock.getResultList())
				.thenReturn(jobPropList);
		PowerMockito.mockStatic(PopulateReaggregationList.class);
		PowerMockito.when(
				populateReaggregationListMock.validateInput(startTime, endTime))
				.thenReturn(true);
		PowerMockito
				.suppress(PowerMockito.methods(PopulateReaggregationList.class,
						"populateReaggListAndTriggerDependentJob"));
		HistoricalReaggregationExecutor.main(arguments);
		Mockito.verify(historicalReaggregationExecutorMock)
				.populateReaggListFromGroup(populateReaggParamConfigMock, file);
	}

	@Test
	public void testWithMultipleGroupForJobId() throws Exception {
		String enableGroup = "VOICE_SEGG,SMS_SEGG";
		String disableGroup = "CQI_SMS:CQI";
		String fileName = "JobGroupMapping.csv";
		File file = FileSystems.getDefault().getPath(fileName).normalize()
				.toFile();
		String startTime = "2019-03-31 23:00:00";
		String endTime = "2019-04-01 01:00:00";
		String regionId = "";
		String[] arguments = { "-e", enableGroup, "-d", "", "-start",
				"2019-03-31 23:00:00", "-end", "2019-04-01 01:00:00", "-r", "",
				"-f", fileName };
		jobTypeDictionary.setJobtypeid("Aggregation");
		jobProperties.setJobProperty(jobPropList);
		jobDictionaryMock.setTypeid(jobTypeDictionary);
		jobDictionaryMock.setId(1);
		jobDictionaryMock.setJobProperties(jobProperties);
		jobDictionaryMock.setJobid("L2");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		jobDictionary.setJobProperties(jobProperties);
		jobDictionary.setJobid("Perf_SMS_L2_1_HOUR_ReaggregateJob");
		jobPropertyPK.setParamname("ALEVEL");
		jobPropertyPK.setJobid(1);
		jobProp.setParamvalue("HOUR");
		jobProp.setParamName("ALEVEL");
		jobProp.setJobPropPK(jobPropertyPK);
		jobProp.setJobDictionary(jobDictionary);
		jobProp.setJobId(1);
		jobPropList.add(jobProp);
		PowerMockito.when(typedQueryMock.getResultList())
				.thenReturn(jobPropList);
		PowerMockito.mockStatic(PopulateReaggregationList.class);
		PowerMockito.when(
				populateReaggregationListMock.validateInput(startTime, endTime))
				.thenReturn(true);
		PowerMockito
				.suppress(PowerMockito.methods(PopulateReaggregationList.class,
						"populateReaggListAndTriggerDependentJob"));
		HistoricalReaggregationExecutor.main(arguments);
		Mockito.verify(historicalReaggregationExecutorMock)
				.populateReaggListFromGroup(populateReaggParamConfigMock, file);
	}
}