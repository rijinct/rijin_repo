package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { ModifySourceJobList.class })
public class ModifySourceJobListTest {

	WorkFlowContext context = new JobExecutionContext();

	Map<String, List<String>> baseSourceMap = new HashMap<>();

	@Mock
	QueryExecutor queryExec;

	@Before
	public void setUp() throws Exception {
		baseSourceMap.clear();
		context.setProperty(JobExecutionContext.BASE_SOURCE_JOB_MAP,
				baseSourceMap);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		String browsingServicesQuery = "select type from saidata.es_sgsn_available_data_1 where source ='BB_BROWSING' and upper(available) = 'YES'";
		String appServicesQuery = "select type from saidata.es_sgsn_available_data_1 where source ='BB_APPLICATIONS' and upper(available) = 'YES'";
		String streamingServicesQuery = "select type from saidata.es_sgsn_available_data_1 where source ='BB_STREAMING' and upper(available) = 'YES'";
		String browsingServicesQueryIn = "select type from saidata.es_sgsn_available_data_1 where source in ('BB_BROWSING') and upper(available) = 'YES'";
		String streamingServicesQueryIn = "select type from saidata.es_sgsn_available_data_1 where source in ('BB_STREAMING') and upper(available) = 'YES'";
		String browsingServicesQueryInWithoutAvailableSrc = "select type from saidata.es_sgsn_available_data_1 where source in ('BB_BROWSING')";
		String streamingServicesQueryInWithoutAvailableSrc = "select type from saidata.es_sgsn_available_data_1 where source in ('BB_STREAMING')";
		String streamingServicesQueryWithoutAvailableSrc = "select type from saidata.es_sgsn_available_data_1 where source ='BB_STREAMING'";
		String browsingServicesQueryWithoutAvailableSrc = "select type from saidata.es_sgsn_available_data_1 where source ='BB_BROWSING'";
		String skipRg1Query = "select TIME_SKIPPED_SERVICES from saidata.es_rgn_tz_info_1 where tmz_region='region1'";
		String skipRg2Query = "select TIME_SKIPPED_SERVICES from saidata.es_rgn_tz_info_1 where tmz_region='region2'";
		String skipRg3Query = "select TIME_SKIPPED_SERVICES from saidata.es_rgn_tz_info_1 where tmz_region='region3'";
		String skipServicesRg1 = "BR_CABLE,BR_COPPER,BR_FIBER";
		String skipServicesRg2 = "STR_CABLE,STR_COPPER,STR_FIBER";
		String skipServicesRg3 = "BR_CABLE,BR_COPPER,BR_FIBER,STR_CABLE,STR_COPPER,STR_FIBER";
		List<String> browsingServicesList = Arrays.asList("BR_CABLE",
				"BR_COPPER", "BR_FIBER");
		List<String> appServicesList = Arrays.asList("APP_CABLE", "APP_COPPER",
				"APP_FIBER");
		List<String> streamingServicesList = Arrays.asList("STR_CABLE",
				"STR_COPPER", "STR_FIBER");
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						browsingServicesQuery, context))
				.thenReturn(browsingServicesList);
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
				browsingServicesQueryWithoutAvailableSrc, context))
				.thenReturn(browsingServicesList);
		PowerMockito.when(queryExec
				.executeMetadatasqlQueryMultiple(appServicesQuery, context))
				.thenReturn(appServicesList);
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						streamingServicesQueryWithoutAvailableSrc, context))
				.thenReturn(streamingServicesList);
		PowerMockito
		.when(queryExec.executeMetadatasqlQueryMultiple(
				streamingServicesQuery, context))
		.thenReturn(streamingServicesList);
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						browsingServicesQueryIn, context))
				.thenReturn(browsingServicesList);
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						browsingServicesQueryInWithoutAvailableSrc, context))
				.thenReturn(browsingServicesList);
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						streamingServicesQueryIn, context))
				.thenReturn(streamingServicesList);
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						streamingServicesQueryInWithoutAvailableSrc, context))
				.thenReturn(streamingServicesList);
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						"select tmz_region,TIME_SKIPPED_SERVICES from saidata.es_rgn_tz_info_1",
						context))
				.thenReturn(Arrays.asList(
						new Object[] { "region1", skipServicesRg1 },
						new Object[] { "region2", skipServicesRg2 },
						new Object[] { "region3", skipServicesRg3 }));
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(skipRg1Query, context))
				.thenReturn(new Object[] { skipServicesRg1 });
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(skipRg2Query, context))
				.thenReturn(new Object[] { skipServicesRg2 });
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(skipRg3Query, context))
				.thenReturn(new Object[] { skipServicesRg3 });
	}

	@Test
	public void testGetListOfSkipTimeZoneForAgnosticJobs() throws Exception {
		baseSourceMap.put("Perf_BB_STR_SEGG_1_DAY_AggregateJob",
				Arrays.asList("Usage_BB_STREAMING_1_LoadJob"));
		ModifySourceJobList modifySrcJobList = new ModifySourceJobList(context);
		List<String> regionsToSkip = modifySrcJobList
				.getListOfSkipTimeZoneForAgnosticJobs();
		assertTrue(regionsToSkip.contains("region2"));
		assertTrue(regionsToSkip.contains("region3"));
		assertFalse(regionsToSkip.contains("region1"));
		baseSourceMap.clear();
		baseSourceMap.put("Perf_BB_WS_SEGG_1_DAY_AggregateJob",
				Arrays.asList("Usage_BB_BROWSING_1_LoadJob"));
		modifySrcJobList = new ModifySourceJobList(context);
		regionsToSkip = modifySrcJobList.getListOfSkipTimeZoneForAgnosticJobs();
		assertTrue(regionsToSkip.contains("region1"));
		assertTrue(regionsToSkip.contains("region3"));
		assertFalse(regionsToSkip.contains("region2"));
	}

	@Test
	public void testModifySourceJobListForAgg() throws Exception {
		baseSourceMap.put("Perf_BB_STR_SEGG_1_DAY_AggregateJob",
				Arrays.asList("Usage_BB_STREAMING_1_LoadJob"));
		baseSourceMap.put("Perf_BB_APP_SEGG_1_DAY_AggregateJob",
				Arrays.asList("Usage_BB_BROWSING_1_LoadJob",
						"Usage_BB_APPLICATIONS_1_LoadJob",
						"Usage_BB_STREAMING_1_LoadJob"));
		baseSourceMap.put("Perf_BB_WS_SEGG_1_DAY_AggregateJob",
				Arrays.asList("Usage_BB_BROWSING_1_LoadJob"));
		String servicesQuery = "select type from saidata.es_sgsn_available_data_1 where source in ('BB_BROWSING','BB_APPLICATIONS','BB_STREAMING')";
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(servicesQuery,
						context))
				.thenReturn(Arrays.asList("STR_CABLE", "STR_COPPER",
						"STR_FIBER", "APP_CABLE", "APP_COPPER", "APP_FIBER",
						"BR_CABLE", "BR_COPPER", "BR_FIBER"));
		List<String> srcJobList = Arrays.asList(
				"Perf_BB_STR_SEGG_1_DAY_AggregateJob",
				"Perf_BB_APP_SEGG_1_DAY_AggregateJob",
				"Perf_BB_WS_SEGG_1_DAY_AggregateJob");
		ModifySourceJobList modifySrcJobList = new ModifySourceJobList(context,
				"region1", srcJobList);
		Set<String> modifiedSourceList = modifySrcJobList
				.modifySourceJobListForAgg();
		assertTrue(modifiedSourceList
				.contains("Perf_BB_APP_SEGG_1_DAY_AggregateJob"));
		assertTrue(modifiedSourceList
				.contains("Perf_BB_STR_SEGG_1_DAY_AggregateJob"));
		assertFalse(modifiedSourceList
				.contains("Perf_BB_WS_SEGG_1_DAY_AggregateJob"));
		modifySrcJobList = new ModifySourceJobList(context, "region2",
				srcJobList);
		modifiedSourceList = modifySrcJobList.modifySourceJobListForAgg();
		assertTrue(modifiedSourceList
				.contains("Perf_BB_APP_SEGG_1_DAY_AggregateJob"));
		assertTrue(modifiedSourceList
				.contains("Perf_BB_WS_SEGG_1_DAY_AggregateJob"));
		assertFalse(modifiedSourceList
				.contains("Perf_BB_STR_SEGG_1_DAY_AggregateJob"));
		modifySrcJobList = new ModifySourceJobList(context, "region2",
				srcJobList);
		modifiedSourceList = modifySrcJobList.modifySourceJobListForAgg();
		assertTrue(modifiedSourceList
				.contains("Perf_BB_APP_SEGG_1_DAY_AggregateJob"));
		assertTrue(modifiedSourceList
				.contains("Perf_BB_WS_SEGG_1_DAY_AggregateJob"));
		assertFalse(modifiedSourceList
				.contains("Perf_BB_STR_SEGG_1_DAY_AggregateJob"));
	}
}
