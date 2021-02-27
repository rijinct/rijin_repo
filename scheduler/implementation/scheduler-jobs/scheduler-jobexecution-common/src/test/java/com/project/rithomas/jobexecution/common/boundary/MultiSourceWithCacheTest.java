package com.project.rithomas.jobexecution.common.boundary;

import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import com.project.rithomas.jobexecution.common.util.DataAvailabilityCacheUtil;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;


@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { MultiSourceWithCache.class, JobDetails.class,
		JobPropertyRetriever.class})
public class MultiSourceWithCacheTest {
	
	WorkFlowContext context = new JobExecutionContext();

	
	@Mock
	JobDetails jobDetailsMock;
	

	@Mock
	MultiSourceWithCache multiSourceWithCacheMock;
	
	@Mock 
	WorkFlowContext workFlowContextMock;
	
	@Mock
	JobPropertyRetriever jobPropertyRetrieverMock;
	
	@Mock
	QueryExecutor queryExec;
	
	@Mock
	DataAvailabilityCacheUtil dataAvailabilityCacheUtilMock;

	
	@Before
	public void setUp() throws Exception {
		
		PowerMockito.whenNew(WorkFlowContext.class).withNoArguments()
		.thenReturn(workFlowContextMock);
		
		PowerMockito.whenNew(JobDetails.class).withAnyArguments()
		.thenReturn(jobDetailsMock);
		
		PowerMockito.whenNew(JobPropertyRetriever.class).withAnyArguments()
		.thenReturn(jobPropertyRetrieverMock);
		
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
		.thenReturn(queryExec);
		
		PowerMockito.whenNew(DataAvailabilityCacheUtil.class).withNoArguments().thenReturn(dataAvailabilityCacheUtilMock);

	}
	
	private Entry<String, Long> stubMaxEntryValue(Map<String, Long> leastReportTimeMap) {
		Entry<String, Long> maxEntry = null;
		for (Entry<String, Long> entry : leastReportTimeMap.entrySet()) {
			if (entry.getValue() != null && (maxEntry == null
					|| entry.getValue() > maxEntry.getValue())) {
				maxEntry = entry;
			}
		}
		return maxEntry;
	}
	
	@Test
	public void testCalculateUBRegionNull() throws Exception {
		
		Calendar calendar = new GregorianCalendar();
		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
		List<Boundary> srcBoundaryList =  new ArrayList<Boundary>();
		Date dateVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2019-03-12 00:00:00");
		Long lbVal = DateFunctionTransformation.getInstance().getTrunc(
				dateVal.getTime(), "HOUR");
		
		Date dateValLeastReportMap =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2019-03-13 16:00:00");
		Long lbValLeastReportMap = DateFunctionTransformation.getInstance().getTrunc(
				dateValLeastReportMap.getTime(), "HOUR");
		
		Boundary srcBoundary = new Boundary();
		srcBoundary.setSourceJobType("Loading");
		srcBoundary.setJobId("Usage_SGSN_1_LoadJob");
		srcBoundary.setId(1);
		srcBoundary.setMaxValue(timeStamp);
		srcBoundaryList.add(srcBoundary);
		String pLevel = "HOUR";
		String query = "select type from saidata.es_sgsn_available_data_1 where source='SGSN' and upper(available) = 'YES'";
		List<String> stageTypeSGSN = Arrays.asList("2G", "3G");
		List<String> excludedStageTypes = Arrays.asList("GB", "IuPS");
		
		Map<String, Long> leastReportTimeMap = new HashMap<String, Long>();
		
		leastReportTimeMap.put("2G",lbValLeastReportMap);
		leastReportTimeMap.put("3G",lbValLeastReportMap);
		
		Entry<String, Long> maxEntry = stubMaxEntryValue(leastReportTimeMap);
		
		PowerMockito.when(workFlowContextMock.getProperty("LB_RGN1")).thenReturn(lbVal);
		
		PowerMockito.when(jobDetailsMock.getSourceBoundList("RGN1")).thenReturn(srcBoundaryList);
		
		PowerMockito.when(jobPropertyRetrieverMock.getExcludedStageTypes()).thenReturn(excludedStageTypes);
		
		PowerMockito.when(jobPropertyRetrieverMock.getPartitionLevel()).thenReturn(pLevel);
		
		PowerMockito.when(jobDetailsMock.getUsageSpecId("Usage_SGSN_1_LoadJob")).thenReturn("SGSN");
		
		MultiSourceWithCache multiSourceWithCache = new MultiSourceWithCache(workFlowContextMock);
		
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				query, workFlowContextMock)).thenReturn(stageTypeSGSN);
		
		PowerMockito.when(jobDetailsMock.getAdaptationFromJobName
				("Usage_SGSN_1_LoadJob")).thenReturn("SGSN");
		
		PowerMockito.when(jobDetailsMock.getSpecIdVersion
				("Usage_SGSN_1_LoadJob")).thenReturn("1");
		
		
		PowerMockito.when(dataAvailabilityCacheUtilMock.getValue("SGSN:1:1552473000000:2G")).thenReturn("1");
		
		PowerMockito.when(dataAvailabilityCacheUtilMock.getValue("SGSN:1:1552473000000:3G")).thenReturn("1");
		
		PowerMockito.when(jobDetailsMock.getLeastMaxValueOfSrcs
				(leastReportTimeMap)).thenReturn(maxEntry);
		
		PowerMockito.when(workFlowContextMock.getProperty(JobExecutionContext.WEEK_START_DAY)).
		thenReturn("0");
		
		PowerMockito.when(workFlowContextMock.getProperty(JobExecutionContext.PLEVEL)).
		thenReturn("HOUR");
		
		Calendar calendarUBValue = multiSourceWithCache.calculateUB(regionId);
		
		System.out.println(calendarUBValue.getTime());
		
		assertEquals(dateValLeastReportMap, calendarUBValue.getTime());
		
		
	}
}
