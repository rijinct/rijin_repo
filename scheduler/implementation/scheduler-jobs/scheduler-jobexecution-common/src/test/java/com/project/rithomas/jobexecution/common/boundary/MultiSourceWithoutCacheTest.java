package com.project.rithomas.jobexecution.common.boundary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

import org.apache.commons.lang.time.DateUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.LBUBUtil;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;


@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { MultiSourceWithoutCache.class, JobDetails.class,
		JobPropertyRetriever.class, LBUBUtil.class, BoundaryQuery.class, BoundaryCalculator.class})
public class MultiSourceWithoutCacheTest {
	
	WorkFlowContext context = new JobExecutionContext();

	
	@Mock
	JobDetails jobDetailsMock;
	

	@Mock
	MultiSourceWithoutCache multiSourceWithoutCacheMock;
	
	@Mock 
	WorkFlowContext workFlowContextMock;
	
	@Mock
	JobPropertyRetriever jobPropertyRetrieverMock;
	
	@Mock
	QueryExecutor queryExec;
	
	
	@Mock
	BoundaryQuery boundaryQueryMock;

	@Mock
	Calendar calendarMock;
	
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
		
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
		.thenReturn(boundaryQueryMock);
		

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
	public void calculateUBTestWithoutEntryNoInSGSNAvailableData() throws Exception {
		
		Calendar calendar = new GregorianCalendar();
		String regionId = "RGN1";
		Date dateVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2019-03-12 00:00:00");
		Long lbVal = DateFunctionTransformation.getInstance().getTrunc(
				dateVal.getTime(), "HOUR");
	
		List<String> stageType = Arrays.asList("VOICE");

		List<String> srcJobList = Arrays.asList("Usage_VOICE_1");
		List<String> strList = Arrays.asList("VOICE", "1");
		
		String query = "select type from saidata.es_sgsn_available_data_1 where type='US_VOICE_1' and available='No'";
		
		PowerMockito.when(workFlowContextMock.getProperty("LB_RGN1")).thenReturn(lbVal);
		
		PowerMockito.when(jobDetailsMock.getSourceJobList("RGN1")).thenReturn(srcJobList);
		
		MultiSourceWithoutCache multiSourceWithoutCache = new MultiSourceWithoutCache(workFlowContextMock);
		
		PowerMockito.when(jobDetailsMock.getSourceJobType()).thenReturn("Loading");
		
		PowerMockito.mockStatic(LBUBUtil.class);
		
		PowerMockito.when(LBUBUtil.getSpecIDVerFromJobName("Usage_VOICE_1",
								"Loading")).thenReturn(strList);
		
		PowerMockito.when(LBUBUtil.isUsageTypeJob("Loading")).thenReturn(true);
		
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				query, workFlowContextMock)).thenReturn(stageType);
		
		Calendar calendarUBValue = multiSourceWithoutCache.calculateUB(regionId);
		
		PowerMockito.mockStatic(Calendar.class);
		PowerMockito.whenNew(Calendar.class).withNoArguments()
		.thenReturn(calendarMock);
		
		System.out.println(calendarUBValue.getTime());
		System.out.println(calendar.getTime());
		assertTrue(DateUtils.truncatedEquals(calendar.getTime(),calendarUBValue.getTime(),Calendar.MINUTE));		
		
	}
	
	
	@Test
	public void calculateUBTestWithEntryYesInSGSNAvailableData() throws Exception {
		
		Timestamp timeStamp = new Timestamp(new SimpleDateFormat(
				"yyyy.MM.dd HH:mm:ss", Locale.ENGLISH).parse(
				"2019.03.12 00:00:00").getTime());
		String regionId = "RGN1";
		Date dateVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2019-03-12 00:00:00");
		Long lbVal = DateFunctionTransformation.getInstance().getTrunc(
				dateVal.getTime(), "HOUR");		

		List<String> srcJobList = Arrays.asList("Usage_VOICE_1");
		List<String> strList = Arrays.asList("VOICE", "1");
		
		Map<String,Long> leastReportTimeMap = new HashMap<String, Long>();
		leastReportTimeMap.put("US_VOICE_1",lbVal);
		
		String query = "select type from saidata.es_sgsn_available_data_1 where type='US_VOICE_1' and available='No'";
		
		List<Boundary> srcBoundaryList =  new ArrayList<Boundary>();
		Boundary srcBoundary = new Boundary();
		srcBoundary.setSourceJobType("Loading");
		srcBoundary.setJobId("Usage_VOICE_1_LoadJob");
		srcBoundary.setId(1);
		srcBoundary.setMaxValue(timeStamp);
		srcBoundaryList.add(srcBoundary);
		String pLevel = "HOUR";
		
		Entry<String, Long> maxEntry = stubMaxEntryValue(leastReportTimeMap);
		
		PowerMockito.when(workFlowContextMock.getProperty("LB_RGN1")).thenReturn(lbVal);
		
		PowerMockito.when(jobDetailsMock.getSourceJobList("RGN1")).thenReturn(srcJobList);
		
		MultiSourceWithoutCache multiSourceWithoutCache = new MultiSourceWithoutCache(workFlowContextMock);
		
		PowerMockito.when(jobDetailsMock.getSourceJobType()).thenReturn("Loading");
		
		PowerMockito.mockStatic(LBUBUtil.class);
		
		PowerMockito.when(LBUBUtil.getSpecIDVerFromJobName("Usage_VOICE_1",
								"Loading")).thenReturn(strList);
		
		PowerMockito.when(LBUBUtil.isUsageTypeJob("Loading")).thenReturn(true);
		
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				query, workFlowContextMock)).thenReturn(null);
		
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(null)).thenReturn(srcBoundaryList);
		
		PowerMockito.when(jobPropertyRetrieverMock.getPartitionLevel()).thenReturn(pLevel);
		
		PowerMockito.when(jobDetailsMock.getLeastMaxValueOfSrcs
				(leastReportTimeMap)).thenReturn(maxEntry);
		
		PowerMockito.when(workFlowContextMock.getProperty(JobExecutionContext.WEEK_START_DAY)).
		thenReturn("0");
		
		PowerMockito.when(workFlowContextMock.getProperty(JobExecutionContext.PLEVEL)).
		thenReturn("HOUR");
		
		Calendar calendarUBValue = multiSourceWithoutCache.calculateUB(regionId);

		assertEquals(dateVal, calendarUBValue.getTime());
		
		
	}
}
