
package com.project.rithomas.jobexecution.common.util;

import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

import org.apache.commons.collections.MapUtils;
import org.junit.Assert;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TimeZoneUtil.class, ZoneId.class })
public class TimeZoneUtilTest {

	JobExecutionContext context = new JobExecutionContext();

	List<Object[]> list;

	@Mock
	QueryExecutor queryExecutor;

	@Before
	public void setUp() throws Exception {
		whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExecutor);
		Object[] row1 = new Object[] { "region1", Double.parseDouble("0") };
		Object[] row2 = new Object[] { "region2", Double.parseDouble("1.5") };
		Object[] row3 = new Object[] { "region3", null };
		list = new ArrayList<Object[]>();
		list.add(row1);
		list.add(row2);
		list.add(row3);
		PowerMockito
				.when(queryExecutor
						.executeMetadatasqlQueryMultiple(
								"select tmz_region, time_difference from rithomas.region_timezone_info",
								context)).thenReturn(list);
		PowerMockito
				.when(queryExecutor
						.executeMetadatasqlQueryMultiple(
								"select tmz_region, time_difference from saidata.es_rgn_tz_info_1",
								context)).thenReturn(list);
		PowerMockito.mockStatic(ZoneId.class);
		ZoneId mockZoneId = PowerMockito.mock(ZoneId.class);
		PowerMockito.when(ZoneId.systemDefault()).thenReturn(mockZoneId);
		PowerMockito.when(mockZoneId.getId()).thenReturn("UTC").thenReturn("UTC").thenReturn("Asia/Kolkata").thenReturn("UTC");
		Map<String, String> regionZoneMapping = new HashMap<>();
		regionZoneMapping.put("region1", "Asia/Kolkata");
		regionZoneMapping.put("region2", "Asia/Kabul");
		regionZoneMapping.put("Default", "Asia/Kolkata");
		context.setProperty(JobExecutionContext.REGION_ZONEID_MAPPING,
				regionZoneMapping);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSetRegionTimeDiffOffset() throws JobExecutionException {
		// Postgres metadata
		TimeZoneUtil.setRegionTimeDiffOffset(context);
		Map<String, Long> map = (Map<String, Long>) context
				.getProperty(JobExecutionContext.REGION_TIMEZONE_OFFSET_MAP);
		validateMap(map);
		TimeZoneUtil.setRegionTimeDiffOffset(context);
		map = (Map<String, Long>) context
				.getProperty(JobExecutionContext.REGION_TIMEZONE_OFFSET_MAP);
		validateMap(map);
	}

	private void validateMap(Map<String, Long> map) {
		for (Entry<String, Long> entry : map.entrySet()) {
			if (entry.getKey().equals("region1")) {
				Assert.assertEquals(0L, (long) entry.getValue());
			} else if (entry.getKey().equals("region2")) {
				Assert.assertEquals(5400000L, (long) entry.getValue());
			} else if (entry.getKey().equals("region3")) {
				Assert.assertEquals(0L, (long) entry.getValue());
			} else {
				Assert.assertTrue("Invalid key: " + entry.getKey(), false);
			}
		}
	}

	@Test
	public void testIsDefaultTzSubPartition() {
		boolean defaultTzSubPart = TimeZoneUtil
				.isDefaultTZSubPartition(context);
		Assert.assertFalse(defaultTzSubPart);
		context.setProperty("DEFAULT_TZ_SUB_PARTITION", "YES");
		defaultTzSubPart = TimeZoneUtil.isDefaultTZSubPartition(context);
		Assert.assertTrue(defaultTzSubPart);
	}

	@Test
	public void testTimeZoneEnabled() {
		boolean timeZoneEnabled = TimeZoneUtil.isTimeZoneEnabled(context);
		Assert.assertFalse(timeZoneEnabled);
		context.setProperty("TIME_ZONE_SUPPORT", "YES");
		timeZoneEnabled = TimeZoneUtil.isTimeZoneEnabled(context);
		Assert.assertTrue(timeZoneEnabled);
	}
	
	@Test
	public void testTimeZoneEnabledArchiving() {
		context.setProperty("TIME_ZONE_SUPPORT", "NO");
		context.setProperty(JobExecutionContext.JOBTYPE, JobTypeDictionary.ARCHIVING_JOB_TYPE);
		context.setProperty(JobExecutionContext.SOURCEJOBID, "Perf_SMS_SEGG_1_HOUR_AggregateJob");
		context.setProperty("Perf_SMS_SEGG_1_HOUR_AggregateJob_TIME_ZONE_SUPPORT", "YES");
		boolean timeZoneEnabled = TimeZoneUtil.isTimeZoneEnabled(context);
		Assert.assertTrue(timeZoneEnabled);
	}

	@Test
	public void testGetRegionZoneIdMapping() throws JobExecutionException {
		Object[] row1 = new Object[] { "region1", "Asia/Kolkata" };
		Object[] row2 = new Object[] { "region2", "Asia/Kabul" };
		Object[] row3 = new Object[] { "region3", null };
		list = new ArrayList<Object[]>();
		list.add(row1);
		list.add(row2);
		list.add(row3);
		PowerMockito.when(queryExecutor.executeMetadatasqlQueryMultiple(
				"select tmz_region, tmz_name from saidata.es_rgn_tz_info_1 order by (case when tmz_name is not null and tmz_name!='null' then now() at time zone tmz_name else null end) desc",
				context)).thenReturn(list).thenReturn(null);
		Map<String, String> map = TimeZoneUtil.getRegionZoneIdMapping(context);
		Assert.assertTrue(MapUtils.isNotEmpty(map));
		Assert.assertEquals("Asia/Kolkata", map.get("region1"));
		Assert.assertEquals("Asia/Kabul", map.get("region2"));
		Assert.assertNull(map.get("region3"));
		Assert.assertEquals("Asia/Kolkata", map.get("Default"));
		map = TimeZoneUtil.getRegionZoneIdMapping(context);
		Assert.assertEquals("UTC", map.get("Default"));
		Assert.assertTrue(
				MapUtils.isEmpty(TimeZoneUtil.getRegionZoneIdMapping(context)));
	}
	
	@Test
	public void testGetZoneId() {
		Assert.assertEquals("Asia/Kabul", TimeZoneUtil.getZoneId(context, "region2"));
		Assert.assertEquals("Asia/Kolkata", TimeZoneUtil.getZoneId(context, null));
		Assert.assertNull(TimeZoneUtil.getZoneId(context, null));
		context.setProperty(JobExecutionContext.REGION_ZONEID_MAPPING,
				null);
		Assert.assertEquals("UTC", TimeZoneUtil.getZoneId(context, null));
	}

	@Test
	public void testIsTimezoneAgnostic() throws WorkFlowExecutionException {
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		context.setProperty(JobExecutionContext.JOB_NAME, "Perf_SMS_SEGG_1_HOUR_AggregateJob");
		Assert.assertFalse(TimeZoneUtil.isTimeZoneAgnostic(context));
		context.setProperty(JobExecutionContext.PLEVEL, "WEEK");
		context.setProperty(JobExecutionContext.JOB_NAME, "Perf_SMS_SEGG_1_WEEK_AggregateJob");
		Assert.assertTrue(TimeZoneUtil.isTimeZoneAgnostic(context));
		context.setProperty(JobExecutionContext.PLEVEL, null);
		context.setProperty(JobExecutionContext.JOB_NAME, "Perf_SMS_SEGG_1_WEEK_ArchivingJob");
		context.setProperty(JobExecutionContext.JOBTYPE, JobTypeDictionary.ARCHIVING_JOB_TYPE);
		context.setProperty(JobExecutionContext.SOURCEJOBID, "Perf_SMS_SEGG_1_WEEK_AggregateJob");
		context.setProperty("Perf_SMS_SEGG_1_WEEK_AggregateJob_PLEVEL", "WEEK");
		Assert.assertTrue(TimeZoneUtil.isTimeZoneAgnostic(context));
	}
}
