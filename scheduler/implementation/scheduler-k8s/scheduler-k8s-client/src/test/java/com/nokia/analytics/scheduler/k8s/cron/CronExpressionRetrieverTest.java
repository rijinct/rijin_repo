
package com.rijin.analytics.scheduler.k8s.cron;

import static org.junit.Assert.assertEquals;

import java.time.ZoneId;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.http.HttpStatus;

import com.rijin.analytics.exception.configuration.ContextProvider;
import com.rijin.analytics.exceptions.ServiceException;
import com.rijin.analytics.scheduler.k8s.cron.CronExpressionRetriever;
import com.rijin.analytics.scheduler.k8s.cron.converter.CalendarToCronTransformer;
import com.rijin.analytics.scheduler.k8s.cron.converter.CronConverter;
import com.rijin.analytics.scheduler.k8s.cron.converter.CronToCalendarTransformer;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ CronExpressionRetriever.class, ServiceException.class })
public class CronExpressionRetrieverTest {

	@Mock
	private ServiceException mockServiceException;

	@Mock
	private CronConverter mockCronConverter;

	String expected = "40 * * * *";

	@Before
	public void setUp() throws Exception {
		PowerMockito.whenNew(ServiceException.class)
				.withArguments("cronExpressionValidation",
						HttpStatus.UNPROCESSABLE_ENTITY)
				.thenReturn(mockServiceException);
		PowerMockito.whenNew(CronConverter.class).withNoArguments()
				.thenReturn(mockCronConverter);
	}

	@Test(expected = ServiceException.class)
	public void testQuartzWithBothDayOfWeekAndDayOfMonth() {
		String cronExpression = "0 10 * * * *";
		CronExpressionRetriever.getCronExpression(cronExpression);
	}

	@Test
	public void testQuartz() {
		String cronExpression = "0 10 * ? * *";
		Mockito.when(mockCronConverter.using(Mockito.anyString()))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.from(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.to(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.convert()).thenReturn(expected);
		assertEquals(expected,
				CronExpressionRetriever.getCronExpression(cronExpression));
	}

	@Test
	public void testQuartzWithSpace() {
		String cronExpression = "0 10 * ? * * ";
		Mockito.when(mockCronConverter.using(Mockito.anyString()))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.from(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.to(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.convert()).thenReturn(expected);
		assertEquals(expected,
				CronExpressionRetriever.getCronExpression(cronExpression));
	}

	@Test
	public void testQuartzWithYear() {
		String cronExpression = "0 10 * ? * * *";
		Mockito.when(mockCronConverter.using(Mockito.anyString()))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.from(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.to(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.convert()).thenReturn(expected);
		assertEquals(expected,
				CronExpressionRetriever.getCronExpression(cronExpression));
	}

	@Test
	public void testUnix() {
		String cronExpression = "10 * * * *";
		Mockito.when(mockCronConverter.using(Mockito.anyString()))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.from(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.to(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.convert()).thenReturn(expected);
		assertEquals(expected,
				CronExpressionRetriever.getCronExpression(cronExpression));
	}

	@Test(expected = ServiceException.class)
	public void testUnixWithQuestionMark() {
		String cronExpression = "10 * * ? *";
		CronExpressionRetriever.getCronExpression(cronExpression);
	}

	@Test
	public void testQuartzToUnixDayOfWeek() {
		String cronExpression = "0 10 * ? * 1";
		String expected = "40 * * * 0";
		Mockito.when(mockCronConverter.using(Mockito.anyString()))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.from(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.to(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.convert()).thenReturn(expected);
		assertEquals(expected,
				CronExpressionRetriever.getCronExpression(cronExpression));
	}

	@Test
	public void testQuartzToUnixDayOfWeek2() {
		String cronExpression = "0 10 * ? * 7";
		String expected = "40 * * * 6";
		Mockito.when(mockCronConverter.using(Mockito.anyString()))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.from(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.to(Mockito.any(ZoneId.class)))
				.thenReturn(mockCronConverter);
		Mockito.when(mockCronConverter.convert()).thenReturn(expected);
		assertEquals(expected,
				CronExpressionRetriever.getCronExpression(cronExpression));
	}

	@Test(expected = ServiceException.class)
	public void testUnixWithOutOfRange() {
		String cronExpression = "10 * * * 12";
		CronExpressionRetriever.getCronExpression(cronExpression);
	}

	@Test(expected = ServiceException.class)
	public void testUnixWithOutOfRange2() {
		String cronExpression = "10 * 32 * 12";
		CronExpressionRetriever.getCronExpression(cronExpression);
	}
}
