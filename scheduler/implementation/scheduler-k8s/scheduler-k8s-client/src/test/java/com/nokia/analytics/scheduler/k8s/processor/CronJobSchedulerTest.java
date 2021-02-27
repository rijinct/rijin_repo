
package com.rijin.analytics.scheduler.k8s.processor;

import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.CRON_EXPRESSION;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.stringtemplate.v4.ST;

import com.rijin.analytics.scheduler.beans.Cron;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.beans.Simple;
import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.k8s.client.K8sCronApi;
import com.rijin.analytics.scheduler.k8s.cron.CronExpressionRetriever;
import com.rijin.analytics.scheduler.k8s.cron.CronGenerator;
import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ CronJobscheduler.class, CronGenerator.class,
		CronExpressionRetriever.class })
public class CronJobschedulerTest {

	@Spy
	CronJobscheduler jobscheduler = new CronJobscheduler();

	@Mock
	private K8sCronApi mockK8sCronApi;

	@Mock
	private Call mockCall;

	@Mock
	private Trigger mockTrigger;

	@Mock
	private JobSchedulingData mockJobDetails;

	@Mock
	private Schedule mockSchedule;

	@Mock
	private ST mockTemplate;

	@Mock
	private CronGenerator mockCronGenerator;

	@Mock
	private Simple mockSimple;

	private List<Trigger> listOfTriggers = new ArrayList<>();

	@Mock
	private Cron mockCron;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		listOfTriggers.add(mockTrigger);
		mockStatic(CronExpressionRetriever.class);
	}

	@Test
	public void testGetErrorKey() {
		assertEquals("cronJobScheduleFailed", jobscheduler.getErrorKey());
	}

	@Test
	public void testGetCall() throws ApiException {
		doReturn(mockK8sCronApi).when(jobscheduler).getCronApiExecutor();
		doNothing().when(jobscheduler).overrideHeaders();
		doReturn(mockCall).when(mockK8sCronApi).patchCronCall("cronJobName",
				"cronPatch");
		assertEquals(mockCall,
				jobscheduler.getCall("cronJobName", "cronPatch"));
		verify(jobscheduler, times(1)).getCronApiExecutor();
		verify(jobscheduler, times(1)).overrideHeaders();
		verify(mockK8sCronApi, times(1)).patchCronCall(anyString(),
				anyString());
	}

	@Test
	public void testGetTemplateName() {
		assertEquals("schedule", jobscheduler.getTemplateName());
	}

	@Test
	public void testGetResourceGroup() {
		doReturn(mockJobDetails).when(jobscheduler).getJobDetails();
		doReturn(mockSchedule).when(mockJobDetails).getSchedule();
		doReturn(listOfTriggers).when(mockSchedule).getTrigger();
		assertEquals(listOfTriggers, jobscheduler.getResourceGroup());
		verify(jobscheduler, times(1)).getJobDetails();
		verify(mockJobDetails, times(1)).getSchedule();
		verify(mockSchedule, times(1)).getTrigger();
	}

	@Test
	public void testFillTemplateSTTrigger() {
		doReturn(true).when(mockTrigger).isCron();
		doReturn(mockCron).when(mockTrigger).getCron();
		doReturn("* * * * *").when(mockCron).getCronExpression();
		PowerMockito
				.when(CronExpressionRetriever.getCronExpression("* * * * *"))
				.thenReturn("* * * * *");
		jobscheduler.fillTemplate(mockTemplate, mockTrigger);
		verify(mockTrigger, times(1)).isCron();
		verify(mockTemplate, times(1)).add(CRON_EXPRESSION, "* * * * *");
	}

	@Test
	public void testFillTemplate() throws Exception {
		String startTime = "2012.11.02 11:15:00";
		String cronExpression = "123";
		whenNew(CronGenerator.class).withNoArguments()
				.thenReturn(mockCronGenerator);
		doReturn(false).when(mockTrigger).isCron();
		doReturn(mockSimple).when(mockTrigger).getSimple();
		doReturn(startTime).when(mockSimple).getStartTime();
		doReturn(cronExpression).when(mockCronGenerator).getCron(startTime);
		jobscheduler.fillTemplate(mockTemplate, mockTrigger);
		verify(mockTemplate, times(1)).add(CRON_EXPRESSION, cronExpression);
	}

	@Test
	public void testGetRequestKeyTrigger() {
		doReturn("sms-1-perf-sms-segg-1-hour-aggregatejob").when(jobscheduler)
				.getJobId(mockTrigger);
		assertEquals("sms-1-perf-sms-segg-1-hour-aggregatejob",
				jobscheduler.getRequestKey(mockTrigger));
		verify(jobscheduler, times(1)).getJobId(mockTrigger);
	}
}
