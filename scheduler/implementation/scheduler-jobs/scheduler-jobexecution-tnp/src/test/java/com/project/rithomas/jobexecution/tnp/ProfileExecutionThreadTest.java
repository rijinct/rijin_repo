
package com.project.rithomas.jobexecution.tnp;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.formula.exception.InvalidApplicabilityException;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.sdk.model.common.ApplicabilityCode;
import com.project.rithomas.sdk.model.performance.PerformanceApplicability;
import com.project.rithomas.sdk.model.performance.ProfIndicatorMeaDuring;
import com.project.rithomas.sdk.model.performance.ProfileIndicatorSpec;
import com.project.rithomas.sdk.model.performance.query.PerformanceApplicabilityQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ConnectionManager.class,
		ProfileExecutionThread.class, ApplicationLoggerFactory.class,
		ApplicationLoggerUtilInterface.class, HiveConfigurationProvider.class })
public class ProfileExecutionThreadTest {

	ProfileExecutionThread thread;

	@Mock
	Connection con;

	@Mock
	Statement st;

	@Mock
	UpdateBoundary updateBoundary;

	List<ProfileIndicatorSpec> profileList = new ArrayList<ProfileIndicatorSpec>();

	@Mock
	PerformanceApplicabilityQuery perfApplQuery;

	@Mock
	private HiveRowCountUtil rowCountUtil;
	
	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;

	@Before
	public void setUp() throws Exception {
		WorkFlowContext context = new JobExecutionContext();
		context.setProperty(JobExecutionContext.JOB_NAME,
				"PI_TNP_VOICE_PS_1_Z15MI_1_ProfileJob");
		context.setProperty(JobExecutionContext.LB, 1428915900000L);
		context.setProperty(JobExecutionContext.UB, 1428916080000L);
		context.setProperty(JobExecutionContext.NEXT_LB, 1428915990000L);
		context.setProperty(JobExecutionContext.PLEVEL, "15MIN");
		context.setProperty(ReConnectUtil.HIVE_RETRY_COUNT, 2);
		context.setProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL, 1000L);
		profileList = getProfileList();
		thread = new ProfileExecutionThread(Thread.currentThread(), context,
				"insert into profile_table select * from kpi_table", 1,
				profileList);
		mockStatic(ConnectionManager.class);
		mockStatic(HiveConfigurationProvider.class);
		when(ConnectionManager.getConnection(schedulerConstants.HIVE_DATABASE))
				.thenReturn(con);
		when(con.createStatement()).thenReturn(st);		
		List<String> hints = new ArrayList<String>();
		hints.add("create temporary function test as 'com.project.Test");
		hints.add("add jar /tmp/udf.jar");
		hints.add("set mapred.reduce.tasks=4");
		PowerMockito.when(HiveConfigurationProvider.getInstance()).thenReturn(mockHiveConfigurationProvider);
		when(mockHiveConfigurationProvider
						.getQueryHints("PI_TNP_VOICE_PS_1_Z15MI_1_ProfileJob", schedulerConstants.HIVE_DATABASE))
				.thenReturn(hints);
		whenNew(UpdateBoundary.class).withNoArguments().thenReturn(
				updateBoundary);
		whenNew(PerformanceApplicabilityQuery.class).withNoArguments()
				.thenReturn(perfApplQuery);
		whenNew(HiveRowCountUtil.class).withNoArguments().thenReturn(
				rowCountUtil);
		when(perfApplQuery.retrieve("TEST", null)).thenReturn(
				getPerformanceApplicability("00:00", "23:59", true));
	}

	@Mock
	ApplicationLoggerUtilInterface mockApplicationLogger;

	@Test
	public void testCall() throws Exception {
		PowerMockito.mockStatic(ApplicationLoggerFactory.class);
		PowerMockito.when(ApplicationLoggerFactory.getApplicationLogger())
				.thenReturn(mockApplicationLogger);
		ExecutorService service = Executors.newFixedThreadPool(1);
		Future f = service.submit(thread);
		while(!f.isDone()){
			Thread.sleep(10);
		}
	}

	private List<ProfileIndicatorSpec> getProfileList() {
		List<ProfileIndicatorSpec> profiles = new ArrayList<ProfileIndicatorSpec>();
		profiles.add(getProfile(1, "CALL_DROP_VOICE", "FIRST(CALL_DROP)",
				"00:00", "23:59", true));
		profiles.add(getProfile(2, "NUMBER_OF_CALLS_VOICE",
				"LAST(NUMBER_OF_CALLS)", "00:00", "23:59", true));
		return profiles;
	}

	private List<ProfileIndicatorSpec> getProfileListInvalidAppl() {
		List<ProfileIndicatorSpec> profiles = new ArrayList<ProfileIndicatorSpec>();
		ProfileIndicatorSpec profile = getProfile(2, "NUMBER_OF_CALLS_VOICE",
				"LAST(NUMBER_OF_CALLS)", "00:00", "23:59", false);
		profiles.add(profile);
		return profiles;
	}

	private ProfileIndicatorSpec getProfile(Integer id, String specId,
			String name, String startTime, String endTime,
			boolean isValidApplicability) {
		ProfileIndicatorSpec profile = new ProfileIndicatorSpec();
		profile.setId(id);
		profile.setIndicatorspecId(specId);
		profile.setPiSpecName(name);
		profile.setDerivationmethod(name);
		List<ProfIndicatorMeaDuring> profMeasDuringList = new ArrayList<ProfIndicatorMeaDuring>();
		ProfIndicatorMeaDuring profIndicatorMeaDuring = new ProfIndicatorMeaDuring();
		PerformanceApplicability perfApplicability = getPerformanceApplicability(
				startTime, endTime, isValidApplicability);
		profIndicatorMeaDuring.setPerfapplicabilityid(perfApplicability);
		profMeasDuringList.add(profIndicatorMeaDuring);
		profile.setProfIndicatorsMeaDuring(profMeasDuringList);
		return profile;
	}

	private PerformanceApplicability getPerformanceApplicability(
			String startTime, String endTime, boolean isValidApplicability) {
		PerformanceApplicability perfApplicability = new PerformanceApplicability();
		perfApplicability.setDays("1-7");
		if (isValidApplicability) {
			perfApplicability.setApplicabilityCode(ApplicabilityCode.APPLICABLE
					.name());
		} else {
			perfApplicability
					.setApplicabilityCode(ApplicabilityCode.UNAPPLICABLE.name());
		}
		perfApplicability.setStartTime(startTime);
		perfApplicability.setEndTime(endTime);
		perfApplicability.setPerfapplicabilityid("TEST");
		return perfApplicability;
	}

	@Test
	public void testCheckProfApplicabilityTrue()
			throws InvalidApplicabilityException {
		assertEquals(
				true,
				this.thread.checkProfileApplicability(profileList,
						System.currentTimeMillis()));
	}

	@Test
	public void testCheckProfApplicabilityFalse()
			throws InvalidApplicabilityException {
		when(perfApplQuery.retrieve("TEST", null)).thenReturn(
				getPerformanceApplicability("00:00", "23:59", false));
		List<ProfileIndicatorSpec> profileList = new ArrayList<ProfileIndicatorSpec>();
		profileList = this.getProfileListInvalidAppl();
		assertEquals(
				false,
				this.thread.checkProfileApplicability(profileList,
						System.currentTimeMillis()));
	}
}
