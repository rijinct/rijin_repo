
package com.project.rithomas.jobexecution.tnp;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.KpiUtil;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.common.ApplicabilityCode;
import com.project.rithomas.sdk.model.performance.PerformanceApplicability;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.ProfIndicatorMeaDuring;
import com.project.rithomas.sdk.model.performance.ProfileIndicatorSpec;
import com.project.rithomas.sdk.model.performance.query.PerformanceApplicabilityQuery;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ModelResources.class, ProfileCalculationJob.class,
		ProfileExecutionThread.class, ConnectionManager.class, KpiUtil.class, ApplicationLoggerFactory.class, HiveConfigurationProvider.class,
		DBConnectionManager.class })
public class ProfileCalculationJobTest {

	ProfileCalculationJob job = new ProfileCalculationJob();

	JobExecutionContext context = new JobExecutionContext();

	List<PerformanceIndicatorSpec> kpiList = new ArrayList<PerformanceIndicatorSpec>();

	List<ProfileIndicatorSpec> profileList = new ArrayList<ProfileIndicatorSpec>();

	@Mock
	EntityManager entityManager;

	@Mock
	TypedQuery<ProfileIndicatorSpec> typedQuery;

	@Mock
	UpdateJobStatus updateJobStatus;

	@Mock
	Connection con;

	@Mock
	Statement st;

	@Mock
	PreparedStatement pst;
	@Mock
	DBConnectionManager dbConnManager;
	@Mock
	UpdateBoundary updateBoundary;

	@Mock
	PerformanceApplicabilityQuery perfApplQuery;

	@Mock
	private HiveRowCountUtil rowCountUtil;
	
	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;

	@Before
	public void setup() throws Exception {
		mockStatic(ModelResources.class);
		mockStatic(ConnectionManager.class);
		mockStatic(HiveConfigurationProvider.class);
		mockStatic(DBConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		when(DBConnectionManager.getInstance()
				.getConnection(Mockito.anyString(), Mockito.anyBoolean()))
				.thenReturn(con);
		when(con.createStatement()).thenReturn(st);		
		when(con.prepareStatement(Mockito.anyString())).thenReturn(pst);
		when(ModelResources.getEntityManager()).thenReturn(entityManager);
		when(
				entityManager
						.createNamedQuery(
								ProfileIndicatorSpec.FIND_BY_DERIVATION_METHOD_ALGORITHM,
								ProfileIndicatorSpec.class)).thenReturn(
				typedQuery);
		when(
				typedQuery.setParameter(ProfileIndicatorSpec.DERIVATION_METHOD,
						"TEST")).thenReturn(typedQuery);
		profileList = getProfileList();
		when(typedQuery.getResultList()).thenReturn(profileList);
		List<String> hints = new ArrayList<String>();
		hints.add("CREATE temporary function test as 'com.project.rithomas.udf.Test'");
		hints.add("set mapred.reduce.tasks=4");
		PowerMockito.when(HiveConfigurationProvider.getInstance()).thenReturn(mockHiveConfigurationProvider);
		when(mockHiveConfigurationProvider.getQueryHints("PS_TNP_VOICE_1_15MIN_ProfileJob", schedulerConstants.HIVE_DATABASE))
				.thenReturn(hints);
		this.kpiList = this.getKPIList();
		mockStatic(KpiUtil.class);
		when(KpiUtil.getListOfAllKpis(Mockito.anyString(), Mockito.anyString()))
				.thenReturn(kpiList);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"PI_TNP_VOICE_PS_1_Z15MI_1_ProfileJob");
		context.setProperty(JobExecutionContext.LB, 1428915900000L);
		context.setProperty(JobExecutionContext.UB, 1428916080000L);
		context.setProperty(JobExecutionContext.NEXT_LB, 1428915990000L);
		context.setProperty(JobExecutionContext.PLEVEL, "15MIN");
		context.setProperty(ReConnectUtil.HIVE_RETRY_COUNT, 2);
		context.setProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL, 1000L);
		context.setProperty(
				JobExecutionContext.SQL,
