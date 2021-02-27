
package com.project.rithomas.jobexecution.tnp;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.rithomas.applicability.Applicability;
import com.project.rithomas.formula.exception.InvalidApplicabilityException;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.util.ApplicabilityCheckUtil;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.KpiUtil;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.sdk.model.performance.PerformanceApplicability;
import com.project.rithomas.sdk.model.performance.ProfIndicatorMeaDuring;
import com.project.rithomas.sdk.model.performance.ProfileIndicatorSpec;
import com.project.rithomas.sdk.model.performance.query.PerformanceApplicabilityQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.quartzdesk.api.agent.log.WorkerThreadLoggingInterceptorRegistry;

public class ProfileExecutionThread implements Callable<Boolean> {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ProfileExecutionThread.class);

	private Map<ProfileIndicatorSpec, Boolean> profileApplicableMap = new HashMap<ProfileIndicatorSpec, Boolean>();

	private WorkFlowContext context;

	private String sql;

	private Integer profileInterval;

	private List<ProfileIndicatorSpec> profileList;

