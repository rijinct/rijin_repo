
package com.project.rithomas.jobexecution.common.util;

import static org.apache.commons.lang.StringUtils.defaultIfBlank;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.apache.commons.lang.StringUtils.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.deployment.util.ProcessBuilderHelper;

public class HiveToHBaseLoader {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveToHBaseLoader.class);

