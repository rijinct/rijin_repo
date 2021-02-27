
package com.project.rithomas.jobexecution.aggregation;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.CoefficientReplacementUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.jobexecution.generator.formula.AbstractFormulaGenerator;
import com.project.rithomas.jobexecution.generator.formula.FormulaGeneratorFactory;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUse;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUseComparator;
import com.project.rithomas.sdk.model.performance.PerfSpecMeasUsing;
import com.project.rithomas.sdk.model.performance.PerfUsageSpecRel;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpecSequenceComparator;
import com.project.rithomas.sdk.model.performance.PerformanceSpecification;
import com.project.rithomas.sdk.model.performance.query.PerformanceIndicatorSpecQuery;
import com.project.rithomas.sdk.model.performance.query.PerformanceSpecificationQuery;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.deployment.util.DeployerConstants;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class AggregationUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AggregationUtil.class);

		Matcher matcher = pattern.matcher(sql);
		while (matcher.find()) {
			String subStr = matcher.group();
			LOGGER.debug("subStr : {}", subStr);
			if (!subStr.isEmpty() && !placeHolders.contains(subStr)) {
				placeHolders.add(subStr);
			}
		}
		try {
			sql = replaceOtherPlaceHolders(sql, placeHolders);
		} catch (JobExecutionException e) {
			context.setProperty(JobExecutionContext.STATUS, 'E');
			throw e;
		}
		matcher = pattern.matcher(sql);
		while (matcher.find()) {
			String subStr = matcher.group();
