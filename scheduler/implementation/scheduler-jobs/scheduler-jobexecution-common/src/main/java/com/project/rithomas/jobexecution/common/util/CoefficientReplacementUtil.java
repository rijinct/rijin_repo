
package com.project.rithomas.jobexecution.common.util;

import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;

public class CoefficientReplacementUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CoefficientReplacementUtil.class);

	public static String replaceCoefficientPlaceholders(String sql,
			List<String> coefficientPlaceHolders) {
		for (String placeHolder : coefficientPlaceHolders) {
			String actualVal = null;
			LOGGER.debug("placeHolder : {}", placeHolder);
