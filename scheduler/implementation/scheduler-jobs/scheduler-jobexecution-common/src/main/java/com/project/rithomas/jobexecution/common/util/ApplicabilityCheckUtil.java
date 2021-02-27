
package com.project.rithomas.jobexecution.common.util;

import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.applicability.Applicability;
import com.project.rithomas.applicability.ApplicabilityCode;

public class ApplicabilityCheckUtil {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ApplicabilityCheckUtil.class);

	private static final boolean DEBUGMODE = LOGGER.isDebugEnabled();

	private static final String UNAPPLICABLE = "UNAPPLICABLE";

	private static final String APPLICABLE = "APPLICABLE";

	public static final boolean isApplicable(
			List<Applicability> applicabilities, Long lbValue) {
		boolean isApplicable = true;
		if (applicabilities != null && !applicabilities.isEmpty()) {
			String codeApplicable = UNAPPLICABLE;
			boolean allApplicable = false;
			for (Applicability applicability : applicabilities) {
				if (applicability != null && applicability
						.getApplicabilityCode() == ApplicabilityCode.APPLICABLE) {
					allApplicable = true;
					break;
				}
			}
			if (allApplicable) {
				codeApplicable = APPLICABLE;
			}
			isApplicable = validate(codeApplicable, lbValue, applicabilities);
		} else {
			isApplicable = true;
		}
		return isApplicable;
	}

	private static boolean validate(String codeApplicable, Long lowerBound,
			List<Applicability> applicabilities) {
		boolean isApplicable = APPLICABLE.equals(codeApplicable) ? false : true;
		for (Applicability applicability : applicabilities) {
			// What is being checked in the condition
			if (applicability.isApplicable(lowerBound)) {
				if (APPLICABLE.equals(codeApplicable)) {
					isApplicable = true;
					break;
				}
			} else {
				if (UNAPPLICABLE.equals(codeApplicable)) {
					isApplicable = false;
					break;
				}
			}
		}
		if (DEBUGMODE) {
			LOGGER.debug(" Is kpi/profile Applicable : {}", isApplicable);
		}
		return isApplicable;
	}
}
