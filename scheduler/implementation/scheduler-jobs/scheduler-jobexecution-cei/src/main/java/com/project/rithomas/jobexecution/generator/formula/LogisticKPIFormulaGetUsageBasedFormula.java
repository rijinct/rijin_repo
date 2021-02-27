
package com.project.rithomas.jobexecution.generator.formula;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.rijin.analytics.hierarchy.model.KPIUsageParameter;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;

public class LogisticKPIFormulaGetUsageBasedFormula
		extends AbstractUserDefinedKPIFormula {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(LogisticKPIFormulaGetUsageBasedFormula.class);

	private static final String FORMULA = "FORMULA";

	private static final String TEMPLATE_NAME = "logisticKPIFormulaGetUsageWeight";

	private static final String AVG_KPI_CASE = "CASE WHEN ${TARGET_KPI} IS NULL THEN NULL WHEN ${AVG_USAGE} IS NULL THEN ${DEFAULT_VALUE} ELSE (${FORMULA}) END";

	private String usageKPIValue;

	private String avgUsage;


	@Override
	public String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig) {
		Map<String, String> propertyMap = getPropertyMap(properties);
		if (PerfIndiSpecFormulaConstants.NO_CONST.equalsIgnoreCase(
				propertyMap.get(PerfIndiSpecFormulaConstants.USAGE_ENABLE))) {
			return "1";
		}
		StringTemplate st = getRawTemplate(TEMPLATE_NAME);
		st.setAttribute(PerfIndiSpecFormulaConstants.TARGET_KPI,
				propertyMap.get(PerfIndiSpecFormulaConstants.TARGET_KPI));
		st.setAttribute(FORMULA, getUsageFormula(propertyMap));
		st.setAttribute(PerfIndiSpecFormulaConstants.USAGE_KPI, usageKPIValue);
		st.setAttribute(PerfIndiSpecFormulaConstants.DEFAULT_VALUE, propertyMap
				.get(PerfIndiSpecFormulaConstants.USAGE_DEFAULT_VALUE));
		LOGGER.debug("Formula to get usage based formula : {}", st.toString());
		String finalFormula = st.toString();
		if (Pattern.compile("/[ ]*AVG_USAGE")
				.matcher(propertyMap
						.get(PerfIndiSpecFormulaConstants.USAGE_FORMULA))
				.find()) {
			Map<String, String> averageUsageFormulaMap = new HashMap<String, String>();
			averageUsageFormulaMap.put(PerfIndiSpecFormulaConstants.AVG_USAGE,
					avgUsage);
			averageUsageFormulaMap.put(
					PerfIndiSpecFormulaConstants.DEFAULT_VALUE, propertyMap.get(
							PerfIndiSpecFormulaConstants.USAGE_DEFAULT_VALUE));
			averageUsageFormulaMap.put(PerfIndiSpecFormulaConstants.TARGET_KPI,
					propertyMap.get(PerfIndiSpecFormulaConstants.TARGET_KPI));
			averageUsageFormulaMap.put(FORMULA, finalFormula);
			finalFormula = StrSubstitutor.replace(AVG_KPI_CASE,
					averageUsageFormulaMap);
			finalFormula = getUsageBasedWeights(kpiIndexConfig, finalFormula);
		}
		return finalFormula;
	}

	private String getUsageBasedWeights(KPIIndexConfiguration kpiIndexConfig,
			String finalFormula) {
		for (KPIUsageParameter usageParameters : kpiIndexConfig.getIndexModel()
				.getUsageBasedWeights().getUsageParameters()) {
			if (getInterval().equals(usageParameters.getInterval())) {
				finalFormula = finalFormula.replace(usageKPIValue,
						usageParameters.getFormula());
				finalFormula = finalFormula.replace(avgUsage,
						StringUtils.defaultIfEmpty(
								usageParameters.getAverageValue(), "null"));
			}
		}
		return finalFormula;
	}

	private String getUsageFormula(Map<String, String> propertyMap) {
		avgUsage = constructStr(
				propertyMap.get(PerfIndiSpecFormulaConstants.USAGE_KPI),
				PerfIndiSpecFormulaConstants.AVG_USAGE.concat("_"));
		usageKPIValue = constructStr(
				propertyMap.get(PerfIndiSpecFormulaConstants.USAGE_KPI),
				PerfIndiSpecFormulaConstants.US_BASE_WEIGHT);
		return propertyMap.get(PerfIndiSpecFormulaConstants.USAGE_FORMULA)
				.replace(PerfIndiSpecFormulaConstants.AVG_USAGE, avgUsage)
				.replace(PerfIndiSpecFormulaConstants.USAGE_KPI, usageKPIValue);
	}

	private String constructStr(String usageKPI, String appender) {
		return PerfIndiSpecFormulaConstants.HASH_SYM.concat(appender)
				.concat(usageKPI).concat(PerfIndiSpecFormulaConstants.HASH_SYM);
	}
}
