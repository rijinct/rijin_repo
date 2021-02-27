
package com.project.rithomas.jobexecution.generator.formula;

import java.util.List;
import java.util.Map;

import org.antlr.stringtemplate.StringTemplate;

import com.rijin.analytics.hierarchy.model.KPIAttributeList;
import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;

public class LogisticKPIFormulaGetThreshold extends AbstractUserDefinedKPIFormula {

	private static final String TEMPLATE_NAME = "logisticKPIFormulaGetThresholdTemplate";

	private static final String KPI_TEMPLATE_OBJECT = "KPI_NAME";

	private static final String UT_TEMPLATE_OBJECT = "UT";

	private static final String LT_TEMPLATE_OBJECT = "LT";

	private static final String TV_TEMPLATE_OBJECT = "TV";

	private static final String KPI_TYPE_TEMPLATE_OBJECT = "GREATER_THE_BETTER";

	@Override
	public String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		Double thresholdValue = 0.0;
		StringTemplate st = getRawTemplate(TEMPLATE_NAME);
		Map<String, String> propertyMap = getPropertyMap(properties);
		String kpiName = propertyMap.get(PerfIndiSpecFormulaConstants.KPI);
		KPIAttributeList kpiAtrributes = (KPIAttributeList) getKPIAttribites(
				kpiName, kpiIndexConfig);
		Double lowerthreshold = Double
				.parseDouble(kpiAtrributes.getLowerThreshold());
		Double upperThreshold = Double
				.parseDouble(kpiAtrributes.getUpperThreshold());
		String thresholdValueKey = kpiName
				.concat(PerfIndiSpecFormulaConstants.TV_CONST_SEPARATOR);
		if (propertyMap.containsKey(thresholdValueKey)) {
			thresholdValue = Double
					.parseDouble(kpiAtrributes.getThresholdValue());
		}
		if (PerfIndiSpecFormulaConstants.GREATER_THE_BETTER
				.equals(propertyMap.get(kpiName.concat(
						PerfIndiSpecFormulaConstants.KPI_TYPE_SEPARATOR)))) {
			st.setAttribute(KPI_TYPE_TEMPLATE_OBJECT, true);
		}
		st.setAttribute(KPI_TEMPLATE_OBJECT, kpiName);
		st.setAttribute(UT_TEMPLATE_OBJECT, upperThreshold);
		st.setAttribute(LT_TEMPLATE_OBJECT, lowerthreshold);
		st.setAttribute(TV_TEMPLATE_OBJECT, thresholdValue);
		return st.toString();
	}
}
