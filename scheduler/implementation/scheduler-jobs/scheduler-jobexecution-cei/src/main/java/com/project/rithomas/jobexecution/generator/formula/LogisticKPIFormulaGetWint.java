
package com.project.rithomas.jobexecution.generator.formula;

import java.util.List;
import java.util.Map;

import org.antlr.stringtemplate.StringTemplate;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;

public class LogisticKPIFormulaGetWint extends AbstractUserDefinedKPIFormula {

	private static final String TEMPLATE_NAME = "logisticKPIFormulaGetWintTemplate";

	private static final String INT_KPI_TEMPLATE_OBJECT = "KPI_INT";

	private static final String WEIGHT_TEMPLATE_OBJECT = "WEIGHT";

	private static final String TARGET_KPI_BUCKET_TEMPLATE_OBJECT = "TARGET_KPI_BUCKET";

	@Override
	public String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		StringTemplate st = getRawTemplate(TEMPLATE_NAME);
		Map<String, String> propertyMap = getPropertyMap(properties);
		st.setAttribute(INT_KPI_TEMPLATE_OBJECT,
				propertyMap.get(PerfIndiSpecFormulaConstants.KPI)
						.concat(PerfIndiSpecFormulaConstants.INT_SUFFIX));
		st.setAttribute(TARGET_KPI_BUCKET_TEMPLATE_OBJECT,
				propertyMap.get(PerfIndiSpecFormulaConstants.TARGET_KPI)
						.concat(PerfIndiSpecFormulaConstants.BUCKET_SUFFIX));
		st.setAttribute(WEIGHT_TEMPLATE_OBJECT,
				getWeightFromJSON(
						propertyMap.get(PerfIndiSpecFormulaConstants.KPI),
						kpiIndexConfig));
		return st.toString();
	}
}
