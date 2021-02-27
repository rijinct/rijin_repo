
package com.project.rithomas.jobexecution.generator.formula;

import java.util.List;
import java.util.Map;

import org.antlr.stringtemplate.StringTemplate;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;

public class LogisticKPIFormulaGetCont extends AbstractUserDefinedKPIFormula {

	private static final String TEMPLATE_NAME = "logisticKPIFormulaGetContTemplate";

	private static final String INT_KPI_TEMPLATE_OBJECT = "KPI_INT";

	private static final String KPI_BUCKET = "KPI_BUCKET";

	private static final String KPI_WEIGHT = "KPI_WEIGHT";

	@Override
	public String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		StringTemplate st = getRawTemplate(TEMPLATE_NAME);
		Map<String, String> propertyMap = getPropertyMap(properties);
		st.setAttribute(INT_KPI_TEMPLATE_OBJECT,
				getKPINameConsideringXval(propertyMap,
						propertyMap.get(PerfIndiSpecFormulaConstants.KPI)));
		st.setAttribute(KPI_BUCKET,
				propertyMap.get(PerfIndiSpecFormulaConstants.TARGET_KPI)
						.concat(PerfIndiSpecFormulaConstants.BUCKET_SUFFIX));
		st.setAttribute(KPI_WEIGHT,
				getWeightFromJSON(
						propertyMap.get(PerfIndiSpecFormulaConstants.KPI),
						kpiIndexConfig));
		return st.toString();
	}
}
