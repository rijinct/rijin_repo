
package com.project.rithomas.jobexecution.generator.formula;

import java.util.List;
import java.util.Map;

import org.antlr.stringtemplate.StringTemplate;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;
import com.project.rithomas.sdk.workflow.generator.formula.WeightedAvgKPIFormula;

public class WeightedAvgKPIFromulaGetIndex
		extends AbstractUserDefinedKPIFormula {

	private static final String TEMPLATE_NAME = "weightedAvgKPIFormulaGetIndex";

	private static final String KPI_WEIGHT_OBJ = "KPI_WEIGHT_OBJ";

	private static final String KPI_BUCKET = "KPI_BUCKET";

	@Override
	public String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		StringTemplate st = getRawTemplate(TEMPLATE_NAME);
		Map<String, String> propertyMap = WeightedAvgKPIFormula
				.getPropertyMap(properties);
		st.setAttribute(KPI_BUCKET,
				propertyMap.get(PerfIndiSpecFormulaConstants.TARGET_KPI)
						.concat(PerfIndiSpecFormulaConstants.BUCKET_SUFFIX));
		st.setAttribute(KPI_WEIGHT_OBJ,
				getKPIWeightObjList(properties, kpiIndexConfig));
		return st.toString();
	}
}
