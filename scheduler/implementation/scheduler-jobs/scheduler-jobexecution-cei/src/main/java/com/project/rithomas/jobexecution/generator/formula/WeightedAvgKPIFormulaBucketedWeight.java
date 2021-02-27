
package com.project.rithomas.jobexecution.generator.formula;

import java.util.List;

import org.antlr.stringtemplate.StringTemplate;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;

public class WeightedAvgKPIFormulaBucketedWeight
		extends AbstractUserDefinedKPIFormula {

	private static final String TEMPLATE_NAME = "weightedAvgKPIFormulaGetBucketedWeight";

	private static final String KPI_WEIGHT_OBJ = "KPI_WEIGHT_OBJ";

	@Override
	public String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		StringTemplate st = getRawTemplate(TEMPLATE_NAME);
		st.setAttribute(KPI_WEIGHT_OBJ,
				getKPIWeightObjList(properties, kpiIndexConfig));
		return st.toString();
	}
}
