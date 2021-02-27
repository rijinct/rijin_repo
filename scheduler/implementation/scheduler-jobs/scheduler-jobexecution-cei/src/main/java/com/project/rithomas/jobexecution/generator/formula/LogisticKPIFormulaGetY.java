
package com.project.rithomas.jobexecution.generator.formula;

import static org.apache.commons.lang.StringUtils.isNotEmpty;

import java.util.List;
import java.util.ListIterator;

import org.antlr.stringtemplate.StringTemplate;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.KPIFormulaConstants;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;

public class LogisticKPIFormulaGetY extends AbstractUserDefinedKPIFormula {

	private static final String TEMPLATE_NAME = "logisticKPIFormulaGetYTemplate";

	private static final String KPI_NAME_TEMPLATE_OBJECT = "KPINAME";

	@Override
	public String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig) {
		StringTemplate st = getRawTemplate(TEMPLATE_NAME);
		ListIterator<PerfIndiSpecFormulaProperty> iterator = properties
				.listIterator();
		String kpiName = null;
		while (iterator.hasNext()) {
			PerfIndiSpecFormulaProperty property = iterator.next();
			String propertyName = property.getpropertyName();
			if (isNotEmpty(propertyName)
					&& KPIFormulaConstants.TARGET_KPI.equals(propertyName)) {
				kpiName = property.getPropertyValue()
						.concat(PerfIndiSpecFormulaConstants.INT_SUFFIX);
				break;
			}
		}
		st.setAttribute(KPI_NAME_TEMPLATE_OBJECT, kpiName);
		return st.toString();
	}
}
