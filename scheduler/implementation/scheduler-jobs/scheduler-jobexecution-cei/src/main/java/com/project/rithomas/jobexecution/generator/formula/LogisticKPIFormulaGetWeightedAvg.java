
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.antlr.stringtemplate.StringTemplate;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.KPIFormulaConstants;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;
import com.project.rithomas.sdk.workflow.generator.util.FormulaGeneratorUtil;

public class LogisticKPIFormulaGetWeightedAvg
		extends AbstractUserDefinedKPIFormula {

	private static final String TEMPLATE_NAME = "logisticKPIFormulaGetWeightedAvgTemplate";

	private static final String KPI_PARAM_TEMPLATE_OBJECT = "KPI_OBJ";

	private static final String TARGET_KPI_TEMPLATE_OBJECT = "TARGET_KPI_BUCKET";

	List<KpiParamObj> kpiParamObjList = new ArrayList<>();

	@Override
	public String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		StringTemplate st = getRawTemplate(TEMPLATE_NAME);
		Map<String, String> propertyMap = getPropertyMap(properties);
		Map<String, Integer> getPropCount = FormulaGeneratorUtil
				.getPropCount(properties);
		int kpiCount = getPropCount.get(PerfIndiSpecFormulaConstants.KPI);
		ListIterator<PerfIndiSpecFormulaProperty> iterator = properties
				.listIterator();
		while (iterator.hasNext()) {
			PerfIndiSpecFormulaProperty property = iterator.next();
			String propertyName = property.getpropertyName();
			if (KPIFormulaConstants.KPI.equals(propertyName)) {
				kpiCount--;
				KpiParamObj kpiParamObj = new KpiParamObj();
				kpiParamObj.setSourceKpiInt(getKPINameConsideringXval(
						propertyMap, property.getPropertyValue()));
				if (kpiCount == 0) {
					kpiParamObj.setCheck(false);
				}
				kpiParamObj.setWeight(getWeightFromJSON(
						property.getPropertyValue(), kpiIndexConfig));
				kpiParamObjList.add(kpiParamObj);
			}
		}
		st.setAttribute(KPI_PARAM_TEMPLATE_OBJECT, kpiParamObjList);
		st.setAttribute(TARGET_KPI_TEMPLATE_OBJECT,
				propertyMap.get(PerfIndiSpecFormulaConstants.TARGET_KPI)
						.concat(PerfIndiSpecFormulaConstants.BUCKET_SUFFIX));
		return st.toString();
	}
}
