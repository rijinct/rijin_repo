
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import org.antlr.stringtemplate.StringTemplate;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.KPIFormulaConstants;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;

public class LogisticKPIFormulaBucketedWeight
		extends AbstractUserDefinedKPIFormula {

	private static final String TEMPLATE_NAME = "logisticKPIFormulaBucketedWeightTemplate";

	private static final String KPI_PARAM_TEMPLATE_OBJECT = "KpiObj";

	public static class KpiObj {

		private List<KpiParamObj> kpiList;

		private boolean check = true;

		private Boolean commonDenom = false;

		public List<KpiParamObj> getKpiList() {
			return Collections.unmodifiableList(kpiList);
		}

		public void setKpiList(List<KpiParamObj> kpiList) {
			kpiList = new ArrayList<>(kpiList);
			this.kpiList = Collections.unmodifiableList(kpiList);
		}

		public boolean isCheck() {
			return check;
		}

		public void setCheck(boolean check) {
			this.check = check;
		}

		public Boolean getCommonDenom() {
			return commonDenom;
		}

		public void setCommonDenom(Boolean commonDenom) {
			this.commonDenom = commonDenom;
		}
	}

	@Override
	public String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		StringTemplate st = getRawTemplate(TEMPLATE_NAME);
		Map<String, String> propertyMap = getPropertyMap(properties);
		Map<String, Integer> sameCommonDenomCountMap = getSameCommonDenomCountMap(
				propertyMap);
		KpiParamObj kpiParamObj = null;
		ListIterator<PerfIndiSpecFormulaProperty> iterator = properties
				.listIterator();
		Map<String, ArrayList<KpiParamObj>> kpiGroupingMap = new LinkedHashMap<>();
		while (iterator.hasNext()) {
			PerfIndiSpecFormulaProperty property = iterator.next();
			String propertyName = property.getpropertyName();
			if (KPIFormulaConstants.KPI.equals(propertyName)) {
				String kpiName = property.getPropertyValue();
				boolean isNext = true;
				if (propertyMap.containsKey(kpiName.concat(
						PerfIndiSpecFormulaConstants.COMMON_DENOM_SEPARATOR))) {
					commonDenominatorGrouping(propertyMap,
							sameCommonDenomCountMap, kpiGroupingMap, property,
							kpiName, isNext, kpiIndexConfig);
				} else {
					kpiParamObj = getKpiParamObj(propertyMap, property, false,
							kpiIndexConfig);
					ArrayList<KpiParamObj> kpiParamObjList = new ArrayList<>();
					kpiParamObjList.add(kpiParamObj);
					int kpiCnt = 0;
					kpiGroupingMap.put(kpiName + kpiCnt++, kpiParamObjList);
				}
			}
		}
		ArrayList<KpiObj> kpiObjListObjs = prepareKpiObjList(kpiGroupingMap);
		st.setAttribute(KPI_PARAM_TEMPLATE_OBJECT, kpiObjListObjs);
		return st.toString();
	}

	private ArrayList<KpiObj> prepareKpiObjList(
			Map<String, ArrayList<KpiParamObj>> kpiGroupingMap) {
		ArrayList<KpiObj> kpiObjListObjs = new ArrayList<>();
		int listSize = kpiGroupingMap.size();
		for (Entry<String, ArrayList<KpiParamObj>> value : kpiGroupingMap
				.entrySet()) {
			listSize--;
			KpiObj kpiObj = new KpiObj();
			ArrayList<KpiParamObj> kpiObjList = new ArrayList<>();
			kpiObjList.addAll(value.getValue());
			kpiObj.setKpiList(kpiObjList);
			if (kpiObjList.size() > 1) {
				kpiObj.setCommonDenom(true);
			}
			if (listSize == 0) {
				kpiObj.setCheck(false);
			}
			kpiObjListObjs.add(kpiObj);
		}
		return kpiObjListObjs;
	}

	private void commonDenominatorGrouping(Map<String, String> propertyMap,
			Map<String, Integer> sameCommonDenomCountMap,
			Map<String, ArrayList<KpiParamObj>> kpiGroupingMap,
			PerfIndiSpecFormulaProperty property, String kpiName,
			boolean isNext, KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		KpiParamObj kpiParamObj;
		String commonDenomKpiName = propertyMap.get(kpiName
				.concat(PerfIndiSpecFormulaConstants.COMMON_DENOM_SEPARATOR));
		sameCommonDenomCountMap.put(commonDenomKpiName,
				sameCommonDenomCountMap.get(commonDenomKpiName) - 1);
		if (kpiGroupingMap.containsKey(commonDenomKpiName)) {
			ArrayList<KpiParamObj> existingGrp = kpiGroupingMap
					.get(commonDenomKpiName);
			if (sameCommonDenomCountMap.get(commonDenomKpiName) == 0) {
				isNext = false;
			}
			kpiParamObj = getKpiParamObj(propertyMap, property, isNext,
					kpiIndexConfig);
			existingGrp.add(kpiParamObj);
			kpiGroupingMap.put(commonDenomKpiName, existingGrp);
		} else {
			if (sameCommonDenomCountMap.get(commonDenomKpiName) == 0) {
				isNext = false;
			}
			kpiParamObj = getKpiParamObj(propertyMap, property, isNext,
					kpiIndexConfig);
			ArrayList<KpiParamObj> kpiParamObjList = new ArrayList<>();
			kpiParamObjList.add(kpiParamObj);
			kpiGroupingMap.put(commonDenomKpiName, kpiParamObjList);
		}
	}

	private Map<String, Integer> getSameCommonDenomCountMap(
			Map<String, String> propertyMap) {
		HashMap<String, Integer> sameCommonDenomCountMap = new HashMap<>();
		for (Entry<String, String> value : propertyMap.entrySet()) {
			if (value.getKey().contains(
					PerfIndiSpecFormulaConstants.COMMON_DENOM_SEPARATOR)) {
				if (sameCommonDenomCountMap.containsKey(value.getValue())) {
					sameCommonDenomCountMap.put(value.getValue(),
							sameCommonDenomCountMap.get(value.getValue()) + 1);
				} else {
					int count = 1;
					sameCommonDenomCountMap.put(value.getValue(), count);
				}
			}
		}
		return sameCommonDenomCountMap;
	}

	private KpiParamObj getKpiParamObj(Map<String, String> propertyMap,
			PerfIndiSpecFormulaProperty property, boolean islast,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		KpiParamObj kpiParamObj = new KpiParamObj();
		kpiParamObj.setSourceKpiInt(getKPINameConsideringSrcKPIType(propertyMap,
				property.getPropertyValue()));
		kpiParamObj.setCheck(islast);
		kpiParamObj.setWeight(
				getWeightFromJSON(property.getPropertyValue(), kpiIndexConfig));
		return kpiParamObj;
	}

	private static String getKPINameConsideringSrcKPIType(
			Map<String, String> propertyMap, String propertyValue) {
		if (PerfIndiSpecFormulaConstants.GET_INT.equalsIgnoreCase(
				propertyMap.get(PerfIndiSpecFormulaConstants.TARGET_KPI_TYPE))
				&& !propertyMap.containsKey(
						propertyValue.concat(KPIFormulaConstants.XVAL))) {
			return propertyValue
					.concat(PerfIndiSpecFormulaConstants.INT_SUFFIX);
		} else {
			return propertyValue;
		}
	}
}
