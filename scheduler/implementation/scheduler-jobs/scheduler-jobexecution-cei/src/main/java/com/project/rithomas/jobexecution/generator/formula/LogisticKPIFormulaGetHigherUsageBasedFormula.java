
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;

public class LogisticKPIFormulaGetHigherUsageBasedFormula
		extends AbstractUserDefinedKPIFormula {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(LogisticKPIFormulaGetHigherUsageBasedFormula.class);

	private static final String TEMPLATE_NAME = "logisticKPIFormulaGetHigherUsageWeight";

	private static final String USAGE_OBJ = "USAGE_OBJ";

	private boolean isUsageBasedNotEnabled = true;

	private List<String> srcKPINotUsageEnabledMap = null;

	private static final String SERVICE_WEIGHT_CASE = "(CASE WHEN ${TARGET_KPI} IS NULL THEN 0 ELSE ${SERVICE_WEIGHT} END)";

	private static final String HIGHER_LEVEL_DEFAULT_WEIGHT_CASE = "(CASE WHEN TARGET_KPI IS NULL THEN NULL ELSE 1 END)";

	@Override
	public String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig) {
		Map<String, String> usageServiceWeightMap = iterateOverPropertiesList(
				properties);
		if (isUsageBasedNotEnabled) {
			return getHigherLevelDefaultCaseValue(properties);
		}
		StringTemplate st = getRawTemplate(TEMPLATE_NAME);
		Map<String, String> propertyMap = getPropertyMap(properties);
		st.setAttribute(PerfIndiSpecFormulaConstants.TARGET_KPI,
				propertyMap.get(PerfIndiSpecFormulaConstants.TARGET_KPI));
		st.setAttribute(USAGE_OBJ,
				constructUsageServiceObjList(usageServiceWeightMap));
		String finalQuery = st.toString();
		StringBuilder replaceString = new StringBuilder();
		for (String notUsageEnableSrcKPI : srcKPINotUsageEnabledMap) {
			replaceString.delete(0, replaceString.length());
			replaceString.append("\\b").append(notUsageEnableSrcKPI)
					.append("\\b");
			finalQuery = finalQuery.replaceAll(replaceString.toString(), "1");
		}
		LOGGER.debug("Logistic formula for higher level usage weight : {}",
				finalQuery);
		return finalQuery;
	}

	private String getHigherLevelDefaultCaseValue(
			List<PerfIndiSpecFormulaProperty> properties) {
		String targetKpi = null;
		for (PerfIndiSpecFormulaProperty perfIndiSpecFormulaProperty : properties) {
			if (perfIndiSpecFormulaProperty.getpropertyName()
					.equals(PerfIndiSpecFormulaConstants.TARGET_KPI)) {
				targetKpi = perfIndiSpecFormulaProperty.getPropertyValue();
				break;
			}
		}
		return StringUtils.replace(HIGHER_LEVEL_DEFAULT_WEIGHT_CASE,
				PerfIndiSpecFormulaConstants.TARGET_KPI, targetKpi);
	}

	private List<UsageWeightObj> constructUsageServiceObjList(
			Map<String, String> usageServiceWeightMap) {
		Object[] usageServiceArr = usageServiceWeightMap.keySet().toArray();
		Object[] usageServiceVals = usageServiceWeightMap.values().toArray();
		List<Map<String, String>> listOfusageServiceWeightsMap = new ArrayList<Map<String, String>>();
		for (int i = 0; i < (1 << usageServiceWeightMap.size()); i++) {
			Map<String, String> usageServiceWeight = new LinkedHashMap<String, String>();
			for (int j = 0; j < usageServiceWeightMap.size(); j++) {
				if ((i & (1 << j)) > 0) {
					usageServiceWeight.put(usageServiceArr[j].toString(),
							usageServiceVals[j].toString());
				}
			}
			if (!usageServiceWeight.isEmpty()) {
				listOfusageServiceWeightsMap.add(usageServiceWeight);
			}
		}
		LOGGER.debug("listOfusageServiceWeightsMap : {}",
				listOfusageServiceWeightsMap);
		return constructUasgeWeightObjList(usageServiceWeightMap,
				listOfusageServiceWeightsMap);
	}

	private List<UsageWeightObj> constructUasgeWeightObjList(
			Map<String, String> usageServiceWeightMap,
			List<Map<String, String>> listOfusageServiceWeightsMap) {
		List<UsageWeightObj> usageWeightObjList = new ArrayList<UsageWeightObj>();
		listOfusageServiceWeightsMap = sortListBasedOnSize(
				listOfusageServiceWeightsMap, usageServiceWeightMap.size());
		for (int i = 0; i < listOfusageServiceWeightsMap.size(); i++) {
			UsageWeightObj usageWtObj = new UsageWeightObj(
					listOfusageServiceWeightsMap.get(i));
			usageWtObj.singleKPI = (listOfusageServiceWeightsMap.get(i)
					.size() == 1) ? true : false;
			usageWtObj.twoKPIs = (listOfusageServiceWeightsMap.get(i)
					.size() == 2) ? true : false;
			usageWeightObjList.add(usageWtObj);
		}
		LOGGER.debug("usage weight object list : {}", usageWeightObjList);
		return usageWeightObjList;
	}

	private List<Map<String, String>> sortListBasedOnSize(
			List<Map<String, String>> listOfMap, int size) {
		List<Map<String, String>> sortedList = new ArrayList<Map<String, String>>();
		do {
			for (Map<String, String> listOfInt : listOfMap) {
				if (size == listOfInt.size()) {
					sortedList.add(listOfInt);
				}
			}
			size--;
		} while (size != 0);
		LOGGER.debug("Sorted list based on size : {}", sortedList);
		return sortedList;
	}

	private Map<String, String> iterateOverPropertiesList(
			List<PerfIndiSpecFormulaProperty> propertyList) {
		List<String> srcKPIList = new ArrayList<String>();
		Map<String, String> srcKPIWeights = new HashMap<String, String>();
		List<String> xvalList = new ArrayList<String>();
		Map<String, String> srcKPIUsageEnabled = new HashMap<String, String>();
		for (PerfIndiSpecFormulaProperty property : propertyList) {
			if (PerfIndiSpecFormulaConstants.KPI
					.equalsIgnoreCase(property.getpropertyName())) {
				srcKPIList.add(property.getPropertyValue());
			} else if (property.getpropertyName()
					.contains(PerfIndiSpecFormulaConstants.WEIGHT)) {
				srcKPIWeights.put(property.getpropertyName(),
						property.getPropertyValue());
			} else if (property.getpropertyName()
					.contains(PerfIndiSpecFormulaConstants.XVAL_SEPARATOR)) {
				xvalList.add(property.getPropertyValue());
			} else if (property.getpropertyName()
					.contains(PerfIndiSpecFormulaConstants.SRC_USAGE_ENABLE)) {
				srcKPIUsageEnabled.put(property.getpropertyName().replace(
						PerfIndiSpecFormulaConstants.SRC_USAGE_ENABLE, ""),
						property.getPropertyValue());
			} else if (PerfIndiSpecFormulaConstants.USAGE_ENABLE
					.equalsIgnoreCase(property.getpropertyName())) {
				isUsageBasedNotEnabled = PerfIndiSpecFormulaConstants.NO_CONST
						.equalsIgnoreCase(property.getPropertyValue());
			}
		}
		LOGGER.debug("xvalList : {}", xvalList);
		LOGGER.debug("srcKPIUsageEnabled : {}", srcKPIUsageEnabled);
		return constructUsageServiceWeightMap(srcKPIList, srcKPIWeights,
				xvalList, srcKPIUsageEnabled);
	}

	private Map<String, String> constructUsageServiceWeightMap(
			List<String> srcKPIList, Map<String, String> srcKPIWeights,
			List<String> xvalList, Map<String, String> srcKPIUsageEnabled) {
		Map<String, String> usageServiceWeightMap = new LinkedHashMap<String, String>();
		srcKPINotUsageEnabledMap = new ArrayList<String>();
		for (String eachSrcKPI : srcKPIList) {
			String usageWeightKPI;
			if (xvalList.contains(eachSrcKPI)) {
				usageWeightKPI = eachSrcKPI
						.replace(PerfIndiSpecFormulaConstants.INT_SUFFIX, "")
						.concat(PerfIndiSpecFormulaConstants.WEIGHT_SUFFIX);
			} else {
				usageWeightKPI = eachSrcKPI
						.concat(PerfIndiSpecFormulaConstants.WEIGHT_SUFFIX);
			}
			if (srcKPIUsageEnabled.containsKey(eachSrcKPI) && "false"
					.equalsIgnoreCase(srcKPIUsageEnabled.get(eachSrcKPI))) {
				srcKPINotUsageEnabledMap.add(usageWeightKPI);
			}
			Map<String, String> serviceWeightKPIMap = new HashMap<String, String>();
			serviceWeightKPIMap.put(PerfIndiSpecFormulaConstants.TARGET_KPI,
					eachSrcKPI.replace(PerfIndiSpecFormulaConstants.INT_SUFFIX,
							""));
			serviceWeightKPIMap.put(PerfIndiSpecFormulaConstants.SERVICE_WEIGHT,
					srcKPIWeights.get(eachSrcKPI
							.concat(PerfIndiSpecFormulaConstants.WEIGHT)));
			usageServiceWeightMap.put(usageWeightKPI, StrSubstitutor
					.replace(SERVICE_WEIGHT_CASE, serviceWeightKPIMap));
		}
		LOGGER.debug("usage service weight map : {}", usageServiceWeightMap);
		return usageServiceWeightMap;
	}
}
