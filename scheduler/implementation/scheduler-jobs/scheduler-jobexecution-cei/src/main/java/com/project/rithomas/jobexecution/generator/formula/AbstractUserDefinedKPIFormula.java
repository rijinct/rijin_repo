
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.rijin.analytics.hierarchy.model.KPIIndexModel;
import com.rijin.analytics.hierarchy.model.WeightedAttribute;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.jobexecution.generator.util.KPIFormulaConstants;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;

public abstract class AbstractUserDefinedKPIFormula {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AbstractUserDefinedKPIFormula.class);

	private String interval = null;

	private static final String DELIMITER = "_";

	public void setInterval(String jobInterval) {
		interval = jobInterval;
	}

	public String getInterval() {
		return interval;
	}

	public abstract String getFormulaToCalculate(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException;

	protected static StringTemplate getRawTemplate(String templateName) {
		StringTemplateGroup templates = new StringTemplateGroup("Formula",
				getTemplateDirectory());
		return templates.getInstanceOf(templateName);
	}

	private static String getTemplateDirectory() {
		return SDKSystemEnvironment.getschedulerConfDirectory()
				+ KPIFormulaConstants.FILE_SEP
				+ KPIFormulaConstants.TEMPLATE_DIRECTORY;
	}

	protected static Map<String, String> getPropertyMap(
			List<PerfIndiSpecFormulaProperty> properties) {
		ListIterator<PerfIndiSpecFormulaProperty> iterator = properties
				.listIterator();
		Map<String, String> propertyMap = new HashMap<>();
		while (iterator.hasNext()) {
			PerfIndiSpecFormulaProperty property = iterator.next();
			propertyMap.put(property.getpropertyName(),
					property.getPropertyValue());
		}
		return propertyMap;
	}

	protected static String getKPINameConsideringXval(
			Map<String, String> propertyMap, String propertyValue) {
		if (propertyMap.containsKey(
				propertyValue.toUpperCase().concat(KPIFormulaConstants.XVAL))) {
			return propertyValue;
		} else {
			return propertyValue
					.concat(PerfIndiSpecFormulaConstants.INT_SUFFIX);
		}
	}

	public Double getWeightFromJSON(String kpiName,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		String kpi = trimSuffix(kpiName);
		Double weight = getWeightForKPI(kpi, kpiIndexConfig);
		LOGGER.info("Retrieved weight : {} for the kpi : {}", weight, kpi);
		return weight;
	}

	private Double getWeightForKPI(String kpiName,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		WeightedAttribute matchingAttribute = getKPIAttribites(kpiName,
				kpiIndexConfig);
		return Double.parseDouble(matchingAttribute.getWeight());
	}

	public WeightedAttribute getKPIAttribites(String kpiName,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		KPIIndexModel indexModel = kpiIndexConfig.getIndexModel();
		List<? extends WeightedAttribute> attributeList = kpiIndexConfig
				.getIsLeafIndex() ? indexModel.getAttributeList()
						: indexModel.getIndexList();
		WeightedAttribute matchingAttribute = attributeList.stream().parallel()
				.filter(e -> e.getName().equals(kpiName)).findFirst()
				.orElse(null);
		if (matchingAttribute == null) {
			throw new FormulaGeneratorException(
					"No matching attribute retrieved from JSON for the kpi :"
							+ kpiName);
		}
		return matchingAttribute;
	}

	public String trimSuffix(String kpiName) {
		String kpi = kpiName;
		if (StringUtils.endsWithAny(kpi,
				PerfIndiSpecFormulaConstants.INT_SUFFIX,
				PerfIndiSpecFormulaConstants.BUCKET_SUFFIX,
				PerfIndiSpecFormulaConstants.CONTRI_SUFFIX)) {
			kpi = kpi.substring(0, kpi.lastIndexOf(DELIMITER));
		}
		return kpi;
	}

	protected static String getKPINameConsideringXval(
			List<PerfIndiSpecFormulaProperty> properties,
			String propertyValue) {
		String kpiName = propertyValue;
		String xValKey = propertyValue.toUpperCase()
				.concat(KPIFormulaConstants.XVAL);
		for (PerfIndiSpecFormulaProperty property : properties) {
			if (xValKey.equalsIgnoreCase(property.getpropertyName())) {
				kpiName = property.getPropertyValue();
				break;
			}
		}
		return kpiName;
	}

	protected List<KPINameWeightObj> getKPIWeightObjList(
			List<PerfIndiSpecFormulaProperty> properties,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		List<KPINameWeightObj> kpiNameWeightObjList = new ArrayList<>();
		int sourceKPICount = getSourceKPICount(properties);
		int i = 1;
		for (PerfIndiSpecFormulaProperty property : properties) {
			String value = property.getPropertyValue();
			if (PerfIndiSpecFormulaConstants.KPI
					.equalsIgnoreCase(property.getpropertyName())) {
				kpiNameWeightObjList.add(new KPINameWeightObj(
						getKPINameConsideringXval(properties, value),
						getWeightFromJSON(value, kpiIndexConfig),
						(i != sourceKPICount)));
				i++;
			}
		}
		return kpiNameWeightObjList;
	}

	private int getSourceKPICount(
			List<PerfIndiSpecFormulaProperty> properties) {
		int sourceKPICount = 0;
		for (PerfIndiSpecFormulaProperty property : properties) {
			if (PerfIndiSpecFormulaConstants.KPI
					.equalsIgnoreCase(property.getpropertyName())) {
				sourceKPICount++;
			}
		}
		return sourceKPICount;
	}
}
