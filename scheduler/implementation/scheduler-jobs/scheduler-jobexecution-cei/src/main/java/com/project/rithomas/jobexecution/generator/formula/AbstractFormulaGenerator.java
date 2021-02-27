
package com.project.rithomas.jobexecution.generator.formula;

import static org.apache.commons.lang.StringUtils.isNotEmpty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.persistence.EntityManager;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.apache.commons.lang.StringUtils;

import com.rijin.analytics.hierarchy.exception.IndexConfigRetrieverException;
import com.rijin.analytics.hierarchy.model.KPIAttributeList;
import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.rijin.analytics.hierarchy.model.KPIIndexList;
import com.rijin.analytics.hierarchy.model.KPIIndexModel;
import com.rijin.analytics.hierarchy.model.WeightedAttribute;
import com.rijin.analytics.hierarchy.service.KPIHierarchyIndexConfigRetreiver;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.jobexecution.generator.util.KPIFormulaConstants;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUse;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.PerformanceSpecInterval;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormula;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperties;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulae;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;
import com.thoughtworks.xstream.XStream;

public abstract class AbstractFormulaGenerator {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AbstractFormulaGenerator.class);

	public Map<String, List<String>> commonDimMap;

	public Boolean isUnionJoin;

	public List<PerfSpecAttributesUse> currentPerfSPecMeasUsing;

	public PerformanceSpecInterval interval;

	public List<PerfSpecAttributesUse> srcKPIs;

	public String jobName;

	public KPIHierarchyIndexConfigRetreiver kpiConfigRetriever;

	public List<String> logisticKpis = new ArrayList<String>();

	List<KPIAttributeList> attributeList = new ArrayList<KPIAttributeList>();

	List<KPIIndexList> indexList = new ArrayList<KPIIndexList>();

	public String getFormula(
			PerformanceIndicatorSpec performanceIndicatorSpecification)
			throws FormulaGeneratorException {
		String derivationMethod = performanceIndicatorSpecification
				.getDerivationmethod();
		String derivationAlgorithm = performanceIndicatorSpecification
				.getDerivationalgorithm();
		logCommonDimension(commonDimMap);
		String resultantFormula = null;
		EntityManager entityManager = null;
		try {
			LOGGER.debug("Derivation algorithm: {}, Derivation method: {}",
					derivationAlgorithm, derivationMethod);
			if (derivationMethod.contains("$")) {
				XStream xstream = new XStream();
				xstream.processAnnotations(PerfIndiSpecFormula.class);
				xstream.processAnnotations(PerfIndiSpecFormulae.class);
				if (derivationAlgorithm != null
						&& derivationAlgorithm.trim().contains("<Formula")
						&& derivationAlgorithm.trim().contains("</Formula>")) {
					if (kpiConfigRetriever == null) {
						LOGGER.info(
								"Invoking kpi-index-config-parser component");
						kpiConfigRetriever = initializeKPIHierarchyIndexConfigRetreiver();
					}
					resultantFormula = getResultantFormula(
							performanceIndicatorSpecification, entityManager,
							xstream);
				} else {
					throw new FormulaGeneratorException(
							"Derivation algorithm is either null or invalid (should start with \"Formula\" tag.) ");
				}
			} else {
				LOGGER.debug(
						"Derivation method didn't have any variables enclosed in $ to be replaced. "
								+ "Hence returning the method as the resultant formula.");
				resultantFormula = derivationMethod.replace("''", "'");
			}
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			throw new FormulaGeneratorException(
					"Error while instantiating the KPIFormula class: "
							+ e.getMessage(),
					e);
		} catch (FormulaGeneratorException e) {
			throw new FormulaGeneratorException(
					"Error while generating formula: " + e.getMessage(), e);
		} catch (IndexConfigRetrieverException e) {
			throw new FormulaGeneratorException(
					"Error while retrieving the index configuration : "
							+ e.getMessage(),
					e);
		} finally {
			ModelResources.releaseEntityManager(entityManager);
		}
		LOGGER.debug("Resultant formula: {}", resultantFormula);
		return resultantFormula;
	}

	private static KPIHierarchyIndexConfigRetreiver initializeKPIHierarchyIndexConfigRetreiver()
			throws IndexConfigRetrieverException {
		KPIHierarchyIndexConfigRetreiver kpiHierarchyIndexConfigRetreiver = new KPIHierarchyIndexConfigRetreiver();
		kpiHierarchyIndexConfigRetreiver.initialize();
		return kpiHierarchyIndexConfigRetreiver;
	}

	private static void logCommonDimension(
			Map<String, List<String>> commonDimensionMap) {
		if (commonDimensionMap != null && !commonDimensionMap.isEmpty()) {
			LOGGER.debug("Common dimensions : {}", commonDimensionMap);
		}
	}

	private String getResultantFormula(
			PerformanceIndicatorSpec performanceIndicatorSpecification,
			EntityManager entityManager, XStream xstream)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, FormulaGeneratorException {
		String derivationAlgorithm = performanceIndicatorSpecification
				.getDerivationalgorithm();
		String st = "";
		derivationAlgorithm = "<Formulae>" + derivationAlgorithm
				+ "</Formulae>";
		PerfIndiSpecFormulae perfIndiSpecFormulae = (PerfIndiSpecFormulae) xstream
				.fromXML(derivationAlgorithm);
		List<PerfIndiSpecFormula> perfIndiSpecFormulas = perfIndiSpecFormulae
				.getPerfIndiSpecFormula();
		entityManager = ModelResources.getEntityManager();
		if (perfIndiSpecFormulas != null && !perfIndiSpecFormulas.isEmpty()) {
			for (PerfIndiSpecFormula perfIndiSpecFormula : perfIndiSpecFormulas) {
				st = getFinalizedTemplate(performanceIndicatorSpecification,
						entityManager, perfIndiSpecFormula);
			}
		}
		return st;
	}

	private String getFinalizedTemplate(
			PerformanceIndicatorSpec performanceIndicatorSpecification,
			EntityManager entityManager,
			PerfIndiSpecFormula perfIndiSpecFormula)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, FormulaGeneratorException {
		KPIIndexConfiguration kpiIndexConfig = null;
		String defaultFormula = null;
		String formulaId = perfIndiSpecFormula.getFormulaID().trim()
				.toUpperCase();
		List<PerfIndiSpecFormula> formulae = entityManager
				.createNamedQuery(PerfIndiSpecFormula.FIND_BY_FORMULAID,
						PerfIndiSpecFormula.class)
				.setParameter(PerfIndiSpecFormula.FORMULA_ID, formulaId)
				.getResultList();
		String ref = perfIndiSpecFormula.getReference();
		StringTemplate st = null;
		String templateStr = "";
		if (formulae != null && !formulae.isEmpty()) {
			StringTemplate kpiFormulaTemplate = getRawTemplate();
			String derivationMethod = performanceIndicatorSpecification
					.getDerivationmethod();
			st = new StringTemplate(derivationMethod);
			List<PerfIndiSpecFormulaProperties> perfIndiSpecFormulaPropertiesList = perfIndiSpecFormula
					.getPerfIndiSpecFormulaProperties();
			List<PropertiesMapping> propertyMappingList = new ArrayList<>();
			if (perfIndiSpecFormulaPropertiesList != null
					&& !perfIndiSpecFormulaPropertiesList.isEmpty()) {
				String targetKPI = getTargetKpi(
						perfIndiSpecFormulaPropertiesList);
				if (isNotEmpty(targetKPI)) {
					kpiIndexConfig = kpiConfigRetriever
							.getKPIResponse(targetKPI);
					if (kpiIndexConfig != null) {
						LOGGER.debug("JSON fetched for {} is {}", targetKPI,
								kpiIndexConfig.toString());
						List<String> dimensions = populateDimensions(
								kpiIndexConfig);
						LOGGER.info("Fetched dimensions for {} are {}",
								targetKPI, dimensions);
						PerfIndiSpecFormulaProperties defaultperfIndiSpecFormulaProperties = getDefaultProperty(
								perfIndiSpecFormulaPropertiesList);
						attributeList = kpiIndexConfig.getIndexModel()
								.getAttributeList();
						indexList = kpiIndexConfig.getIndexModel()
								.getIndexList();
						for (String dimension : dimensions) {
							if (PerfIndiSpecFormulaConstants.DEFAULT
									.equals(dimension)) {
								defaultFormula = getDefaultFormula(
										performanceIndicatorSpecification,
										formulaId,
										defaultperfIndiSpecFormulaProperties,
										customizeJSON(kpiIndexConfig,
												dimension));
							} else {
								propertyMappingList = getDimensionBasedFormula(
										performanceIndicatorSpecification,
										formulaId,
										defaultperfIndiSpecFormulaProperties,
										propertyMappingList,
										customizeJSON(kpiIndexConfig,
												dimension),
										dimension);
							}
						}
						reLoadJSONWithAttributeList(kpiIndexConfig);
					} else {
						throw new FormulaGeneratorException(
								"JSON does not contain entry for "
										.concat(targetKPI)
										+ ". Check if the Portal Service is up");
					}
				} else {
					throw new FormulaGeneratorException(
							"Error loading JSON. Target KPI cannot be null.");
				}
				templateStr = replaceTemplateVariable(kpiFormulaTemplate, st,
						ref, defaultFormula, propertyMappingList);
			}
		} else {
			throw new FormulaGeneratorException(
					"Formula with ID: " + formulaId + " not defined. ");
		}
		return templateStr;
	}

	private KPIIndexConfiguration customizeJSON(
			KPIIndexConfiguration kpiIndexConfig, String dimension) {
		reLoadJSONWithAttributeList(kpiIndexConfig);
		LOGGER.info("isLeafIndex:true hence customizing JSON");
		List<WeightedAttribute> attributeList = new ArrayList<WeightedAttribute>();
		for (WeightedAttribute attribute : kpiIndexConfig.getIsLeafIndex()
				? kpiIndexConfig.getIndexModel().getAttributeList()
				: kpiIndexConfig.getIndexModel().getIndexList()) {
			if (attribute.getWeight() != null) {
				WeightedAttribute newAttribute = kpiIndexConfig.getIsLeafIndex()
						? new KPIAttributeList() : new KPIIndexList();
				newAttribute.setName(attribute.getName());
				LOGGER.debug(
						"Customizing attribute values for attribute {} based on dimension based string {}",
						attribute.getName(), attribute.getWeight());
				LOGGER.debug("Customizing weight for dimension : {} ",
						dimension, attribute.getName());
				newAttribute.setWeight(getDimensionBasedValue(dimension,
						attribute.getWeight()));
				LOGGER.debug("Customizing UT for dimension {}", dimension);
				if (kpiIndexConfig.getIsLeafIndex()) {
					((KPIAttributeList) newAttribute)
							.setUpperThreshold(getDimensionBasedValue(dimension,
									((KPIAttributeList) attribute)
											.getUpperThreshold()));
					LOGGER.debug("Customizing LT for dimension {}", dimension);
					((KPIAttributeList) newAttribute)
							.setLowerThreshold(getDimensionBasedValue(dimension,
									((KPIAttributeList) attribute)
											.getLowerThreshold()));
					LOGGER.debug("Customizing TV for dimension {}", dimension);
					((KPIAttributeList) newAttribute)
							.setThresholdValue(getDimensionBasedValue(dimension,
									((KPIAttributeList) attribute)
											.getThresholdValue()));
					((KPIAttributeList) newAttribute).setContType(
							((KPIAttributeList) attribute).getContType());
				}
				attributeList.add(newAttribute);
			}
		}
		setIndexModel(kpiIndexConfig, attributeList);
		return kpiIndexConfig;
	}

	private void setIndexModel(KPIIndexConfiguration kpiIndexConfig,
			List<? extends WeightedAttribute> attributeList) {
		KPIIndexModel indexModel = kpiIndexConfig.getIndexModel();
		if (kpiIndexConfig.getIsLeafIndex()) {
			indexModel.setAttributeList((List<KPIAttributeList>) attributeList);
		} else {
			indexModel.setIndexList((List<KPIIndexList>) attributeList);
		}
	}

	private void reLoadJSONWithAttributeList(
			KPIIndexConfiguration kpiIndexConfig) {
		if (attributeList != null) {
			kpiIndexConfig.getIndexModel().setAttributeList(attributeList);
		}
		if (indexList != null) {
			kpiIndexConfig.getIndexModel().setIndexList(indexList);
		}
	}

	private String getDimensionBasedValue(String dimension,
			String weightAndThreshold) {
		String attributeValue = null;
		if (weightAndThreshold != null) {
			if (PerfIndiSpecFormulaConstants.DEFAULT.equals(dimension)) {
				attributeValue = getDefaultValue(weightAndThreshold);
			} else {
				LOGGER.debug(
						"Dimesnion {} case and string from value to be fetched is {}",
						dimension, weightAndThreshold);
		for (String value : values) {
			if (!Pattern.matches(".*[a-zA-Z].*", value.replace(":", ""))) {
				attributeValue = value.replace(":", "");
			}
		}
		return attributeValue;
	}

	private List<String> populateDimensions(
			KPIIndexConfiguration kpiIndexConfig) {
		Set<String> dimensions = new HashSet<String>();
		LOGGER.debug("Fetching all dimensions");
		List<? extends WeightedAttribute> attributeList = kpiIndexConfig
				.getIsLeafIndex()
						? kpiIndexConfig.getIndexModel().getAttributeList()
						: kpiIndexConfig.getIndexModel().getIndexList();
		for (WeightedAttribute attribute : attributeList) {
			if (isNotEmpty(attribute.getWeight())) {
				LOGGER.debug("Trying to fetch dimensions from {}",
						attribute.getWeight());
