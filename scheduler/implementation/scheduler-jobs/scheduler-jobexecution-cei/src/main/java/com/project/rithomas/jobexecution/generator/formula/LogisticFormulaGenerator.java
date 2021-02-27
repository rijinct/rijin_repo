
package com.project.rithomas.jobexecution.generator.formula;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.applicationbeans.ApplicationContextLoader;
import com.project.rithomas.jobexecution.applicationbeans.LogisticKPIFormulaTypeProvider;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.jobexecution.generator.util.FormulaGeneratorUtil;
import com.project.rithomas.jobexecution.generator.util.UserDefinedKPIGeneratorUtil;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;

public class LogisticFormulaGenerator
		extends AbstractUserDefinedFormulaGenerator {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(LogisticFormulaGenerator.class);

	KPIIndexConfiguration kpiAttributesMap;

	public String getFormula(
			PerformanceIndicatorSpec performanceIndicatorSpecification,
			Map<String, Object> nameFormulaMapping)
			throws FormulaGeneratorException {
		String formula = super.getFormula(performanceIndicatorSpecification);
		addKPIFormulaEntriesForINTnBUCKET(performanceIndicatorSpecification,
				nameFormulaMapping);
		return formula;
	}

	@Override
	public String getGenericInputStructure(
			PerformanceIndicatorSpec performanceIndicatorSpecification,
			List<PerfIndiSpecFormulaProperty> properties, String kpiFormulaID,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		setPerfIndiSpecDetailsFromContext();
		setTargetKPITypeIfINTKpi(properties, performanceIndicatorSpecification);
		setTargetKPITypeForBucketKpi(properties);
		setTargetKPITypeIfUsageWeightKPI(properties);
		String formula = "";
		try {
			formula = getFormulaToCalculate(properties, kpiIndexConfig);
		} catch (Exception e) {
			LOGGER.error("Error generating formula: {}", e);
			throw new FormulaGeneratorException(
					"Exception while generating formula :", e);
		}
		return formula;
	}

	private void setTargetKPITypeForBucketKpi(
			List<PerfIndiSpecFormulaProperty> properties) {
		String propValue = null;
		if (isBucket(properties)) {
			for (PerfIndiSpecFormulaProperty property : properties) {
				if (PerfIndiSpecFormulaConstants.KPI
						.equals(property.getpropertyName())) {
					PerformanceIndicatorSpec srcKPI = getSrcKpi(
							property.getPropertyValue());
					if (FormulaGeneratorUtil.isUserDefinedFormula(
							srcKPI.getDerivationmethod(),
							srcKPI.getDerivationalgorithm())
							&& UserDefinedKPIGeneratorUtil
									.islogisticKPI(srcKPI)) {
						propValue = PerfIndiSpecFormulaConstants.GET_INT;
					} else {
						propValue = PerfIndiSpecFormulaConstants.GET_BASE_INT;
					}
					addNewProperty(properties,
							PerfIndiSpecFormulaConstants.TARGET_KPI_TYPE,
							propValue);
					break;
				}
			}
		}
	}

	private void setTargetKPITypeIfINTKpi(
			List<PerfIndiSpecFormulaProperty> properties,
			PerformanceIndicatorSpec performanceIndicatorSpecification) {
		String propValue = null;
		if (isINT(properties)) {
			if (performanceIndicatorSpecification.getPiSpecName()
					.endsWith(PerfIndiSpecFormulaConstants.INT_SUFFIX)) {
				boolean isXval = isXval(properties);
				if (UserDefinedKPIGeneratorUtil.islogisticKPI(
						getSrcKpi(performanceIndicatorSpecification
								.getPiSpecName().replace(
										PerfIndiSpecFormulaConstants.INT_SUFFIX,
										"")))
						|| isXval) {
					propValue = PerfIndiSpecFormulaConstants.GET_INT;
					if (isXval) {
						addNewProperty(properties,
								PerfIndiSpecFormulaConstants.TARGET_KPI,
								performanceIndicatorSpecification
										.getPiSpecName().replace(
												PerfIndiSpecFormulaConstants.INT_SUFFIX,
												""));
					}
				} else {
					propValue = PerfIndiSpecFormulaConstants.GET_BASE_INT;
				}
			} else {
				propValue = PerfIndiSpecFormulaConstants.GET_INT;
			}
			customizeProperties(properties, PerfIndiSpecFormulaConstants.TYPE,
					propValue);
		}
	}

	private boolean isXval(List<PerfIndiSpecFormulaProperty> properties) {
		boolean isXval = false;
		for (PerfIndiSpecFormulaProperty property : properties) {
			if (property.getpropertyName().toUpperCase()
					.contains(PerfIndiSpecFormulaConstants.XVAL_SEPARATOR)) {
				isXval = true;
				break;
			}
		}
		LOGGER.info(" isXval  : {}", isXval);
		return isXval;
	}

	private void customizeProperties(
			List<PerfIndiSpecFormulaProperty> properties, String propName,
			String newPropValue) {
		for (PerfIndiSpecFormulaProperty property : properties) {
			if (propName.equals(property.getpropertyName())) {
				property.setPropertyValue(newPropValue);
				break;
			}
		}
	}

	private boolean isINT(List<PerfIndiSpecFormulaProperty> properties) {
		boolean isINT = false;
		for (PerfIndiSpecFormulaProperty property : properties) {
			if (PerfIndiSpecFormulaConstants.GET_X
					.equalsIgnoreCase(property.getPropertyValue())) {
				isINT = true;
				break;
			}
		}
		LOGGER.debug("is Int : {}", isINT);
		return isINT;
	}

	public void addKPIFormulaEntriesForINTnBUCKET(
			PerformanceIndicatorSpec performanceIndicatorSpec,
			Map<String, Object> nameFormulaMapping)
			throws FormulaGeneratorException {
		LOGGER.debug(
				"Checking if  int and bucket entries should be added for {}",
				performanceIndicatorSpec);
		List<PerfIndiSpecFormulaProperty> properties = UserDefinedKPIGeneratorUtil
				.getPropertiesIfUserDefined(performanceIndicatorSpec);
		if (UserDefinedKPIGeneratorUtil.isValidLogisticKPI(
				performanceIndicatorSpec) && !isTypeGetX(properties)) {
			LOGGER.debug("Adding int and bucket entries for {}",
					performanceIndicatorSpec);
			setKPIFormulaForINTandBUCKET(performanceIndicatorSpec, null,
					nameFormulaMapping);
			addKPIFormulaRelsForSrcKPIINT(performanceIndicatorSpec, properties,
					nameFormulaMapping);
		}
	}

	private void addKPIFormulaRelsForSrcKPIINT(
			PerformanceIndicatorSpec performanceIndicatorSpec,
			List<PerfIndiSpecFormulaProperty> properties,
			Map<String, Object> nameFormulaMapping)
			throws FormulaGeneratorException {
		for (PerfIndiSpecFormulaProperty property : properties) {
			if (PerfIndiSpecFormulaConstants.KPI
					.equals(property.getpropertyName())
					&& notXval(property.getPropertyValue(), properties)) {
				PerformanceIndicatorSpec srcKPI = getSrcKpi(
						property.getPropertyValue());
				if (!FormulaGeneratorUtil.isUserDefinedFormula(
						srcKPI.getDerivationmethod(),
						srcKPI.getDerivationalgorithm())) {
					setKPIFormulaRelSrcKPI(performanceIndicatorSpec, srcKPI,
							nameFormulaMapping);
				}
			}
		}
	}

	private boolean notXval(String kpiName,
			List<PerfIndiSpecFormulaProperty> properties) {
		boolean isNotXval = true;
		for (PerfIndiSpecFormulaProperty property : properties) {
			if (kpiName.concat(PerfIndiSpecFormulaConstants.XVAL_SEPARATOR)
					.equals(property.getpropertyName())) {
				isNotXval = false;
				break;
			}
		}
		return isNotXval;
	}

	private void setKPIFormulaRelSrcKPI(
			PerformanceIndicatorSpec performanceIndicatorSpec,
			PerformanceIndicatorSpec srcKPI,
			Map<String, Object> nameFormulaMapping)
			throws FormulaGeneratorException {
		LOGGER.debug("Setting formula for srcKPI:"
				+ performanceIndicatorSpec.getSpecId());
		setFormula(performanceIndicatorSpec,
				PerfIndiSpecFormulaConstants.INT_SUFFIX, srcKPI,
				nameFormulaMapping);
	}

	private void setKPIFormulaForINTandBUCKET(
			PerformanceIndicatorSpec performanceIndicatorSpec,
			PerformanceIndicatorSpec srcKPI,
			Map<String, Object> nameFormulaMapping)
			throws FormulaGeneratorException {
		setFormula(performanceIndicatorSpec,
				PerfIndiSpecFormulaConstants.INT_SUFFIX, srcKPI,
				nameFormulaMapping);
		setFormula(performanceIndicatorSpec,
				PerfIndiSpecFormulaConstants.BUCKET_SUFFIX, srcKPI,
				nameFormulaMapping);
	}

	private void setFormula(PerformanceIndicatorSpec performanceIndicatorSpec,
			String suffix, PerformanceIndicatorSpec srcKPI,
			Map<String, Object> nameFormulaMapping)
			throws FormulaGeneratorException {
		String formulaProperty = PerfIndiSpecFormulaConstants.GET_X;
		if (PerfIndiSpecFormulaConstants.BUCKET_SUFFIX.equals(suffix)) {
			formulaProperty = PerfIndiSpecFormulaConstants.GET_BUCKETED_WEIGHT;
		}
		PerformanceIndicatorSpec additionalKPI = getAdditionalPerfIndiSpeccustomizeDerivationAlgo(
				performanceIndicatorSpec, formulaProperty, suffix, srcKPI);
		String formula = this.getFormula(additionalKPI);
		LOGGER.debug(
				"formula to be set for additional entries for {} with suffix {} is {}",
				performanceIndicatorSpec, suffix, formula);
		LOGGER.debug("Adding kpi : {} with formula :{}",
				additionalKPI.getPiSpecName(), formula);
		nameFormulaMapping.put(
				HASH_ENCLOSER + additionalKPI.getPiSpecName() + HASH_ENCLOSER,
				formula);
	}

	private boolean isTypeGetX(List<PerfIndiSpecFormulaProperty> properties) {
		boolean isINT = false;
		for (PerfIndiSpecFormulaProperty property : properties) {
			if (PerfIndiSpecFormulaConstants.TYPE
					.equals(property.getpropertyName())
					&& PerfIndiSpecFormulaConstants.GET_X
							.equalsIgnoreCase(property.getPropertyValue())) {
				isINT = true;
				break;
			}
		}
		LOGGER.debug("istypegetx :{}", isINT);
		return isINT;
	}

	protected AbstractUserDefinedKPIFormula getKPIFormulaTypeClass(
			String formulaType) {
		ApplicationContextLoader loader = ApplicationContextLoader
				.getInstance();
		LogisticKPIFormulaTypeProvider provider = loader
				.getLogisticKPITypeProvider();
		return provider.getBean(formulaType);
	}

	protected String getFormulaType(
			List<PerfIndiSpecFormulaProperty> properties) {
		String formulaType = null;
		ListIterator<PerfIndiSpecFormulaProperty> iterator = properties
				.listIterator();
		while (iterator.hasNext()) {
			PerfIndiSpecFormulaProperty property = iterator.next();
			String propertyName = property.getpropertyName();
			String propertyValue = property.getPropertyValue();
			if (StringUtils.isNotEmpty(propertyName)
					&& PerfIndiSpecFormulaConstants.TYPE.equals(propertyName)) {
				if ((PerfIndiSpecFormulaConstants.GET_XY.equals(propertyValue)
						|| PerfIndiSpecFormulaConstants.GET_Y
								.equals(propertyValue))) {
					formulaType = PerfIndiSpecFormulaConstants.GET_Y;
				} else {
					formulaType = propertyValue;
					break;
				}
			}
		}
		LOGGER.debug("formulaType : {}", formulaType);
		return formulaType;
	}
}
