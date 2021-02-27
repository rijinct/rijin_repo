
package com.project.rithomas.jobexecution.generator.formula;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.project.rithomas.jobexecution.applicationbeans.ApplicationContextLoader;
import com.project.rithomas.jobexecution.applicationbeans.WeightedAvgKPIFormulaTypeProvider;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;

public class WeightedAvgFormulaGenerator
		extends AbstractUserDefinedFormulaGenerator {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(WeightedAvgFormulaGenerator.class);

	public String getFormula(
			PerformanceIndicatorSpec performanceIndicatorSpecification,
			Map<String, Object> nameFormulaMapping)
			throws FormulaGeneratorException {
		String formula = super.getFormula(performanceIndicatorSpecification);
		addKPIFormulaEntriesForBUCKET(performanceIndicatorSpecification,
				nameFormulaMapping);
		return formula;
	}

	public void addKPIFormulaEntriesForBUCKET(
			PerformanceIndicatorSpec performanceIndicatorSpec,
			Map<String, Object> nameFormulaMapping)
			throws FormulaGeneratorException {
		LOGGER.debug("Checking if  bucket entries should be added for {}",
				performanceIndicatorSpec);
		if (!performanceIndicatorSpec.isAdditionalKPI()) {
			LOGGER.debug("Adding bucket entries for {}",
					performanceIndicatorSpec);
			setKPIFormula(performanceIndicatorSpec, nameFormulaMapping, null);
		}
	}

	private void setKPIFormula(
			PerformanceIndicatorSpec performanceIndicatorSpec,
			Map<String, Object> nameFormulaMapping,
			PerformanceIndicatorSpec srcKPI) throws FormulaGeneratorException {
		PerformanceIndicatorSpec additionalKPI = getAdditionalPerfIndiSpeccustomizeDerivationAlgo(
				performanceIndicatorSpec,
				PerfIndiSpecFormulaConstants.GET_BUCKETED_WEIGHT,
				PerfIndiSpecFormulaConstants.BUCKET_SUFFIX, srcKPI);
		String formula = this.getFormula(additionalKPI);
		nameFormulaMapping.put(
				HASH_ENCLOSER + additionalKPI.getPiSpecName() + HASH_ENCLOSER,
				formula);
	}

	@Override
	public String getGenericInputStructure(
			PerformanceIndicatorSpec performanceIndicatorSpecification,
			List<PerfIndiSpecFormulaProperty> properties, String kpiFormulaID,
			KPIIndexConfiguration kpiIndexConfig)
			throws FormulaGeneratorException {
		setPerfIndiSpecDetailsFromContext();
		setTargetKPITypeIfUsageWeightKPI(properties);
		String formula = "";
		try {
			formula = getFormulaToCalculate(properties, kpiIndexConfig);
		} catch (FormulaGeneratorException e) {
			LOGGER.error("Error generating formula: {}", e);
			throw new FormulaGeneratorException(
					"Exception while generating formula :", e);
		}
		return formula;
	}

	protected AbstractUserDefinedKPIFormula getKPIFormulaTypeClass(
			String formulaType) {
		ApplicationContextLoader loader = ApplicationContextLoader
				.getInstance();
		WeightedAvgKPIFormulaTypeProvider provider = loader
				.getWeightedAvgKPITypeProvider();
		return provider.getBean(formulaType);
	}

	protected String getFormulaType(
			List<PerfIndiSpecFormulaProperty> properties) {
		String formulaType = "GET_WEIGHTED_AVG";
		for (PerfIndiSpecFormulaProperty property : properties) {
			if (PerfIndiSpecFormulaConstants.TYPE
					.equals(property.getpropertyName())) {
				formulaType = property.getPropertyValue();
			}
		}
		LOGGER.debug("formulaType : {}", formulaType);
		return formulaType;
	}
}
