
package com.project.rithomas.jobexecution.generator.formula;

import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.workflow.generator.util.UserDefinedKPIGeneratorUtil;

public class FormulaGeneratorFactory {

	WeightedAvgFormulaGenerator weightedAvgFormulaGenerator;

	LogisticFormulaGenerator logisticFormulaGenerator;

	public AbstractFormulaGenerator getFormulaGenerator(
			PerformanceIndicatorSpec performanceIndicatorSpec) {
		if (UserDefinedKPIGeneratorUtil
				.isWeightedAverageKPI(performanceIndicatorSpec)) {
			return intializeWeightedAvgFormulaGenerator();
		} else {
			return intializeLogisticFormulaGenerator();
		}
	}

	private WeightedAvgFormulaGenerator intializeWeightedAvgFormulaGenerator() {
		if (weightedAvgFormulaGenerator == null) {
			weightedAvgFormulaGenerator = new WeightedAvgFormulaGenerator();
		}
		return weightedAvgFormulaGenerator;
	}

	private LogisticFormulaGenerator intializeLogisticFormulaGenerator() {
		if (logisticFormulaGenerator == null) {
			logisticFormulaGenerator = new LogisticFormulaGenerator();
		}
		return logisticFormulaGenerator;
	}
}
