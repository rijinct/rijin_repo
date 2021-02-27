
package com.project.rithomas.jobexecution.applicationbeans;

import com.project.rithomas.jobexecution.generator.formula.AbstractUserDefinedKPIFormula;

public interface WeightedAvgKPIFormulaTypeProvider {

	AbstractUserDefinedKPIFormula getBean(String formulaType);
}
