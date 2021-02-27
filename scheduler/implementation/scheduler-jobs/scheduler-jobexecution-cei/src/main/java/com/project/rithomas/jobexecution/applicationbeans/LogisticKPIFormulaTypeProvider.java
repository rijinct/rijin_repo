
package com.project.rithomas.jobexecution.applicationbeans;

import com.project.rithomas.jobexecution.generator.formula.AbstractUserDefinedKPIFormula;

public interface LogisticKPIFormulaTypeProvider {

	AbstractUserDefinedKPIFormula getBean(String formulaType);
}
