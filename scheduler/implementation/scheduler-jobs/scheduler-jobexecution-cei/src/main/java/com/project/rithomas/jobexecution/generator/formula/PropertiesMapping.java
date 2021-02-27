
package com.project.rithomas.jobexecution.generator.formula;

import java.util.List;

public class PropertiesMapping {

	List<String> dimAndValList;

	String weightedAvgFormula;

	public PropertiesMapping(String weightedAvgFormula,
			List<String> dimAndValList) {
		this.dimAndValList = dimAndValList;
		this.weightedAvgFormula = weightedAvgFormula;
	}

	public List<String> getDimAndValList() {
		return dimAndValList;
	}

	public String getWeightedAvgFormula() {
		return weightedAvgFormula;
	}
}
