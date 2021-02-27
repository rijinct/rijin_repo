
package com.project.rithomas.jobexecution.generator.formula;

public class KPINameWeightObj {

	private String sourceKpi = null;

	private Double weight = 0.0;

	private Boolean check = true;

	public KPINameWeightObj(String sourceKpi, Double weight, Boolean check) {
		super();
		this.sourceKpi = sourceKpi;
		this.weight = weight;
		this.check = check;
	}

	public String getSourceKpi() {
		return sourceKpi;
	}

	public void setSourceKpi(String sourceKpi) {
		this.sourceKpi = sourceKpi;
	}

	public Double getWeight() {
		return weight;
	}

	public void setWeight(Double weight) {
		this.weight = weight;
	}

	public Boolean getCheck() {
		return check;
	}

	public void setCheck(Boolean check) {
		this.check = check;
	}
}
