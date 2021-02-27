
package com.project.rithomas.jobexecution.generator.formula;

import java.util.Map;

public class UsageWeightObj {

	public boolean singleKPI = false;

	public boolean twoKPIs = false;

	private Map<String, String> usageServiceWeightMap;

	public UsageWeightObj(Map<String, String> usageServiceWeightMap) {
		super();
		this.usageServiceWeightMap = usageServiceWeightMap;
	}

	public Map<String, String> getUsageServiceWeightMap() {
		return usageServiceWeightMap;
	}

	public void setUsageServiceWeightMap(
			Map<String, String> usageServiceWeightMap) {
		this.usageServiceWeightMap = usageServiceWeightMap;
	}

	@Override
	public String toString() {
		return new StringBuilder("UsageWeightObj[usageServiceWeightMap=")
				.append(usageServiceWeightMap).append(", singleKPI=")
				.append(singleKPI).append(", twoKPIs=").append(twoKPIs)
				.append("]").toString();
	}
}
