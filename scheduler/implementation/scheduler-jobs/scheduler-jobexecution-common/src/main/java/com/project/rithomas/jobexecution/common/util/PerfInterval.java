
package com.project.rithomas.jobexecution.common.util;

public enum PerfInterval {
	MIN_15(900000),
	HOUR(3600000),
	DAY(86400000);

	private long intervalInSeconds;

	private PerfInterval(long intervalAsString) {
		this.intervalInSeconds = intervalAsString;
	}

	public long convertToSeconds() {
		return this.intervalInSeconds;
	}
}
