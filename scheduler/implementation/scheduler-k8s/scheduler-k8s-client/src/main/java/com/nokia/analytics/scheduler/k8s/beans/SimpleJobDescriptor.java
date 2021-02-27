
package com.rijin.analytics.scheduler.k8s.beans;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import io.swagger.annotations.ApiModelProperty;

public class SimpleJobDescriptor extends JobDescriptor {

	@ApiModelProperty(notes = "${job.starttime}", example = "2018.11.02 11:15:00", required = true, position = 3)
	private String startTime;

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	@Override
	public boolean isSchedule() {
		return isNotEmpty(startTime);
	}
}
