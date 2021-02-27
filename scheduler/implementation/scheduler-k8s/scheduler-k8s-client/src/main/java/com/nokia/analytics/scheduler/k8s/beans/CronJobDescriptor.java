
package com.rijin.analytics.scheduler.k8s.beans;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import javax.validation.constraints.NotBlank;

import io.swagger.annotations.ApiModelProperty;

public class CronJobDescriptor extends JobDescriptor {

	@ApiModelProperty(notes = "${job.cronexpression}", example = "0 0 * * *", required = true, position = 3)
	@NotBlank
	private String cronExpression;

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	@Override
	public boolean isSchedule() {
		return isNotEmpty(cronExpression);
	}
}
