
package com.rijin.analytics.scheduler.k8s.beans;

import javax.validation.constraints.NotBlank;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel
public class JobDescriptor {

	@JsonIgnore
	private String operation;

	@ApiModelProperty(notes = "${job.group}", example = "VLR_1", required = true, position = 1)
	@NotBlank
	private String jobGroup;

	@ApiModelProperty(notes = "${job.name}", example = "Perf_VLR_1_DAY_AggregateJob", required = true, position = 2)
	@NotBlank
	private String jobName;

	public String getJobGroup() {
		return jobGroup;
	}

	public void setJobGroup(String jobGroup) {
		this.jobGroup = jobGroup;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

	@JsonIgnore
	public boolean isSchedule() {
		return false;
	}
}
