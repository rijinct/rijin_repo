
package com.rijin.analytics.scheduler.beans;

import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
@XStreamAlias("Job")
public class Job {

	@XStreamAlias("Name")
	private String name;

	@XStreamAlias("JobClass")
	private String jobClass;

	@XStreamAlias("JobGroup")
	private String jobGroup;

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return this.name;
	}

	public void setJobClass(String jobClass) {
		this.jobClass = jobClass;
	}

	public String getJobClass() {
		return this.jobClass;
	}

	public void setJobGroup(String jobGroup) {
		this.jobGroup = jobGroup;
	}

	public String getJobGroup() {
		return this.jobGroup;
	}

	public void trim() {
		if (jobClass != null) {
			setJobClass(jobClass.trim());
		}
		setName(name.trim());
		setJobGroup(jobGroup.trim());
	}

	@Override
	public String toString() {
		return "Job [name=" + name + ", jobClass=" + jobClass + ", jobGroup="
				+ jobGroup + "]";
	}
}
