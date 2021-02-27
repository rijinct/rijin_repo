
package com.rijin.analytics.scheduler.beans;

import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
@XStreamAlias("Simple")
public class Simple {

	@XStreamAlias("TriggerName")
	private String name;

	@XStreamAlias("TriggerGroup")
	private String group;

	@XStreamAlias("JobName")
	private String jobName;

	@XStreamAlias("JobGroup")
	private String jobGroup;

	@XStreamAlias("StartTime")
	private String startTime;

	@XStreamAlias("RepeatCount")
	private String repeatCount;

	@XStreamAlias("RepeatInterval")
	private String repeatInterval;

	@XStreamAlias("Description")
	private String description;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getJobGroup() {
		return jobGroup;
	}

	public void setJobGroup(String jobGroup) {
		this.jobGroup = jobGroup;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getRepeatCount() {
		return repeatCount;
	}

	public void setRepeatCount(String repeatCount) {
		this.repeatCount = repeatCount;
	}

	public String getRepeatInterval() {
		return repeatInterval;
	}

	public void setRepeatInterval(String repeatInterval) {
		this.repeatInterval = repeatInterval;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getDescription() {
		return description;
	}

	public void trim() {
		if (name != null) {
			setName(name.trim());
		}
		if (group != null) {
			setGroup(group.trim());
		}
		if (jobGroup != null) {
			setJobGroup(jobGroup.trim());
		}
		if (jobName != null) {
			setJobName(jobName.trim());
		}
		if (startTime != null) {
			setStartTime(startTime.trim());
		}
		if (repeatCount != null) {
			setRepeatCount(repeatCount.trim());
		}
		if (repeatInterval != null) {
			setRepeatInterval(repeatInterval.trim());
		}
		if (description != null) {
			setDescription(description.trim());
		}
	}

	@Override
	public String toString() {
		return "Simple [name=" + name + ", group=" + group + ", jobName="
				+ jobName + ", jobGroup=" + jobGroup + ", startTime="
				+ startTime + ", repeatCount=" + repeatCount
				+ ", repeatInterval=" + repeatInterval + ", description="
				+ description + "]";
	}
}