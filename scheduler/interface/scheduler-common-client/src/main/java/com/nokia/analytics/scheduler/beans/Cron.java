
package com.rijin.analytics.scheduler.beans;

import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
@XStreamAlias("Cron")
public class Cron {

	@XStreamAlias("TriggerName")
	private String name;

	@XStreamAlias("TriggerGroup")
	private String group;

	@XStreamAlias("Description")
	private String description;

	@XStreamAlias("JobName")
	private String jobName;

	@XStreamAlias("JobGroup")
	private String jobGroup;

	@XStreamAlias("StartTime")
	private String startTime;

	@XStreamAlias("CronExpression")
	private String cronExpression;

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

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
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
		if (cronExpression != null) {
			setCronExpression(cronExpression.trim());
		}
		if (description != null) {
			setDescription(description.trim());
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
	}

	@Override
	public String toString() {
		return "Cron [name=" + name + ", group=" + group + ", description="
				+ description + ", jobName=" + jobName + ", jobGroup="
				+ jobGroup + ", startTime=" + startTime + ", cronExpression="
				+ cronExpression + "]";
	}
}
