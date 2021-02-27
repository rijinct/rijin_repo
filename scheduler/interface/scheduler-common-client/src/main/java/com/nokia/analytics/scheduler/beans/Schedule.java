
package com.rijin.analytics.scheduler.beans;

import java.util.List;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
@XStreamAlias("Schedule")
public class Schedule {

	@XStreamImplicit
	private List<Job> jobs;

	@XStreamImplicit
	private List<Trigger> trigger;

	public void setJobs(List<Job> jobs) {
		this.jobs = jobs;
	}

	public List<Job> getJobs() {
		return this.jobs;
	}

	public void setTrigger(List<Trigger> trigger) {
		this.trigger = trigger;
	}

	public List<Trigger> getTrigger() {
		return trigger;
	}

	public void trim() {
		if (jobs != null) {
			for (Job job : jobs) {
				job.trim();
			}
		}
		if (trigger != null) {
			for (Trigger trigger : trigger) {
				if (trigger != null) {
					trigger.trim();
				}
			}
		}
	}

	@Override
	public String toString() {
		return "Schedule [jobs=" + jobs + ", triggers=" + trigger + "]";
	}
}
