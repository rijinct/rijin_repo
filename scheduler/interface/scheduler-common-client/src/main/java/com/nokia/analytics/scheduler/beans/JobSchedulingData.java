
package com.rijin.analytics.scheduler.beans;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

import lombok.EqualsAndHashCode;


@EqualsAndHashCode
@XStreamAlias("JobSchedulingData")
public class JobSchedulingData {

	@XStreamAsAttribute
	private String operation;

	@XStreamAlias("Schedule")
	private Schedule schedule;

	public void setOperation(String operation) {
		this.operation = operation;
	}

	public String getOperation() {
		return this.operation;
	}

	public void setSchedule(Schedule schedule) {
		this.schedule = schedule;
	}

	public Schedule getSchedule() {
		return this.schedule;
	}

	public void trim() {
		if (operation != null) {
			setOperation(operation.trim());
		}
		if (schedule != null) {
			schedule.trim();
		}
	}

	@Override
	public String toString() {
		return "JobSchedulingData [operation=" + operation + ", schedule="
				+ schedule + "]";
	}
}
