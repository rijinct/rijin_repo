
package com.rijin.analytics.scheduler.beans;

import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
@XStreamAlias("Trigger")
public class Trigger {

	@XStreamAlias("Cron")
	private Cron cron;

	@XStreamAlias("Simple")
	private Simple simple;

	public void setCron(Cron cron) {
		this.cron = cron;
	}

	public Cron getCron() {
		return cron;
	}

	public void setSimple(Simple simple) {
		this.simple = simple;
	}

	public Simple getSimple() {
		return simple;
	}
	
	public void trim() {
		if (cron != null) {
			cron.trim();
		}
		if (simple != null) {
			simple.trim();
		}
	}

	public boolean isCron() {
		return cron != null;
	}

	public boolean isSimple() {
		return simple != null;
	}

	@Override
	public String toString() {
		return "Trigger [Cron=" + cron + ", Simple=" + simple + "]";
	}
}
