
package com.rijin.scheduler.jobexecution.hive.settings;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("Job")
public class Job {

	@XStreamAlias("Name")
	private String name;

	@XStreamAlias("QueryHints")
	private QueryHints queryHints;

	@XStreamAlias("Pattern")
	private String pattern;
	@XStreamAlias("CustomDbUrl")
	private Boolean isCustomDbUrl;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public QueryHints getQueryHints() {
		return queryHints;
	}

	public void setQueryHints(QueryHints queryHints) {
		this.queryHints = queryHints;
	}

	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public void setCustomDbUrl(Boolean customDbUrl) {
		this.isCustomDbUrl = customDbUrl;
	}
	public Boolean isCustomDbUrl() {
		return this.isCustomDbUrl;
	}
}
