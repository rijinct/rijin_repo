
package com.rijin.scheduler.jobexecution.hive.settings;

import java.util.List;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

@XStreamAlias("Hive2Settings")
public class Hive2Settings {

	@XStreamImplicit(itemFieldName = "Job")
	private List<Job> jobId;

	public List<Job> getJobId() {
		return jobId;
	}

	public void setJobId(List<Job> jobId) {
		this.jobId = jobId;
	}
}
