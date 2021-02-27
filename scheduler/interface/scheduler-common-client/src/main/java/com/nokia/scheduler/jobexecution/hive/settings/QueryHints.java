
package com.rijin.scheduler.jobexecution.hive.settings;

import java.util.List;

import com.thoughtworks.xstream.annotations.XStreamImplicit;

public class QueryHints {

	@XStreamImplicit(itemFieldName = "hint")
	private List<String> hint;

	public List<String> getHint() {
		return hint;
	}

	public void setHint(List<String> hint) {
		this.hint = hint;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return hint != null ? hint.toString() : "null";
	}
}
