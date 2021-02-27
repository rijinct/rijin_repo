
package com.rijin.analytics.scheduler.beans;

import java.util.HashSet;
import java.util.Set;

public class schedulerManagementOperationResult {

	private int errorCount;

	private int successCount;

	private Set<String> errorMsgs;

	private boolean error;

	private boolean success;

	public schedulerManagementOperationResult() {
		this.errorCount = 0;
		this.errorMsgs = new HashSet<String>();
	}

	public int getErrorCount() {
		return errorCount;
	}

	public void setErrorCount(int errorCount) {
		this.errorCount = errorCount;
	}

	public int getSuccessCount() {
		return successCount;
	}

	public void setSuccessCount(int successCount) {
		this.successCount = successCount;
	}

	public void addErrorMsg(String msg) {
		this.errorMsgs.add(msg);
	}

	public Set<String> getErrorMsgs() {
		return errorMsgs;
	}

	public void setError(boolean error) {
		this.error = error;
	}

	public boolean isError() {
		return error;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public boolean isSuccess() {
		return success;
	}
}
