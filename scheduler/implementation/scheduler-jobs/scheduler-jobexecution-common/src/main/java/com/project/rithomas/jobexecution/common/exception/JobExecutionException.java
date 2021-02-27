
package com.project.rithomas.jobexecution.common.exception;

public class JobExecutionException extends Exception {

	private static final long serialVersionUID = 1L;

	public JobExecutionException(String msg) {
		super(msg);
	}

	public JobExecutionException(String message, Throwable ex) {
		super(message, ex);
	}
}
