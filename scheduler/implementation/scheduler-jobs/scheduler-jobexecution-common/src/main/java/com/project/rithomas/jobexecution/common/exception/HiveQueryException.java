
package com.project.rithomas.jobexecution.common.exception;

public class HiveQueryException extends Exception {

	private static final long serialVersionUID = 1L;

	public HiveQueryException(String msg) {
		super(msg);
	}

	public HiveQueryException(String msg, Throwable ex) {
		super(msg, ex);
	}
}
