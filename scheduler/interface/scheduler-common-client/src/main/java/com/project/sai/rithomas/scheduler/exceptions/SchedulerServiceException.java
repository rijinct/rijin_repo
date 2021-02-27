
package com.project.sai.rithomas.scheduler.exceptions;

import com.project.sai.rithomas.scheduler.service.schedulerService;

public class schedulerServiceException extends RuntimeException {

	private static final long serialVersionUID = -6626735800806157134L;

	public schedulerServiceException(String message) {
		super(message);
	}

	public schedulerServiceException(String message, Throwable cause) {
		super(message, cause);
	}

	public schedulerServiceException(Throwable cause) {
		super(cause);
	}
}
