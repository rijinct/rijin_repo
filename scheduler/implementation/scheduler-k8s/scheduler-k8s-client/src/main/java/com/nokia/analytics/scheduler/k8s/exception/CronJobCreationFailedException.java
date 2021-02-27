
package com.rijin.analytics.scheduler.k8s.exception;

import org.springframework.http.HttpStatus;

import com.rijin.analytics.exceptions.ServiceException;

public class CronJobCreationFailedException extends ServiceException {

	private static final long serialVersionUID = -5152488485974499628L;

	public CronJobCreationFailedException() {
		super("cronJobCreationFailed", HttpStatus.UNPROCESSABLE_ENTITY);
	}
}
