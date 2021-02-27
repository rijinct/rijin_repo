
package com.rijin.analytics.scheduler.k8s.exception;

import org.springframework.http.HttpStatus;

import com.rijin.analytics.exceptions.ServiceException;

public class InvalidXMLException extends ServiceException {

	private static final long serialVersionUID = -4965255388742534819L;

	public InvalidXMLException(String developerMessage,String errorCode) {
		super(developerMessage,errorCode, HttpStatus.BAD_REQUEST);
	}
}