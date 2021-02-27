package com.rijin.analytics.scheduler.k8s.exception;

import org.springframework.http.HttpStatus;

import com.rijin.analytics.exceptions.ServiceException;

public class AbortJobException extends ServiceException {
	private static final long serialVersionUID = 1L;

	public AbortJobException(String developerMessage,String errorCode) {
		super(developerMessage,errorCode, HttpStatus.OK);
	}
}
