
package com.project.sai.rithomas.scheduler.service;

import com.project.sai.rithomas.scheduler.exceptions.schedulerServiceException;

public interface schedulerService {

	void start() throws schedulerServiceException;

	void standBy() throws schedulerServiceException;

	void shutDown() throws schedulerServiceException;

	void pauseAllTriggers() throws schedulerServiceException;

	void resumeAllTriggers() throws schedulerServiceException;
}
