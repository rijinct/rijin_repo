
package com.rijin.analytics.scheduler.processor;

import java.util.Map;

import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.schedulerManagementOperationResult;
import com.project.sai.rithomas.scheduler.constants.JobSchedulingOperation;
import com.project.sai.rithomas.scheduler.exceptions.schedulerServiceException;
import com.project.sai.rithomas.scheduler.service.schedulerService;

public abstract class BaseClientProcessor {

	protected boolean isAutoTriggerMode = false;
	
	public schedulerManagementOperationResult processRequest(
			JobSchedulingData request) throws schedulerServiceException {
		return this.processRequest(request, null);
	}

	public schedulerManagementOperationResult processRequest(
			JobSchedulingData request, Map<String, Object> jobMetaData)
			throws schedulerServiceException {
		schedulerManagementOperationResult result = null;
		if (request == null) {
			throw new schedulerServiceException("Request cannot be null");
		}
		if (request.getOperation() == null) {
			throw new schedulerServiceException(
					"Request operation cannot be null");
		}
		result = scheduleOperation(request, jobMetaData);
		if (result != null) {
			postProcessOperations(result);
		}
		return result;
	}
	
	public void setAutoTriggerMode(boolean isAutoTriggerMode) {
		this.isAutoTriggerMode = isAutoTriggerMode;
	}

	protected abstract schedulerService getService();

	private schedulerManagementOperationResult scheduleOperation(
			JobSchedulingData request, Map<String, Object> jobMetaData)
			throws schedulerServiceException {
		JobSchedulingOperation operation = JobSchedulingOperation
				.valueOf(request.getOperation().trim().toUpperCase());
		return processJobSchedule(operation, request, jobMetaData);
	}

	private schedulerManagementOperationResult processJobSchedule(
			JobSchedulingOperation operation,
			JobSchedulingData jobSchedulingData,
			Map<String, Object> jobMetaData) {
		schedulerManagementOperationResult result = null;
		if (jobSchedulingData.getSchedule() != null) {
			jobSchedulingData.trim();
			if (jobMetaData == null) {
				result = getJobProcessor(operation)
						.processRequest(jobSchedulingData);
			} else {
				result = getJobProcessor(operation)
						.processRequest(jobSchedulingData, jobMetaData);
			}
		}
		return result;
	}

	protected JobProcessor getJobProcessor(JobSchedulingOperation operation) {
		JobProcessor processor = null;
		switch (operation) {
		case CREATE:
			processor = getJobCreator();
			break;
		case DELETE:
			processor = getJobDeletor();
			break;
		case SCHEDULE:
			processor = getJobscheduler();
			break;
		case UNSCHEDULE:
			processor = getJobUnscheduler();
			break;
		case RESCHEDULE:
			processor = getJobRescheduler();
			break;
		case ABORT:
			processor = getJobAborter();
			break;
		case ASYNCHRONOUS_SCHEDULE:
			processor = getAsynchronousScheduleProcessor();
			break;
		default:
			throw new schedulerServiceException(
					"Unsupported operation " + operation);
		}
		return processor;
	}


	protected abstract JobProcessor getJobCreator();

	protected abstract JobProcessor getJobDeletor();

	protected abstract JobProcessor getJobscheduler();

	protected abstract JobProcessor getJobUnscheduler();

	protected abstract JobProcessor getJobRescheduler();

	protected abstract JobProcessor getJobAborter();
	
	protected abstract JobProcessor getAsynchronousScheduleProcessor();

	protected abstract void postProcessOperations(
			schedulerManagementOperationResult result);
}
