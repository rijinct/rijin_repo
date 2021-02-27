
package com.rijin.analytics.scheduler.processor;

import java.util.Map;

import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.schedulerManagementOperationResult;

public abstract class JobProcessor {

	public schedulerManagementOperationResult processRequest(
			JobSchedulingData request) {
		return null;
	}

	public schedulerManagementOperationResult processRequest(
			JobSchedulingData request, Map<String, Object> jobMetaData) {
		return null;
	}
}
