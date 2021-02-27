
package com.project.rithomas.jobexecution.common.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.model.meta.query.RuntimePropertyQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ReConnectUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ReConnectUtil.class);

	public static final String TTRANSPORT_EXCEPTION = "TTransportException";

	public static final String RETURN_CODE_1 = "return code 1";

	public static final String RETURN_CODE_2 = "return code 2";

	public static final Set<String> retryExceptions = new HashSet<String>();

	public static final int DEFAULT_RETRIES = 3;

	public static final long DEFAULT_WAIT_TIME_IN_MILLI = 30000;

	public static final String HIVE_RETRY_COUNT = "HIVE_RETRY_COUNT";

	public static final String HIVE_RETRY_WAIT_INTERVAL = "HIVE_RETRY_WAIT_INTERVAL";

	private static final String ILLEGAL_STATE_EXCEPTION = "IllegalStateException";

	private int numberOfRetries;

	private int numberOfTriesLeft;

	private long timeToWait;

	public long gettimeToWait() {
		return this.timeToWait;
	}

	public long getnumberOfTriesLeft() {
		return this.numberOfTriesLeft;
	}

	public ReConnectUtil() {
		this(DEFAULT_RETRIES, DEFAULT_WAIT_TIME_IN_MILLI);
	}

	public ReConnectUtil(int numberOfRetries, long timetowait) {
		this.numberOfRetries = numberOfRetries;
		this.numberOfTriesLeft = numberOfRetries;
		this.timeToWait = timetowait;
	}

	public boolean shouldRetry() {
		LOGGER.debug("The value of numberOfTriesLeft {}", numberOfTriesLeft);
		return numberOfTriesLeft > 0;
	}

	public void updateRetryAttempts() throws JobExecutionException {
		this.numberOfTriesLeft--;
		if (shouldRetry()) {
			LOGGER.debug("Attempts left to retry : {} Will retry after {} ms",
					numberOfTriesLeft, timeToWait);
		} else {
			throw new JobExecutionException(
					"ReConnect to database Failed with  " + numberOfRetries
							+ " attempts");
		}
		waitUntilNextTry();
	}

	public void updateRetryAttemptsForHive(String message,
			WorkFlowContext context) throws WorkFlowExecutionException {

		numberOfTriesLeft--;
		if (shouldRetry()) {
			LOGGER.info("Attempts left to retry : {} Will retry after {} ms",
					numberOfTriesLeft, timeToWait);
		} else {
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION, message);
			throw new WorkFlowExecutionException("Retry Failed with  "
					+ numberOfRetries + " attempts. Exception: " + message);
		}
		waitUntilNextTry();
	}

	private void waitUntilNextTry() {
		try {
			Thread.sleep(gettimeToWait());
		} catch (InterruptedException exception) {
			LOGGER.error("Exception While waiting" + exception.getMessage());
		}
	}

	private static Collection<String> getRetryExceptions() {
		if (retryExceptions.isEmpty()) {
			WorkFlowContext context = new JobExecutionContext();
			try {
				retryExceptions.addAll(getRetryExceptionsFromDB(context));
				LOGGER.debug("Retry exceptions from DB: {}", retryExceptions);
			} catch (JobExecutionException e) {
				LOGGER.warn(
						"Exception trying to get runtime property for retry messages: {}",
						e.getMessage());
			}
			retryExceptions.add(TTRANSPORT_EXCEPTION);
			retryExceptions.add(RETURN_CODE_1);
			retryExceptions.add(RETURN_CODE_2);
			retryExceptions.add(ILLEGAL_STATE_EXCEPTION);
		}
		return retryExceptions;
	}

	private static Collection<String> getRetryExceptionsFromDB(
			WorkFlowContext context) throws JobExecutionException {
		Collection<String> retryExceptionList = new ArrayList<String>();
		QueryExecutor executor = new QueryExecutor();
		List resultList = executor.executeMetadatasqlQueryMultiple(
				QueryConstants.GET_HIVE_RETRY_EXCEPTIONS, context);
		if (resultList != null && !resultList.isEmpty()) {
			for (Object result : resultList) {
				if (result != null) {
					retryExceptionList.add(result.toString());
				}
			}
		}
		return retryExceptionList;
	}

	private Boolean retryRequiredForAllSQLExceptions() {
		RuntimePropertyQuery runtimePropQuery = new RuntimePropertyQuery();
		Boolean value = Boolean.valueOf(
				runtimePropQuery.retrieve("RETRY_ALL_HIVE_EXCEPTIONS", "true"));
		return value;
	}

	public boolean isRetryRequired(String errorMessage) {
		if (retryRequiredForAllSQLExceptions()) {
			LOGGER.debug("Retry required for all hive exceptions");
			return true;
		}
		for (String retryException : getRetryExceptions()) {
			if (errorMessage.contains(retryException)) {
				LOGGER.debug(
						"Exception message \"{}\" contains '{}', so retry required",
						errorMessage, retryException);
				return true;
			}
		}
		return false;
	}

	public Integer reconnectCount() {
		RuntimePropertyQuery query = new RuntimePropertyQuery();
		return Integer.parseInt(query.retrieve(HIVE_RETRY_COUNT,
				String.valueOf(DEFAULT_RETRIES)));
	}

	public Long reconnectWaitTime() {
		RuntimePropertyQuery query = new RuntimePropertyQuery();
		return Long.parseLong(query.retrieve(HIVE_RETRY_WAIT_INTERVAL,
				String.valueOf(DEFAULT_WAIT_TIME_IN_MILLI)));
	}
}
