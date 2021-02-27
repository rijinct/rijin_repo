
package com.project.rithomas.jobexecution.common.util;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.io.IOException;

import com.rijin.analytics.cacheservice.common.util.DataAvailabilityServiceProvider;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.common.cache.service.CacheServiceException;
import com.project.rithomas.common.cache.service.dataavailability.DataAvailabilityCacheService;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class DataAvailabilityCacheUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DataAvailabilityCacheUtil.class);

	DataAvailabilityCacheService cacheService;

	DataAvailabilityCacheServiceRestClient cacheServiceClient;

	public DataAvailabilityCacheUtil() throws CacheServiceException {
		if (isKubernetesEnvironment()) {
			cacheServiceClient = getDataAvailabilityCacheServiceRestClient();
		} else {
			cacheService = getDataAvailabilityCacheService();
		}
	}

	public String getValue(String key) throws CacheServiceException {
		String value = null;
		if (isKubernetesEnvironment()) {
			value = getValueFromEndpoint(key);
		} else {
			value = cacheService.getValue(key);
		}
		return value;
	}

	private String getValueFromEndpoint(String key)
			throws CacheServiceException {
		String value = null;
		try {
			cacheServiceClient.setDataAvailabilityKey(key);
			value = cacheServiceClient.execute();
		} catch (IOException ex) {
			LOGGER.error(
					"Exception while retrieving value from couchbase for the key {}",
					key, ex);
			throw new CacheServiceException(ex.getMessage());
		}
		return value;
	}

	private DataAvailabilityCacheService getDataAvailabilityCacheService()
			throws CacheServiceException {
		return DataAvailabilityServiceProvider
				.getDataAvailabilityCacheService();
	}

	public void closeConnection() {
		if (cacheService != null) {
			try {
				cacheService.closeCacheConnection();
			} catch (CacheServiceException e) {
				LOGGER.warn("Error while closing the cache connection: {}",
						e.getMessage(), e);
			}
		}
		cacheService = null;
		cacheServiceClient = null;
	}

	private static boolean isKubernetesEnvironment() {
		return toBoolean(System.getenv(schedulerConstants.IS_K8S));
	}

	private DataAvailabilityCacheServiceRestClient getDataAvailabilityCacheServiceRestClient() {
		return new DataAvailabilityCacheServiceRestClient();
	}
}
