
package com.project.rithomas.jobexecution.project;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.common.cache.service.CacheServiceException;
import com.project.rithomas.common.cache.service.CacheServiceFactory;
import com.project.rithomas.common.cache.service.constants.CacheConstants;
import com.project.rithomas.common.cache.service.subscriber.SubscriberCacheService;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.sdk.model.meta.RuntimeProperty;
import com.project.rithomas.sdk.model.meta.query.RuntimePropertyQuery;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class SubscriberCacheLoader extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(SubscriberCacheLoader.class);

	Integer imsiBatchSize = 5000;

	Integer prefetchSize = 10;

	Integer numberOfLoops = 10;

	Integer numberOfThreads = 10;

	Long imsiIdBeginValue = 1L;

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = true;
		List<String> imsis = null;
		LOGGER.info("Retrieving imsi from es_subscriber_1..");
		try {
			imsis = getImsisFromSubscriberDimension(context);
		} catch (SQLException e1) {
			LOGGER.error(
					"SQL exception while getting imsi list from subscriber table: {}",
					e1.getMessage(), e1);
			throw new WorkFlowExecutionException(
					"SQL exception while getting imsi list from subscriber table: "
							+ e1.getMessage(),
					e1);
		} catch (Exception exception) {
			LOGGER.error(
					"exception while getting imsi list from subscriber table: {}",
					exception.getMessage(), exception);
			throw new WorkFlowExecutionException(
					"exception while getting imsi list from subscriber table: "
							+ exception.getMessage(),
					exception);
		}
		initializeParameters();
		Thread[] threads = new Thread[numberOfThreads];
		LOGGER.info("Imsi count retrieved from dimension table: {}",
				imsis.size());
		for (int i = 0; i < numberOfThreads; i++) {
			LOGGER.debug("Creating thread {}", i);
			Thread t = new Thread(new IdGenerator(imsis, i + 1));
			threads[i] = t;
			t.start();
		}
		for (Thread aThread : threads) {
			try {
				if (aThread != null) {
					aThread.join();
				}
			} catch (InterruptedException e) {
				throw new WorkFlowExecutionException(
						"Interrupted exception while updating cache: "
								+ e.getMessage(),
						e);
			}
		}
		return success;
	}

	private void initializeParameters() {
		RuntimePropertyQuery query = new RuntimePropertyQuery();
		RuntimeProperty imsiStartProperty = query
				.retrieve(RuntimeProperty.IMSI_ID_START_INDEX_FOR_ETL);
		RuntimeProperty prefetchSizeProperty = query
				.retrieve(RuntimeProperty.IMSI_SEQUENCE_NUMBER_RANGE);
		RuntimeProperty threadCountProperty = query
				.retrieve(RuntimeProperty.IMSI_ID_GEN_THREAD_COUNT);
		RuntimeProperty batchSizeProperty = query
				.retrieve(RuntimeProperty.IMSI_ID_BATCH_SIZE);
		if (imsiStartProperty != null
				&& imsiStartProperty.getParamvalue() != null) {
			imsiIdBeginValue = Long
					.parseLong(imsiStartProperty.getParamvalue());
		}
		if (prefetchSizeProperty != null
				&& prefetchSizeProperty.getParamvalue() != null) {
			prefetchSize = Integer
					.parseInt(prefetchSizeProperty.getParamvalue());
		}
		if (threadCountProperty != null
				&& threadCountProperty.getParamvalue() != null) {
			numberOfThreads = Integer
					.parseInt(threadCountProperty.getParamvalue());
		}
		if (batchSizeProperty != null
				&& batchSizeProperty.getParamvalue() != null) {
			imsiBatchSize = Integer.parseInt(batchSizeProperty.getParamvalue());
		}
		LOGGER.info(
				"Thread count: {}, imsi begin index: {}, batch size: {}, prefetch size: {}",
				new Object[] { numberOfThreads, imsiIdBeginValue, imsiBatchSize,
						prefetchSize });
	}

	private List<String> getImsisFromSubscriberDimension(
			WorkFlowContext context) throws Exception {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		List<String> allImsis = new ArrayList<String>();
		try {
			con = ConnectionManager
					.getConnection(schedulerConstants.HIVE_DATABASE);
			st = con.createStatement();
			String sqlToExecute = QueryConstants.IMSI_FROM_SUBSCRIBER;
			context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
					sqlToExecute);
			rs = st.executeQuery(sqlToExecute);
			while (rs.next()) {
				allImsis.add(rs.getString(1));
			}
		} catch (SQLException exception) {
			LOGGER.error("SQL Exception", exception.getMessage());
			throw exception;
		} catch (Exception exception) {
			LOGGER.error("Exception", exception.getMessage());
			throw exception;
		} finally {
			try {
				ConnectionManager.closeResultSet(rs);
				ConnectionManager.closeStatement(st);
				ConnectionManager.releaseConnection(con,
						schedulerConstants.HIVE_DATABASE);
			} catch (Exception e) {
				LOGGER.warn("Error while closing connection", e);
			}
		}
		return allImsis;
	}

	public class IdGenerator implements Runnable {

		List<String> imsiList = null;

		SubscriberCacheService cacheService = null;

		SecureRandom rand = new SecureRandom();

		int endId = -1;

		int currentId = -1;

		int threadId = 0;

		public IdGenerator(List<String> imsiList, int threadId)
				throws WorkFlowExecutionException {
			this.imsiList = imsiList;
			// Initialize Couchbase connections
			try {
				this.threadId = threadId;
				LOGGER.debug("Initializing thread: {}", this.threadId);
				Map<String, Object> additionalInfo = new HashMap<String, Object>();
				additionalInfo.put(CacheConstants.DIMENSION_CATEGORY_NAME,
						CacheConstants.SUBSCRIBER_BUCKET);
				additionalInfo.put(CacheConstants.BUCKETTING_CONDITION,
						CacheConstants.IGNORE);
				cacheService = CacheServiceFactory
						.getSubscriberCacheService(additionalInfo);
				LOGGER.debug("Thread[{}] got couchbase con:", threadId);
				// Initialise the counter to default begin value if not
				// initialised already.
				cacheService.incrementCounter(0L, imsiIdBeginValue);
				LOGGER.debug("Thread[{}] Initial Counter Value: {}", threadId,
						cacheService.getCounter());
				prefetchIds();
			} catch (CacheServiceException e) {
				LOGGER.error(
						"Exception while initializing cache loading thread: {}",
						e.getMessage(), e);
				throw new WorkFlowExecutionException(
						"Exception while initializing cache loading thread: "
								+ e.getMessage(),
						e);
			}
		}

		@Override
		public void run() {
			LOGGER.debug("Thread[{}] run Starts->", threadId);
			try {
				for (int i = 0; i < numberOfLoops; i++) {
					processBatch();
				}
			} catch (CacheServiceException e) {
				LOGGER.error(
						"Exception while loading the subscriber details to cache: {}",
						e.getMessage(), e);
			} finally {
				try {
					cacheService.closeCacheConnection();
				} catch (CacheServiceException e) {
					LOGGER.warn("Exception while closing cache connection: {}",
							e.getMessage(), e);
				}
			}
			LOGGER.debug("Thread[{}] run Ends->", threadId);
		}

		// Process a batch of IMSIs
		private void processBatch() throws CacheServiceException {
			LOGGER.debug("Inside Thread[{}] processBatch->", this.threadId);
			// Generate batch of IMSIs
			List<String> imsis = generateImsis();
			LOGGER.debug("Imsi list batch size: {}", imsis.size());
			// Get Ids for batch
			Map<String, Object> imsiIds = cacheService.getBulk(imsis);
			// Get missing IMSIs
			List<String> imsisMissing = getMissingImsis(imsis,
					imsiIds.keySet());
			if (imsisMissing.size() == 0) {
				LOGGER.debug("There are no missing imsis to be processed");
			} else {
				List<String> failedImsis = new ArrayList<String>();
				// Set Ids for missing IMSIs
				for (String imsi : imsisMissing) {
					if (currentId >= endId) {
						prefetchIds();
					}
					boolean added = addImsiId(imsi);
					if (!added) {
						failedImsis.add(imsi);
						LOGGER.debug(
								"Thread[{}] Id already present for {} with value {}",
								new Object[] { threadId, imsi,
										cacheService.getValue(imsi) });
					} else {
						imsiIds.put(imsi, currentId++);
					}
				}
				// Get Ids for failed IMSIs
				Map<String, Object> failedImsiIds = cacheService
						.getBulk(failedImsis);
				imsiIds.putAll(failedImsiIds);
			}
		}

		// Set Id in Couchbase for IMSI
		private boolean addImsiId(String imsi) throws CacheServiceException {
			LOGGER.debug("Thread[{}] Adding imsi: {} with id: {}",
					new Object[] { threadId, imsi, currentId });
			boolean result = cacheService.addDocument(imsi, currentId);
			return result;
		}

		// Prefetch Ids when local Ids are exhausted
		private void prefetchIds() throws CacheServiceException {
			endId = cacheService.incrementCounter((long) prefetchSize);
			currentId = endId - prefetchSize;
			LOGGER.debug(
					"Thread[{}] Initialized Ids --> startId: {}, endId: {}",
					new Object[] { threadId, currentId, endId });
		}

		// Generate random IMSIs for a batch from a list of all IMSIs
		private List<String> generateImsis() {
			List<String> imsis = new ArrayList<String>(imsiBatchSize);
			for (int i = 0; i < imsiBatchSize; i++) {
				imsis.add(imsiList.get(rand.nextInt(imsiList.size())));
			}
			return imsis;
		}

		// Get IMSIs of missing IMSI Ids
		private List<String> getMissingImsis(List<String> allImsis,
				Set<String> imsisPresent) {
			List<String> imsis = new ArrayList<String>(allImsis.size());
			imsis.addAll(allImsis);
			imsis.removeAll(imsisPresent);
			return imsis;
		}
	}
}
