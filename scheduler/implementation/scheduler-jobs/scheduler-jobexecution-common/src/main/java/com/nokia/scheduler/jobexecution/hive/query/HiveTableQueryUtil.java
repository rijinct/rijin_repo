
package com.rijin.scheduler.jobexecution.hive.query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.exception.HiveQueryException;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.meta.query.GeneratorPropertyQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class HiveTableQueryUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveTableQueryUtil.class);

	private static final String PERIOD = ".";

	private final static String UNDERSCORE = "_";

	private static Map<String, String> tableNameHDFSLocationMap = new HashMap<>();

	public static List<String> retrieveTargetTableHDFSLocation(
			WorkFlowContext context) throws HiveQueryException {
		String tableName = (String) context
				.getProperty(JobExecutionContext.TARGET);
		return retrieveHDFSLocationGivenTableName(context, tableName);
	}

	public static String getTableLocation(WorkFlowContext context)
			throws HiveQueryException {
		String tableLocation = null;
		List<String> tableLocations = HiveTableQueryUtil
				.retrieveTargetTableHDFSLocation(context);
		if (CollectionUtils.isNotEmpty(tableLocations)) {
			tableLocation = tableLocations.get(0);
		}
		return tableLocation;
	}

	public static List<String> retrieveHDFSLocationGivenTableName(
			WorkFlowContext context, String tableName)
			throws HiveQueryException {
		List<String> hdfsLocationList = null;
		boolean isCustomDbUrl = (boolean) context
				.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL);
		LOGGER.info("Retrieving the HDFS location for the table name: {}.",
				tableName);
		Statement st = null;
		Connection connection = null;
		ResultSet queryResultList = null;
		try {
			if (tableName != null && !tableName.isEmpty()) {
				String[] tableNamesList = null;
				if (tableName.contains(",")) {
					tableNamesList = tableName.split(",");
				} else {
					tableNamesList = new String[1];
					tableNamesList[0] = tableName;
				}
				if (tableNamesList != null && tableNamesList.length > 0) {
					hdfsLocationList = new ArrayList<String>();
					for (String tableNameObj : tableNamesList) {
						String hdfsPath = tableNameHDFSLocationMap
								.get(tableNameObj);
						if (hdfsPath == null) {
							if (connection == null) {
								connection = DBConnectionManager.getInstance()
										.getConnection(
										schedulerConstants.HIVE_DATABASE,
												isCustomDbUrl);
								st = connection.createStatement();
							}
							String query = String.format(
									"describe formatted %s", tableNameObj);
							queryResultList = st.executeQuery(query);
							LOGGER.debug(
									"The queryResultList for the table : {}",
									queryResultList);
							if (queryResultList != null) {
								while (queryResultList.next()) {
									if (queryResultList.getString(1) != null
											&& queryResultList.getString(1)
													.contains(
															JobExecutionContext.HDFS_LOCATION)) {
										hdfsPath = queryResultList.getString(2);
										tableNameHDFSLocationMap
												.put(tableNameObj, hdfsPath);
									}
								}
							}
						}
						LOGGER.debug("HDFS Path : {}", hdfsPath);
						if (StringUtils.isNotEmpty(hdfsPath)) {
							hdfsLocationList.add(hdfsPath);
						}
					}
				}
			}
		} catch (SQLException e1) {
			LOGGER.error("SQLException : " + e1.getMessage());
			throw new HiveQueryException(
					"Exception during query Execution : " + e1.getMessage(),
					e1);
		} catch (Exception exception) {
			LOGGER.error("Exception : " + exception.getMessage());
			throw new HiveQueryException("Exception during query Execution : "
					+ exception.getMessage(), exception);
		} finally {
			try {
				ConnectionManager.closeResultSet(queryResultList);
				ConnectionManager.closeStatement(st);
				ConnectionManager.releaseConnection(connection,
						schedulerConstants.HIVE_DATABASE);
			} catch (Exception e) {
				LOGGER.warn("Exception while clossing the connection {}",
						e.getMessage());
			}
		}
		LOGGER.debug("Retrieved the HDFS location : {} for the table name: {}",
				hdfsLocationList, tableName);
		return hdfsLocationList;
	}

	public static List<String> getSpecIDVerFromTabName(String tableName) {
		if (tableName.contains(PERIOD)) {
			tableName = tableName.substring(tableName.indexOf(PERIOD));
		}
		String specID = tableName.substring(tableName.indexOf(UNDERSCORE) + 1,
				tableName.lastIndexOf(UNDERSCORE));
		String specVersion = tableName
				.substring(tableName.lastIndexOf(UNDERSCORE) + 1);
		List<String> strList = new ArrayList<>();
		strList.add(specID);
		strList.add(specVersion);
		LOGGER.debug("Spec ID:{}", specID);
		LOGGER.debug("Spec Version:{}", specVersion);
		return strList;
	}

	public static String getHiveExportDirectory() {
		GeneratorPropertyQuery genPropQuery = new GeneratorPropertyQuery();
		return genPropQuery.retrieve("EXP_RITHOMAS_EXPORT_DIR",
				"/var/local/hive-export/");
	}
}
