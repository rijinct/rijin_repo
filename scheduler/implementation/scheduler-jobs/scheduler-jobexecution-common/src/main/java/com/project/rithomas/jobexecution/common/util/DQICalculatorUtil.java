
package com.project.rithomas.jobexecution.common.util;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.etl.exception.ETLException;
import com.project.rithomas.etl.hdfs.HDFSClient;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class DQICalculatorUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DQICalculatorUtil.class);

	private static final String DQI_LOCATION = "/rithomas/ps/dqi/";

	private static final String UNDERSCORE = "_";

	private static final String FILE_SEPARATOR = "/";

	public HDFSClient getHDFSClient(WorkFlowContext context) {
		Configuration conf = GetDBResource.getInstance().getHdfsConfiguration();
		HDFSClient hdfsClient = new HDFSClient(conf);
		return hdfsClient;
	}

	public QueryExecutor getQueryExecutor() {
		return new QueryExecutor();
	}

	public String getCacheKey(String regionId, String adaptationId,
			String adaptationVersion, String usageSpecName) {
		String key = JobExecutionContext.DATA_AVBL_CACHE_KEY;
		if (regionId != null) {
			key = JobExecutionContext.DATA_AVBL_CACHE_KEY_TIMEZONE
					.replace(JobExecutionContext.TIMEZONE_REGION, regionId);
		}
		key = key
				.replace(JobExecutionContext.ADAPTATION_ID_VERSION,
						adaptationId + "_" + adaptationVersion)
				.replace(JobExecutionContext.USAGE_SPECIFICATION_ID_VERSION,
				usageSpecName);
		return key;
	}

	@SuppressWarnings("rawtypes")
	public void writeToDQITable(WorkFlowContext context, Statement st, Long lb,
			double dqi, String regionId, String description, String jobName,
			String adaptationPLevelStr)
			throws ETLException, JobExecutionException, SQLException {
		String dqiTableName = (String) context
				.getProperty(JobExecutionContext.DQI_TABLE_NAME);
		if (dqiTableName == null) {
			dqiTableName = "di_" + adaptationPLevelStr;
		}
		context.setProperty(JobExecutionContext.DQI_TABLE_NAME, dqiTableName);
		String adaptationId = (String) context
				.getProperty(JobExecutionContext.ADAPTATION_ID);
		String adaptationVersion = (String) context
				.getProperty(JobExecutionContext.ADAPTATION_VERSION);
		String query = "select kpi_name from perf_spec_kpi_list where job_name='"
				+ jobName + "'";
		QueryExecutor queryExecutor = getQueryExecutor();
		List kpiList = queryExecutor.executeMetadatasqlQueryMultiple(query,
				context);
		List<String[]> records = new ArrayList<String[]>();
		Double dqiVal = Math.round(dqi * 100.0) / 100.0;
		SimpleDateFormat df = new SimpleDateFormat(
				JobExecutionContext.DATEFORMAT);
		Date d = new Date(lb);
		String reportTime = df.format(d);
		String region = regionId != null ? regionId : "\\N";
		if (kpiList != null && !kpiList.isEmpty()) {
			LOGGER.debug("kpi list size : {}", kpiList.size());
			LOGGER.debug("KPI list: {}", kpiList);
			for (Object itObj : kpiList) {
				String kpiName = itObj.toString();
				String[] dqiRow = { reportTime, kpiName,
						Double.toString(dqiVal), region, description };
				LOGGER.debug("dqi Row added : {}", Arrays.toString(dqiRow));
				records.add(dqiRow);
			}
		} else {
			String[] dqiRow = { reportTime, "\\N", Double.toString(dqiVal),
					region, description };
			LOGGER.debug("dqi Row added : {}", Arrays.toString(dqiRow));
			records.add(dqiRow);
		}
		String hdfsFilePath = DQI_LOCATION + adaptationId + UNDERSCORE
				+ adaptationVersion + FILE_SEPARATOR + adaptationPLevelStr
				+ FILE_SEPARATOR + "dt=" + lb;
		HDFSClient hdfsClient = getHDFSClient(context);
		hdfsClient.mkdir(hdfsFilePath);
		final String sql = "msck repair table " + dqiTableName;
		if (regionId != null) {
			context.setProperty(
					JobExecutionContext.HIVE_SESSION_QUERY + "_" + regionId,
					sql);
		} else {
			context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY, sql);
		}
		st.execute(sql);
		String fileName = hdfsFilePath + FILE_SEPARATOR + adaptationPLevelStr
				+ UNDERSCORE + lb;
		if (regionId != null) {
			fileName = fileName + regionId;
		}
		if (context.getProperty(JobExecutionContext.REAGG_LAST_LB) != null
				&& !hdfsClient.fileExists(fileName)) {
			// Re-aggregation is done but dqi file is not found. Indicates
			// aggregation was done without dqm enabled. So no need to update
			// dqi values
			LOGGER.info(
					"Not updating dqi values for re-aggregation as dqi file is not found to be updated.");
		} else {
			// delete if the file exists to write the new file with updated dqi
			// values.
			hdfsClient.deleteFile(fileName);
			hdfsClient.createRCFiles(records, fileName, null);
			LOGGER.info("DQI values calculated and updated.");
		}
	}

	public boolean checkIfDQIAlreadyCalculated(WorkFlowContext context, Long lb,
			String regionId, String adaptationPLevelStr) throws ETLException {
		String adaptationId = (String) context
				.getProperty(JobExecutionContext.ADAPTATION_ID);
		String adaptationVersion = (String) context
				.getProperty(JobExecutionContext.ADAPTATION_VERSION);
		String hdfsFilePath = DQI_LOCATION + adaptationId + UNDERSCORE
				+ adaptationVersion + FILE_SEPARATOR + adaptationPLevelStr
				+ FILE_SEPARATOR + "dt=" + lb;
		HDFSClient hdfsClient = getHDFSClient(context);
		String fileName = hdfsFilePath + FILE_SEPARATOR + adaptationPLevelStr
				+ UNDERSCORE + lb;
		if (regionId != null) {
			fileName = fileName + regionId;
		}
		return hdfsClient.fileExists(fileName);
	}
}
