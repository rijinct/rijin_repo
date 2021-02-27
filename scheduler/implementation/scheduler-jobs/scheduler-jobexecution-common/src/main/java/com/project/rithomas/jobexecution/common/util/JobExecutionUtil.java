
package com.project.rithomas.jobexecution.common.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class JobExecutionUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(JobExecutionUtil.class);

	public static String getExecutionEngine(WorkFlowContext context) {
		String executionEngine = (String) context
				.getProperty(JobExecutionContext.EXECUTION_ENGINE);
		LOGGER.debug("executionEngine:{}", executionEngine);
		return executionEngine == null ? schedulerConstants.HIVE_DATABASE
				: executionEngine.toUpperCase();
	}

	public static List<String> readSparkResultFile(WorkFlowContext context,
			String outputDirName) throws JobExecutionException {
		GetDBResource dbResource = GetDBResource.getInstance();
		Configuration conf = dbResource.getHdfsConfiguration();
		List<String> fileContent = new ArrayList<String>();
		FileSystem fileSystem = null;
		String hdfsLocation = "/spark/result/".concat(outputDirName);
		try {
			fileSystem = FileSystem.get(conf);
			FileStatus[] fileStatuses = fileSystem
					.listStatus(new Path(hdfsLocation));
			for (FileStatus fileStatus : fileStatuses) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fileSystem.open(fileStatus.getPath())));
				try {
					String line;
					line = br.readLine();
					while (line != null) {
						fileContent.add(line);
						line = br.readLine();
					}
				} finally {
					br.close();
				}
			}
		} catch (IOException e) {
			throw new JobExecutionException(
					"Exception while connecting to hdfs", e);
		} finally {
			try {
				if (fileSystem != null) {
					fileSystem.delete(new Path(hdfsLocation), true);
				}
			} catch (IllegalArgumentException | IOException e) {
				throw new JobExecutionException(
						"Exception while deleting hdfs file "
								.concat(hdfsLocation),
						e);
			} finally {
				try {
					if (fileSystem != null) {
						fileSystem.close();
					}
				} catch (IOException e) {
					throw new JobExecutionException(
							"Exception while closing file system object", e);
				}
			}
		}
		return fileContent;
	}

	public static void runQueryHints(schedulerJobRunner jobRunner,
			List<String> queryHints) throws SQLException {
		jobRunner.setQueryHints(queryHints);
	}
}
