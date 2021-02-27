
package com.rijin.analytics.scheduler.k8s;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public final class HealthCheckUtils {

	private static volatile QueryExecutor executor;

	private static volatile WorkFlowContext context;

	private static volatile Configuration conf;

	private static volatile FileSystem fileSystem;

	private HealthCheckUtils() {
		throw new IllegalStateException("HealthCheckUtils.class");
	}

	private static void initializePostgresConf() throws JobExecutionException {
		if (executor == null || context == null) {
			executor = new QueryExecutor();
			context = new JobExecutionContext();
			GetDBResource.getInstance()
					.retrievePostgresObjectProperties(context);
		}
	}

	private static void initializeHdfsConf() throws IOException {
		if (conf == null || fileSystem == null) {
			conf = GetDBResource.getInstance().getHdfsConfiguration();
			fileSystem = FileSystem.get(conf);
		}
	}

	public static Object[] executeQuery(String query)
			throws JobExecutionException {
		initializePostgresConf();
		return executor.executeMetadatasqlQuery(query, context);
	}

	public static List executeMultiQuery(String query)
			throws JobExecutionException {
		initializePostgresConf();
		return executor.executeMetadatasqlQueryMultiple(query, context);
	}

	public static String getFolderModifiedTimestamp(String directory)
			throws IOException {
		initializeHdfsConf();
		FileStatus[] status = fileSystem
				.listStatus(new Path(directory).getParent());
		return DateFunctionTransformation.getInstance()
				.getFormattedDate(status[0].getModificationTime());
	}
}
