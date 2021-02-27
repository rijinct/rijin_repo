
package com.project.rithomas.jobexecution.export.executors;

import java.io.IOException;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.HiveToHBaseLoader;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ExportToHbaseExecutor {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ExportToHbaseExecutor.class);

	public void execute(WorkFlowContext context, Long lb, Long nextLb)
			throws WorkFlowExecutionException, IOException {
		LOGGER.info(
				"Hive to Hbase Loading is enabled. Exporting data to Hbase");
		HiveToHBaseLoader hbaseLoader = new HiveToHBaseLoader();
		hbaseLoader.execute(context, lb, nextLb);
	}
}
