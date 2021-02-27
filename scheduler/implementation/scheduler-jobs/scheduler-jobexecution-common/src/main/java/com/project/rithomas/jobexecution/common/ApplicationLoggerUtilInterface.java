
package com.project.rithomas.jobexecution.common;

import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;

public interface ApplicationLoggerUtilInterface {

	Thread startApplicationIdLog(Thread parentThread, Statement stmnt,
			String jobId, String sleepTime);

	void stopApplicationIdLog(Thread logThread);

	void killApplication(Configuration conf, String jobId);

	boolean getRowCountStatus();

	long getRowCount();
}
