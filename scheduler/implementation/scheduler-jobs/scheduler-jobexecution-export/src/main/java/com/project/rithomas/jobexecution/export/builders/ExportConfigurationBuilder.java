
package com.project.rithomas.jobexecution.export.builders;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.rijin.analytics.sdk.export.model.ExportConfiguration;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.export.ExportManagerUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class ExportConfigurationBuilder {

	private static final int DEFAULT_COMPRESSION_SIZE_IN_BYTES = 1024;

	public static final String EXPORT_FILE_EXTENSION = "FILEEXTENSION";

	private static final String EXPORT_JOB_PREFIX = "Exp_";

	private static final String EXPORT_JOB_SUFFIX = "_ExportJob";

	private static final String FILE_SEP = System.getProperty("file.separator",
			"/");

	private static final String UNDERSCORE = "_";

	private ExportConfiguration configuration = new ExportConfiguration();

	public ExportConfiguration build(WorkFlowContext context) {
		toggleCompression(context);
		toggleEncryption(context);
		toggleMergeFiles(context);
		setJobName(context);
		setFileExtension(context);
		setNumberOfBytes(context);
		setLocalDirectory(context);
		setHdfsExportDirectory(context);
		setColumnHeaderNames(context);
		return configuration;
	}

	private void toggleEncryption(WorkFlowContext context) {
		String exportEncryption = (String) context
				.getProperty(JobExecutionContext.EXPORT_ENCRYPTION);
		configuration.setEncryptionRequired(toBoolean(exportEncryption));
	}

	private void toggleCompression(WorkFlowContext context) {
		String exportCompression = (String) context
				.getProperty(JobExecutionContext.EXPORT_COMPRESSION);
		configuration.setCompressionRequired(toBoolean(exportCompression));
	}

	private void toggleMergeFiles(WorkFlowContext context) {
		String mergeFiles = (String) context
				.getProperty(JobExecutionContext.EXPORT_MERGE);
		configuration.setShouldMergeFiles(toBoolean(mergeFiles));
	}

	private void setLocalDirectory(WorkFlowContext context) {
		String exportLocation = (String) context
				.getProperty(JobExecutionContext.EXPORT_LOCATIONS);
		configuration.setLocalDirectory(exportLocation);
	}

	public void setFileExtension(WorkFlowContext context) {
		String fileExtension = (String) context
				.getProperty(EXPORT_FILE_EXTENSION);
		configuration.setFileExtension(fileExtension);
	}

	public void setNumberOfBytes(WorkFlowContext context) {
		String bytes = (String) context
				.getProperty(JobExecutionContext.EXPORT_COMPRESSION_BYTE);
		configuration.setCompressionBytes(
				NumberUtils.toInt(bytes, DEFAULT_COMPRESSION_SIZE_IN_BYTES));
	}

	public void setFileName(String fileName) {
		configuration.setFileName(fileName);
	}

	private void setJobName(WorkFlowContext context) {
		String fullJobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String jobName = StringUtils.substringBetween(fullJobName,
				EXPORT_JOB_PREFIX, EXPORT_JOB_SUFFIX);
		configuration.setJobName(jobName);
	}

	private void setHdfsExportDirectory(WorkFlowContext context) {
		String baseHdfsExportDirectory = (String) context
				.getProperty(JobExecutionContext.EXPORT_DIRECTORY);
		String adaptationID = (String) context
				.getProperty(JobExecutionContext.ADAPTATION_ID);
		String adaptationVersion = (String) context
				.getProperty(JobExecutionContext.ADAPTATION_VERSION);
		String hdfsExportDirectory = addFileSeparator(baseHdfsExportDirectory)
				+ adaptationID + UNDERSCORE + adaptationVersion + FILE_SEP
				+ configuration.getJobName() + UNDERSCORE + "temp";
		configuration.setHdfsExportDirectory(hdfsExportDirectory);
	}

	private String addFileSeparator(String tempExportPath) {
		String newTempExpPath = tempExportPath;
		if (!tempExportPath.endsWith(FILE_SEP)) {
			newTempExpPath = tempExportPath + FILE_SEP;
		}
		return newTempExpPath;
	}

	private void setColumnHeaderNames(WorkFlowContext context) {
		String columnNames = "";
		String exportColumnHeader = (String) context
				.getProperty(JobExecutionContext.EXPORT_COLUMN_HEADER);
		if (toBoolean(exportColumnHeader)) {
			columnNames = ExportManagerUtil.getColumnNames(context);
		}
		configuration.setColumnHeaderNames(columnNames);
	}
}
