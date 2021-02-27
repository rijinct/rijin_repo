
package com.project.rithomas.jobexecution.entity;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.etl.common.ETLConstants;
import com.project.rithomas.etl.dimension.exception.DimensionLoadingException;
import com.project.rithomas.etl.dimension.exception.ETLDimensionLoaderInitializationException;
import com.project.rithomas.etl.dimension.service.ETLDimensionLoader;
import com.project.rithomas.etl.exception.ETLServiceInitializerException;
import com.project.rithomas.etl.service.ETLServiceInitializer;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ProcessUtil;
import com.project.rithomas.jobexecution.common.util.StringModificationUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.deployment.util.ProcessBuilderHelper;

public class ETLDimensionLoaderJob extends AbstractWorkFlowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ETLDimensionLoaderJob.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean jobExecuted = false;
		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		Process process = null;
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String schedulerBinDirectory = SDKSystemEnvironment
				.getschedulerBinDirectory();
		LOGGER.debug("scheduler bin directory {}", schedulerBinDirectory);
		File directory = StringModificationUtil
				.getFileWithCanonicalPath(schedulerBinDirectory);
		LOGGER.debug("scheduler bin directory full path {} ", directory);
		// query the metadata for this job from etl service
		ETLServiceInitializer etlServiceInitializer = new ETLServiceInitializer();
		ETLDimensionLoader dimensionLoader = null;
		Multimap<String, String> adapMapForImportDirAndWorkFilepath = ArrayListMultimap
				.create();
		try {
			List<Map<String, Object>> metadataMapList = etlServiceInitializer
					.retrieveMetadataOfEntitySpec(jobName);
			if (metadataMapList != null && !metadataMapList.isEmpty()) {
				dimensionLoader = new ETLDimensionLoader();
				for (Map<String, Object> metadataMapObj : metadataMapList) {
					ProcessBuilderHelper executor = new ProcessBuilderHelper();
					String importDir = metadataMapObj
							.get(ETLConstants.ADAP_IMPORT_DIR).toString();
					LOGGER.debug("Adaptation directory {}", importDir);
					if(!validateFileExtension(importDir)){
						LOGGER.error("File name cannot contain multiple dot characters. Rename the files and rerun the Job");
						throw new WorkFlowExecutionException(
								"Incorrect File Name");
					}
					List<String> workingFilepathList = new ArrayList<>();
					if(checkPresenceOfEncodingScript(directory+"/file_encoding_conversion.sh")){
						LOGGER.info("File encoding started...");
						List<String> command = new ArrayList<>();
						command.add("./file_encoding_conversion.sh");
						command.add(importDir);
						process = executor.executeCommand(command, directory);
						Integer exitValue = ProcessUtil.getExitValue(process, 120);
						if (exitValue == 0) {
							LOGGER.info("Pre-processing summary: {}",
									getScriptOutput(process.getInputStream(), isr,
										br));
						} else {
							List<String> errorOutput = getScriptOutput(
								process.getErrorStream(), isr, br);
							LOGGER.error(errorOutput.toString());
							throw new WorkFlowExecutionException(
									"Could not encode the files.");
						}
					}
					deleteFilesInHDFSWorkDir(context, metadataMapObj);
					dimensionLoader.initialize(metadataMapObj);
					dimensionLoader.loadESData(workingFilepathList);
					adapMapForImportDirAndWorkFilepath.putAll(importDir,
							workingFilepathList);
				}
			}
			context.setProperty(
					JobExecutionContext.ADAP_MAP_IMPORT_DIR_AND_WORK_FILE_PATH_LIST,
					adapMapForImportDirAndWorkFilepath);
			jobExecuted = true;
		} catch (ETLServiceInitializerException e) {
			LOGGER.error("Could not start dimension loading job:" + jobName
					+ ". Cause:" + e.getMessage(), e);
			updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Could not start dimension loading job:" + jobName
							+ ". Cause:" + e.getMessage(),
					e);
		} catch (ETLDimensionLoaderInitializationException e) {
			LOGGER.error("Could not start dimension loading job:" + jobName
					+ ". Cause:" + e.getMessage(), e);
			updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Could not start dimension loading job:" + jobName
							+ ". Cause:" + e.getMessage(),
					e);
		} catch (DimensionLoadingException e) {
			LOGGER.error("Could not finish dimension loading job:" + jobName
					+ ". Cause:" + e.getMessage(), e);
			updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Could not finish dimension loading job:" + jobName
							+ ". Cause:" + e.getMessage(),
					e);
		} catch (IOException e) {
			LOGGER.error("Could not finish dimension loading job:" + jobName
					+ ". Cause:" + e.getMessage(), e);
			updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Could not finish dimension loading job:" + jobName
							+ ". Cause:" + e.getMessage(),
					e);
		} catch (Exception e) {
			LOGGER.error("Could not finish dimension loading job:" + jobName
					+ ". Cause:" + e.getMessage(), e);
			updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Could not finish dimension loading job:" + jobName
							+ ". Cause:" + e.getMessage(),
					e);
		} finally {
			if (dimensionLoader != null) {
				dimensionLoader.closeResources();
			}
			LOGGER.info(
					"Finished ETL Dimension Loader Job. Job name: {}, Job Execution status: {}",
					jobName, jobExecuted);
			try {
				close(isr);
				close(is);
				if (process != null) {
					close(process.getOutputStream());
					close(process.getInputStream());
					close(process.getErrorStream());
					process.destroy();
				}
			} catch (IOException e) {
				LOGGER.warn("Error occured while closing the stream. Cause: "
						+ e.getMessage(), e);
			}
		}
		return jobExecuted;
	}

	private boolean checkPresenceOfEncodingScript(String filePath) {
		File file = StringModificationUtil.getFileWithCanonicalPath(filePath);
		return file.exists() && file.isFile();
	}

	private List<String> getScriptOutput(InputStream is, InputStreamReader isr,
			BufferedReader br) throws IOException {
		isr = new InputStreamReader(is, StandardCharsets.UTF_8);
		br = new BufferedReader(isr);
		String line;
		List<String> scriptOutput = new ArrayList<>();
		while ((line = br.readLine()) != null) {
			scriptOutput.add(line);
		}
		return scriptOutput;
	}

	private void deleteFilesInHDFSWorkDir(WorkFlowContext context,
			Map<String, Object> metadataObjMap) throws IOException {
		String workDir = (String) metadataObjMap
				.get(ETLConstants.HDFS_TMP_DIR_PATH);
		LOGGER.debug("Work directory to be cleaned up: {}", workDir);
		if (workDir != null) {
			LOGGER.info(
					"Cleaning up the work directory: {}, if any files are found..",
					workDir);
			Configuration hdfsConfig = GetDBResource.getInstance()
					.getHdfsConfiguration();
			// Cleanup hdfs work directory before loading.
			FileSystem fs = FileSystem.get(hdfsConfig);
			FileStatus[] fileStatusList = fs.listStatus(new Path(workDir));
			if (fileStatusList != null && fileStatusList.length > 0) {
				LOGGER.debug("Files found inside work directory..");
				for (FileStatus fileStatus : fileStatusList) {
					LOGGER.debug("Deleting file: {}", fileStatus.getPath());
					fs.delete(fileStatus.getPath(), false);
				}
			}
		}
	}

	private void updateETLErrorStatusInTable(Exception e,
			WorkFlowContext context) throws WorkFlowExecutionException {
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		LOGGER.info("Updating ETL Error Status in Table. Cause:"
				+ e.getMessage() + " JOB NAME:" + jobName);
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		try {
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			LOGGER.info("Updated ETL Error Status in Table. Cause:"
					+ e.getMessage() + " context:");
		} catch (WorkFlowExecutionException e1) {
			LOGGER.error("Could not update ETL Error status in table for job:"
					+ jobName, e1);
		}
	}

	private void close(Closeable c) throws IOException {
		if (c != null) {
			c.close();
		}
	}
	
	private boolean validateFileExtension(String importDirectory) {
		File inputDirectoryFile = StringModificationUtil
				.getFileWithCanonicalPath(importDirectory);
		File[] files = inputDirectoryFile.listFiles();
		if (files != null && files.length > 0) {
			for (File file : files) {
				String fileName = file.getName()
						.replace(ETLConstants.PROCESSING_FILE_EXTENSION, "");
				long count = fileName.chars().filter(num -> num == '.').count();
				if (count > 1) {
					return false;
				}
			}
		}
		return true;
	}

}
