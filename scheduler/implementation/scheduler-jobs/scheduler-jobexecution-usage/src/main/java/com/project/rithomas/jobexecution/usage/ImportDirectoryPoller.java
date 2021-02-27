
package com.project.rithomas.jobexecution.usage;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.JobProperty;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.model.meta.query.JobPropertyQuery;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ImportDirectoryPoller extends AbstractWorkFlowStep {

	private FileSystem fileSystem;

	private final static long WAIT_INTERVAL = 15000L;

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ImportDirectoryPoller.class);

	private final static boolean DEBUGMODE = LOGGER.isDebugEnabled();

	private Set<String> directories;

	private boolean commitDone;

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String status = (String) context
				.getProperty(JobExecutionContext.PREV_STATUS);
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		LOGGER.info("Checking for files for job {}", jobName);
		this.directories = new HashSet<String>();
		this.commitDone = false;
		boolean previousFailed = false;
		boolean firstRun = true;
		boolean deleted = false;
		boolean found = false;
		try {
			GetDBResource.getInstance().retrievePostgresObjectProperties(context);
			String importDir = (String) context
					.getProperty(JobExecutionContext.IMPORT_PATH);
			if (StringUtils.isNotEmpty(importDir)) {
				LOGGER.info("Import dir for Job {} is {}", jobName,
						importDir);
				this.fileSystem = FileSystem.get(GetDBResource.getInstance()
						.getHdfsConfiguration());
				if (DEBUGMODE) {
					LOGGER.debug(
							"Obtained hdfs file system for file monitoring.");
				}
				Path importDirPath = new Path(importDir);
				Path workDirPath = new Path(
						importDir.replace("import", "work"));
				// check for previous jobs status from context
				if (status != null && !status.isEmpty()) {
					if (status.equalsIgnoreCase("E")) {
						if (DEBUGMODE) {
							LOGGER.debug("Previous instance of Job {} failed.",
									jobName);
						}
						previousFailed = true;
					} else if (status.equalsIgnoreCase(
							JobExecutionContext.COMMIT_DONE)) {
						if (DEBUGMODE) {
							LOGGER.debug("Commit done for previous instance.");
						}
						commitDone = true;
					}
				}
				while (!found) {
					// check for files in import dir
					this.checkForFilesInDir(importDirPath, false, context);
					// check for files in work dir if previous data was not
					// committed
					if (previousFailed && firstRun) {
						this.checkForFilesInDir(workDirPath, false, context);
						firstRun = false;
					}
					// delete files in work dir if previous deletion failed
					if (commitDone && !deleted) {
						this.checkForFilesInDir(workDirPath, true, context);
						deleted = true;
					}
					if (this.directories != null && !directories.isEmpty()) {
						found = true;
						List<JobProperty> jobPropList = getJobProp(jobName,
								context);
						if (CollectionUtils.isNotEmpty(jobPropList)) {
							for (JobProperty jobProp : jobPropList) {
								context.setProperty(
										jobProp.getJobPropPK().getParamname(),
										jobProp.getParamvalue());
								LOGGER.info(
										jobProp.getJobPropPK().getParamname()
												+ " : {}",
										jobProp.getParamvalue());
							}
						}
						context.setProperty(JobExecutionContext.IMPORT_DIR_PATH,
								this.directories);
						context.setProperty(JobExecutionContext.DESCRIPTION,
								"");
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION, "");
						context.setProperty(JobExecutionContext.STATUS, "R");
						updateJobStatus.updateFinalJobStatus(context);
					} else {
						context.setProperty(JobExecutionContext.DESCRIPTION,
								"Waiting for files");
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION, "");
						context.setProperty(JobExecutionContext.STATUS, "I");
						updateJobStatus.updateFinalJobStatus(context);
						Thread.sleep(WAIT_INTERVAL);
					}
				}
			} else {
				throw new WorkFlowExecutionException(
						"Entry not present for job " + jobName
								+ " in TNES_TYPES table.");
			}
		} catch (IOException e) {
			context.setProperty(JobExecutionContext.STATUS, "E");
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
			throw new WorkFlowExecutionException(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOGGER.warn("Interrupted exception occured.", e.getMessage());
			throw new WorkFlowExecutionException(e.getMessage(), e);
		} catch (JobExecutionException e) {
			LOGGER.error("Exception occurred while retrieving postgres configuration details", e);
			throw new WorkFlowExecutionException("Exception occurred while retrieving postgres configuration details", e);
		} finally {
			if (!found) {
				new UpdateJobStatus().updateFinalJobStatus(context);
			}
		}
		return found;
	}

	private void checkForFilesInDir(Path path, boolean deleteFiles,
			WorkFlowContext context) throws IOException {
		if (!this.getFileSystem().exists(path)) {
			LOGGER.error("Path {} does not exist in hdfs.", path);
		}
		if (DEBUGMODE) {
			LOGGER.debug("Checking for files in dir {} in hdfs.", path);
		}
		FileStatus[] dirStatus = this.fileSystem.listStatus(path);
		if (dirStatus != null && dirStatus.length != 0) {
			for (FileStatus dir : dirStatus) {
				if (dir.isDirectory()) {
					if (DEBUGMODE) {
						LOGGER.debug("Current directory : {} ", dir.getPath());
					}
					if (commitDone && deleteFiles) {
						this.fileSystem.delete(dir.getPath(), true);
						if (DEBUGMODE) {
							LOGGER.debug(
									"Deleted directory {} as part of cleanup.",
									dir.getPath());
						}
						break;
					}
					if (TimeZoneUtil.isTimeZoneEnabled(context)
							|| (TimeZoneUtil.isDefaultTZSubPartition(context)
									&& path.toString().contains("work"))) {
						LOGGER.debug(
								"timezone is enabled or default sub-partition in work");
						FileStatus[] regionFileStatus = this.fileSystem
								.listStatus(dir.getPath());
						if (regionFileStatus != null
								&& regionFileStatus.length != 0) {
							for (FileStatus tmpFile : regionFileStatus) {
								if (tmpFile.isDirectory()) {
									LOGGER.debug(
											"region dir exists and path is {}",
											tmpFile.getPath());
									FileStatus[] fileStatus = getFileStatusWithFilter(
											tmpFile.getPath());
									if (fileStatus != null
											&& fileStatus.length != 0) {
										for (FileStatus file : fileStatus) {
											if (file.isDirectory()) {
												LOGGER.debug("directory: {}",
														file.getPath());
												continue;
											} else {
												LOGGER.debug(
														"Files are found: {}",
														file.getPath());
												this.directories.add(tmpFile
														.getPath().toString());
												break;
											}
										}
									} else {
										if (DEBUGMODE) {
											LOGGER.debug(
													"No files present in directory {}.",
													tmpFile.getPath());
										}
									}
								}
							}
						}
					} else {
						LOGGER.debug("when timezone is not set");
						FileStatus[] fileStatus = this.fileSystem
								.listStatus(dir.getPath(), new PathFilter() {

									@Override
									public boolean accept(Path path) {
										return path.toString().endsWith(".dat");
									}
								});
						if (fileStatus != null && fileStatus.length != 0) {
							for (FileStatus file : fileStatus) {
								if (file.isDirectory()) {
									LOGGER.debug("region dir: {}",
											file.getPath());
									continue;
								} else {
									if (DEBUGMODE) {
										LOGGER.debug(
												"Found files in directory {}.",
												dir.getPath());
									}
									this.directories
											.add(dir.getPath().toString());
									break;
								}
							}
						} else {
							if (DEBUGMODE) {
								LOGGER.debug(
										"No files present in directory {}.",
										dir.getPath());
							}
						}
					}
				}
			}
		}
		// Don't close HDFS. Its a single instance from Hadoop. Not
		// required to be closed, else others using HDFS will fail
	}

	private FileSystem getFileSystem() {
		return this.fileSystem;
	}

	private FileStatus[] getFileStatusWithFilter(Path path) throws IOException {
		FileStatus[] fileStatus = this.fileSystem.listStatus(path,
				new PathFilter() {
					@Override
					public boolean accept(Path path) {
						return path.toString().endsWith(".dat");
					}
				});
		return fileStatus;
	}

	protected List<JobProperty> getJobProp(String jobName,
			WorkFlowContext context) {
		JobDictionaryQuery jobDictQuery = new JobDictionaryQuery();
		JobPropertyQuery jobPropQuery = new JobPropertyQuery();
		JobDictionary jobId = jobDictQuery.retrieve(jobName);
		List<JobProperty> jobPropList = null;
		if (jobId != null) {
			context.setProperty(JobExecutionContext.ADAPTATION_ID,
					jobId.getAdaptationId());
			context.setProperty(JobExecutionContext.ADAPTATION_VERSION,
					jobId.getAdaptationVersion());
			jobPropList = jobPropQuery.retrieve(jobId.getId());
		}
		return jobPropList;
	}
}
