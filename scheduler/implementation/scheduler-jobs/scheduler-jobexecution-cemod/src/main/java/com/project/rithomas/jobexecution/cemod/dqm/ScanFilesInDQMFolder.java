
package com.project.rithomas.jobexecution.project.dqm;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.AgeFileFilter;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.StringModificationUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.DeploymentDictionary;
import com.project.rithomas.sdk.model.meta.GeneratorProperty;
import com.project.rithomas.sdk.model.meta.query.GeneratorPropertyQuery;
import com.project.rithomas.sdk.model.others.DeploymentDictionaryConstants;
import com.project.rithomas.sdk.model.others.EntityType;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

public class ScanFilesInDQMFolder extends AbstractWorkFlowStep {

	private String dqmFolder = null;

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ScanFilesInDQMFolder.class);

	private static final Character UNDERSCORE = '_';

	private static final Character SEPARATOR = '/';

	public static final String PROCESS_MONITOR_HDFS_LOC = "/rithomas/common/process_monitor/";

	private static final String FILE_NAME_PREFIX = "pm_";

	private static final String FILE_NAME_SUFFIX = ".dqm";

	private static final String DEFAULT_RETENTIONTIME_FOR_DQM_FILES = "5";

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean status = false;
		boolean filesCopied = false;
		dqmFolder = getDQMFolder(context);
		List<String> deployedUsageDirs = getDeployedUsageSpecDirs(context);
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		int retentionDays = Integer.parseInt((String) context
				.getProperty(JobExecutionContext.RETENTIONDAYS));
		String retentionTimeForDQMFiles = (String) context
				.getProperty(JobExecutionContext.RETENTION_TIME_FOR_DQM_FILES);
		if (retentionTimeForDQMFiles == null) {
			retentionTimeForDQMFiles = DEFAULT_RETENTIONTIME_FOR_DQM_FILES;
		}
		Calendar calendar = Calendar.getInstance();
		Date date = new Date();
		long dateInUnixFormat = date.getTime();
		calendar.setTimeInMillis(dateInUnixFormat);
		calendar.add(Calendar.DAY_OF_MONTH, -retentionDays);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		try {
			Configuration configuration = GetDBResource.getInstance()
					.getHdfsConfiguration();
			FileSystem fs = FileSystem.get(configuration);
			for (String usageDir : deployedUsageDirs) {
				File dir = StringModificationUtil
						.getFileWithCanonicalPath(dqmFolder + usageDir);
				if (dir.exists() && dir.isDirectory()) {
					LOGGER.info("Moving dqm files (if any) for the folder: {}",
							dir.getAbsolutePath());
					long cutoff = System.currentTimeMillis()
							- (Integer.parseInt(retentionTimeForDQMFiles) * 60
									* 1000);
					Collection files = FileUtils.listFiles(dir,
							(IOFileFilter) new AgeFileFilter(cutoff, true),
							DirectoryFileFilter.DIRECTORY);
					LOGGER.debug("No. of files in the collection: {}",
							files.size());
					File outFileName = StringModificationUtil
							.getFileWithCanonicalPath(
									dqmFolder + FILE_NAME_PREFIX
											+ System.currentTimeMillis()
											+ FILE_NAME_SUFFIX);
					
					java.nio.file.Path outFile = outFileName.toPath();
					FileChannel out = null;
					try {
						if (!files.isEmpty()) {
							out = FileChannel.open(outFile,
									StandardOpenOption.CREATE,
									StandardOpenOption.WRITE);
							for (Object obj : files) {
								File file = (File) obj;
								LOGGER.debug("File in the dqm folder: {}",
										file);
								mergeSmallFilesToOutFile(out, file);
							}
						}
					} finally {
						if (out != null) {
							out.close();
						}
					}
					if (moveMergedFileToHdfs(fs, outFileName.getAbsolutePath(), outFile)) {
						filesCopied = true;
					}
				} else {
					LOGGER.debug("Directory {} doesn't exist", dir);
				}
			}
			if (filesCopied) {
				context.setProperty(GeneratorWorkFlowContext.DESCRIPTION,
						".dqm files copied to the process monitor table");
			} else {
				context.setProperty(GeneratorWorkFlowContext.DESCRIPTION,
						"No .dqm files to copy to the process monitor table.");
			}
			Path path = new Path(PROCESS_MONITOR_HDFS_LOC);
			FileStatus filestatusArray[] = fs.listStatus(path);
			for (FileStatus fileStatus : filestatusArray) {
				long unixTSofFile = fileStatus.getModificationTime();
				Date dateFile = new Date(unixTSofFile);
				Path filePath = fileStatus.getPath();
				int comp = dateFile.compareTo(calendar.getTime());
				if (comp < 0) {
					LOGGER.info(
							"File {} was created before {}. Hence deleting the file.",
							filePath, calendar.getTime());
					fs.delete(filePath, false);
				}
			}
			status = true;
		} catch (IOException e) {
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"IOException: " + e.getMessage(), e);
		} finally {
			if (!status) {
				context.setProperty(JobExecutionContext.DESCRIPTION,
						"Error while copying .dqm files to the hdfs directory.");
				context.setProperty(JobExecutionContext.STATUS, "E");
				updateJobStatus.updateFinalJobStatus(context);
			}
		}
		return status;
	}

	private boolean moveMergedFileToHdfs(FileSystem fs, String outFileName,
			java.nio.file.Path outFile) throws IOException {
		boolean filesCopied = false;
		File file = StringModificationUtil
				.getFileWithCanonicalPath(outFileName);
		if (file.exists() && Files.size(outFile) > 0) {
			LOGGER.info("File to be copied to HDFS: {}", file);
			fs.copyFromLocalFile(true, true, new Path(file.getAbsolutePath()),
					new Path(PROCESS_MONITOR_HDFS_LOC + file.getName()));
			filesCopied = true;
		} else if (file.exists()) {
			boolean deletionStatus = file.delete();
			if (!deletionStatus) {
				LOGGER.warn("File deletion failed");
			}
		}
		return filesCopied;
	}

	private void mergeSmallFilesToOutFile(FileChannel out, File smallFile)
			throws IOException {
		if (!smallFile.getAbsolutePath().contains("_TEMP/")
				&& smallFile.getAbsolutePath().endsWith(FILE_NAME_SUFFIX)) {
			java.nio.file.Path inFile = Paths.get(smallFile.getAbsolutePath());
			FileChannel in = null;
			boolean merged = false;
			try {
				in = FileChannel.open(inFile, StandardOpenOption.READ);
				for (long p = 0, l = in.size(); p < l;) {
					p += in.transferTo(p, l - p, out);
				}
				merged = true;
			} catch (IOException ex) {
				LOGGER.warn("Unable to copy the file: {}. Reason: {}",
						smallFile.getAbsolutePath(), ex);
			} finally {
				if (in != null) {
					in.close();
				}
				if (merged && !smallFile.delete()) {
					LOGGER.warn("Unable to delete small files");
				}
			}
		}
	}

	protected String getDQMFolder(WorkFlowContext context) {
		if (dqmFolder == null) {
			dqmFolder = "/mnt/staging/dqm/";
			GeneratorPropertyQuery generatorPropertyQuery = new GeneratorPropertyQuery();
			GeneratorProperty generatorProperty = generatorPropertyQuery
					.retrieve(GeneratorWorkFlowContext.RITHOMAS_DQM_DIR);
			if (generatorProperty != null
					&& generatorProperty.getParamvalue() != null) {
				dqmFolder = generatorProperty.getParamvalue();
			}
		}
		LOGGER.debug("DQM folder: {}", dqmFolder);
		return dqmFolder;
	}

	protected List<String> getDeployedUsageSpecDirs(WorkFlowContext context) {
		List<String> deployedUsageSpecDirs = new ArrayList<String>();
		EntityManager entityManager = ModelResources.getEntityManager();
		List<String> statusList = new ArrayList<String>();
		statusList.add(DeploymentDictionaryConstants.DEPLOYED);
		String version = "1";
		String entityType = EntityType.USAGE_SPECIFICATION.name();
		List<String> objectIDList = entityManager
				.createNamedQuery(
						DeploymentDictionary.FIND_DISTINCT_DEPLOYED_ENTITY,
						String.class)
				.setParameter(DeploymentDictionary.STATUS_LIST, statusList)
				.setParameter(DeploymentDictionary.ENTITY_TYPE, entityType)
				.setParameter(DeploymentDictionary.OBJECT_VERSION, version)
				.getResultList();
		LOGGER.debug("Deployed usage specification size: {}",
				objectIDList.size());
		for (String usageSpecId : objectIDList) {
			List<DeploymentDictionary> dictionaries = entityManager
					.createNamedQuery(
							DeploymentDictionary.FIND_BY_OBJID_VER_ENT_TYPE_ONLY,
							DeploymentDictionary.class)
					.setParameter(DeploymentDictionary.OBJECT_ID, usageSpecId)
					.setParameter(DeploymentDictionary.OBJECT_VERSION, version)
					.setParameter(DeploymentDictionary.ENTITY_TYPE, entityType)
					.getResultList();
			if (dictionaries != null && !dictionaries.isEmpty()) {
				String adaptationId = dictionaries.get(0).getAdaptationid();
				String adaptationVersion = dictionaries.get(0)
						.getAdaptationVersion();
				String folderName = adaptationId + UNDERSCORE
						+ adaptationVersion + SEPARATOR + usageSpecId
						+ UNDERSCORE + version + SEPARATOR;
				deployedUsageSpecDirs.add(folderName);
			}
		}
		LOGGER.debug("Deployed usage specification directories: {}",
				deployedUsageSpecDirs);
		return deployedUsageSpecDirs;
	}
}
