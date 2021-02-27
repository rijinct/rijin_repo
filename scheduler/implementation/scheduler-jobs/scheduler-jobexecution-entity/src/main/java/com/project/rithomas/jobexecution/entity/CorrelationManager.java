
package com.project.rithomas.jobexecution.entity;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.step.AbortableWorkflowStep;
import com.project.rithomas.jobexecution.common.util.CommonCorrelationUtil;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.StringModificationUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.ext.adddb.workflow.generator.util.HiveGeneratorUtil;
import com.project.rithomas.sdk.model.common.CharacteristicSpecification;
import com.project.rithomas.sdk.model.common.query.CharacteristicSpecificationQuery;
import com.project.rithomas.sdk.model.corelation.EntityCharacteristics;
import com.project.rithomas.sdk.model.corelation.EntitySpec;
import com.project.rithomas.sdk.model.corelation.EntitySpecReferences;
import com.project.rithomas.sdk.model.corelation.EntitySpecification;
import com.project.rithomas.sdk.model.corelation.query.EntitySpecificationQuery;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.deployment.util.DeployerConstants;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.util.EntitySpecificationGeneratorUtil;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class CorrelationManager extends AbortableWorkflowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CorrelationManager.class);

	private static final String DUPLICATE_VIOLATION = "duplicate";

	private static final String FOREIGN_KEY_VIOLATION = "foreign_key";

	private static final boolean TIMER_STATUS = false;

	private long delay;

	private Timer connTimer = new Timer();

	private ApplicationLoggerUtilInterface applicationLogger;

	private String executionEngine;

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		LOGGER.info("Starting Dimension Loading");
		String sqlToExecute = null;
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String adaptationType = (String) context
				.getProperty(JobExecutionContext.ADAPTATION_ID);
		applicationLogger = ApplicationLoggerFactory.getApplicationLogger();
		context.setProperty(JobExecutionContext.APPLICATION_ID_LOGGER,
				applicationLogger);
		executionEngine = JobExecutionUtil.getExecutionEngine(context);
		schedulerJobRunner jobRunner = null;
		try {
			ReConnectUtil reConnectUtil = new ReConnectUtil();
			while (reConnectUtil.shouldRetry()) {
				if (!interrupted) {
					try {
						jobRunner = schedulerJobRunnerfactory.getRunner(
								executionEngine, (boolean) context.getProperty(
										JobExecutionContext.IS_CUSTOM_DB_URL));
						CommonCorrelationUtil
								.constructMapForDependentQSJob(context);
						// Checking if duplicate entries exist in external
						// table. if
						// yes
						// throw exception
						String targetTableName = (String) context
								.getProperty(JobExecutionContext.TARGET);
						delay = (Long) context
								.getProperty(JobExecutionContext.QUERY_TIMEOUT);
						// Executing Hive UDF functions
						JobExecutionUtil.runQueryHints(jobRunner,
								HiveConfigurationProvider.getInstance()
										.getQueryHints(jobId, executionEngine));
						// Generic method for retrieving and replacing the
						// dynamic values in SQL
						String sql = RetrieveDimensionValues
								.replaceDynamicValuesInJobSQL(context,
										(String) context.getProperty(
												JobExecutionContext.SQL),
										jobRunner);
						String sqlToExecList[] = sql.split(
								GeneratorWorkFlowContext.QUERY_SEPARATOR);
						// Enable compression for insert queries
						for (String enableCompress : QueryConstants.ENABLE_COMPRESSION_COMM) {
							jobRunner.setQueryHint(enableCompress);
						}
						boolean duplicatesFound = false;
						boolean isCommonAdaptation = JobExecutionContext.COMMON_ADAP_ID
								.equalsIgnoreCase(adaptationType);
						// disable compression
						jobRunner.setQueryHint(
								QueryConstants.DISABLE_COMPRESSION_COMM);
						if (!isCommonAdaptation) {
							duplicatesFound = checkForDuplicates(context);
						}
						if (JobExecutionContext.ES_LOCATION_JOBID.equals(jobId)
								|| JobExecutionContext.ES_SUBSCRIBER_GROUP_JOBID
										.equals(jobId)
								|| JobExecutionContext.ENTITY_SUBS_GROUP_OTHERS_1_CORRELATIONJOB
										.equals(jobId)) {
							jobRunner.setQueryHint(QueryConstants.MR_TASK_1);
						}
						List<String> queries = new ArrayList<String>();
						String copiedFile = null;
						if (!isCommonAdaptation) {
							List<String> idVersion = HiveGeneratorUtil
									.getSpecIDVerFromTabName(targetTableName);
							EntitySpecificationQuery query = new EntitySpecificationQuery();
							EntitySpecification entitySpecification = (EntitySpecification) query
									.retrieveLatest(idVersion.get(0));
							queries = getEntityRelation(entitySpecification);
							if (!queries.isEmpty()) {
								copiedFile = copyViolationsToLocal(context,
										queries.get(1), queries.get(0),
										FOREIGN_KEY_VIOLATION);
							}
						}
						for (String sqlSplit : sqlToExecList) {
							// Mandatory to set this context key in all
							// scenarios
							// across the
							// module where hive query is to be executed,so that
							// abort job
							// will retrieve
							// the current hive session to kill the
							// corresponding
							// map reduce
							// job
							String currentTimeHint = targetTableName
									+ System.currentTimeMillis();
							connTimer = new Timer();
							sqlToExecute = sqlSplit.replace(
									JobExecutionContext.TIMESTAMP_HINT,
									currentTimeHint);
							if (!queries.isEmpty()) {
								String replacementQuery = queries.get(0);
								if (copiedFile != null) {
									LOGGER.debug(
											"Foreign key Constraint violation file : {}",
											copiedFile);
									File file = StringModificationUtil
											.getFileWithCanonicalPath(
													copiedFile);
									LOGGER.debug("Size of file: {}",
											file.length());
									if (file.length() > 0) {
										replacementQuery = queries.get(2);
									}
								}
								sqlToExecute = sqlToExecute.replace(
										JobExecutionContext.SOURCE_TABLE_NAME_TEMPLATE,
										replacementQuery);
							}
							LOGGER.debug("SQL to execute: {}", sqlToExecute);
							context.setProperty(
									JobExecutionContext.HIVE_SESSION_QUERY,
									sqlToExecute);
							context.setProperty(
									JobExecutionContext.TIMESTAMP_HINT,
									currentTimeHint);
							context.setProperty(JobExecutionContext.FLAG,
									"false");
							connTimer.schedule(new Watcher(), delay * 1000);
							if (schedulerConstants.HIVE_DATABASE
									.equals(executionEngine)) {
								jobRunner.setQueryHint(
										QueryConstants.HIVE_ROW_NUMBER_UDF);
							} else {
								jobRunner.setQueryHint(
										QueryConstants.HIVE_ROW_NUMBER_UDF_SPARK);
							}
							if (duplicatesFound) {
								jobRunner
										.setQueryHint(QueryConstants.MR_TASK_1);
							}
							String sleepTime = (String) context.getProperty(
									JobExecutionContext.APPLICATION_ID_LOGGER_SLEEP_TIME_IN_SEC);
							Thread logThread = applicationLogger
									.startApplicationIdLog(
											Thread.currentThread(),
											jobRunner.getPreparedStatement(
													sqlToExecute),
											jobId, sleepTime);
							jobRunner.runQuery(sqlToExecute);
							HiveRowCountUtil rowCountUtil = new HiveRowCountUtil();
							Long recordCount = rowCountUtil
									.getNumberOfRecordsInserted(context, null,
											applicationLogger);
							if (recordCount == -1) {
								LOGGER.info(
										"Number of record retrieval merged into table {} failed. Please check records merged - manually",
										getTableName(context));
							} else {
								LOGGER.info(
										"Number of records merged into table {}: {}",
										getTableName(context), recordCount);
							}
							applicationLogger.stopApplicationIdLog(logThread);
							if (!TIMER_STATUS) {
								connTimer.cancel();
							}
						}
						// disable compression
						dropExtTableData(context);
						context.setProperty(JobExecutionContext.DESCRIPTION,
								schedulerConstants.DIMENSION_LOADING_SUCCESS_MESSAGE);
						// Adding code to handle cache cleanup
						List<String> tableNames = new ArrayList<String>();
						if (JobExecutionContext.ES_TERMINAL_GROUP_JOBID
								.equals(jobId)) {
							tableNames.addAll(JobExecutionContext.JOB_TABLE_MAP
									.get(JobExecutionContext.ES_TERMINAL_GROUP_JOBID));
						} else if (JobExecutionContext.ES_CLEAR_CODE_GROUP_JOBID
								.equals(jobId)) {
							tableNames.addAll(JobExecutionContext.JOB_TABLE_MAP
									.get(JobExecutionContext.ES_CLEAR_CODE_GROUP_JOBID));
						} else if (JobExecutionContext.ES_LOCATION_JOBID
								.equals(jobId)) {
							tableNames.addAll(JobExecutionContext.JOB_TABLE_MAP
									.get(JobExecutionContext.ES_LOCATION_JOBID));
						} else {
							tableNames.add((String) context
									.getProperty(JobExecutionContext.TARGET));
						}
						success = true;
						break;
					} catch (SQLException | UndeclaredThrowableException e) {
						LOGGER.error(" Exception {}", e.getMessage(), e);
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION,
								e.getMessage());
						if (reConnectUtil.isRetryRequired(e.getMessage())) {
							reConnectUtil.updateRetryAttemptsForHive(
									e.getMessage(), context);
						} else {
							throw new WorkFlowExecutionException(
									"SQLException: " + e.getMessage(), e);
						}
					} catch (JobExecutionException e) {
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION,
								e.getMessage());
						LOGGER.error(" Exception {}", e.getMessage(), e);
						throw new WorkFlowExecutionException(e.getMessage(), e);
					} catch (IOException e) {
						LOGGER.error("Hadoop FileSystem Exception {}",
								e.getMessage(), e);
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION,
								e.getMessage());
						throw new WorkFlowExecutionException(
								"FileSystem Exception", e);
					} catch (InterruptedException e) {
						LOGGER.error("Interrupted Exception {}", e.getMessage(),
								e);
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION,
								e.getMessage());
						throw new WorkFlowExecutionException(
								"Interrupted Exception", e);
					} catch (Exception exception) {
						LOGGER.error("Exception : " + exception.getMessage(),
								exception);
						throw new WorkFlowExecutionException(
								"Exception  : " + exception.getMessage(),
								exception);
					}
				} else {
					LOGGER.debug("Job was aborted");
					updateErrorStatus(context);
					throw new WorkFlowExecutionException(
							"InterruptExpetion: Job was aborted by the user");
				}
			}
		} catch (Exception exception) {
			LOGGER.error("Exception : " + exception.getMessage(), exception);
			throw new WorkFlowExecutionException(
					"Exception  : " + exception.getMessage(), exception);
		} finally {
			try {
				jobRunner.closeConnection();
				if (!success) {
					updateErrorStatus(context);
				}
			} catch (Exception e) {
				success = false;
				LOGGER.warn("Exception while closing the connection {}",
						e.getMessage());
			}
		}
		return success;
	}

	private boolean checkForDuplicates(WorkFlowContext context) {
		boolean duplicatesFound = false;
		LOGGER.info("checking if duplicate records exist.");
		String sourceTableName = (String) context
				.getProperty(JobExecutionContext.SOURCE);
		String[] sourceTables = sourceTableName.split(",");
		for (String sourceTable : sourceTables) {
			LOGGER.debug("Source Table Name : " + sourceTable);
			String checkDuplicatequery = getQuery(context, sourceTable,
					"checkDuplicatequery");
			context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
					checkDuplicatequery);
			LOGGER.info("Query to check if duplicate records exists :"
					+ checkDuplicatequery);
			ResultSet rs = null;
			schedulerJobRunner jobRunner = null;
			try {
				jobRunner = schedulerJobRunnerfactory.getRunner(executionEngine,
						(boolean) context.getProperty(
								JobExecutionContext.IS_CUSTOM_DB_URL));
				// Executing Hive UDF functions
				for (String hiveUDF : HiveConfigurationProvider.getInstance()
						.getQueryHints(
								(String) context.getProperty(
										JobExecutionContext.JOB_NAME),
								executionEngine)) {
					LOGGER.debug("Executing hiveUDF: " + hiveUDF);
					jobRunner.setQueryHint(hiveUDF);
				}
				Object result = jobRunner.runQuery(checkDuplicatequery,
						(String) context
								.getProperty(JobExecutionContext.JOB_NAME),
						context);
				if (result instanceof ResultSet) {
					rs = (ResultSet) result;
					if (rs.next()) {
						duplicatesFound = true;
						LOGGER.debug(
								"Duplicate record exist in {}  " + sourceTable);
						logDuplicates(context, sourceTable);
					}
				} else {
					List<String> resultSet = (List<String>) result;
					if (!resultSet.isEmpty()) {
						duplicatesFound = true;
						LOGGER.debug(
								"Duplicate record exist in {}  " + sourceTable);
						logDuplicates(context, sourceTable);
					}
				}
			} catch (SQLException e) {
				LOGGER.warn("Exception while checking for duplicates: {}",
						e.getMessage(), e);
			} catch (WorkFlowExecutionException e) {
				LOGGER.warn("Exception while checking for duplicates: {}",
						e.getMessage(), e);
			} catch (Exception exception) {
				LOGGER.warn("Exception : " + exception.getMessage());
			} finally {
				ConnectionManager.closeResultSet(rs);
				if (jobRunner != null) {
					jobRunner.closeConnection();
				}
			}
		}
		return duplicatesFound;
	}

	private List<String> getEntityRelation(
			EntitySpecification entitySpecification) {
		List<String> referenceChkQueries = new ArrayList<String>();
		EntitySpecReferences refs = entitySpecification.getEntitySpecRef();
		String targetTableName = EntitySpecificationGeneratorUtil
				.getEntitySpecExtTableName(entitySpecification.getSpecId(),
						DeployerConstants.DEFAULT_VERSION);
		if (refs != null) {
			List<EntitySpec> entitySpecs = refs.getEntitySpecs();
			List<String> whereConditions = new ArrayList<String>();
			StringBuilder query = new StringBuilder("select ")
					.append(targetTableName).append(".* from ")
					.append(targetTableName);
			for (EntitySpec entitySpec : entitySpecs) {
				String id = entitySpec.getEntityID();
				String sourceTableName = EntitySpecificationGeneratorUtil
						.getEntitySpecTableName(id,
								DeployerConstants.DEFAULT_VERSION);
				query.append(" join ").append(sourceTableName).append(" on ");
				List<EntityCharacteristics> chars = entitySpec.getEntityChars();
				boolean firstChar = true;
				for (EntityCharacteristics entityChar : chars) {
					String sourceChar = getCharSpec(entityChar.getSourceChar())
							.getName();
					String condition = getRefCondition(targetTableName,
							sourceTableName, entityChar, sourceChar);
					LOGGER.debug("condition: {}", condition);
					if (firstChar) {
						firstChar = false;
					} else {
						query.append(" and ");
					}
					query.append(condition);
					whereConditions.add(
							sourceTableName + "." + sourceChar + " is null");
				}
			}
			StringBuilder checkQuery = new StringBuilder(
					query.toString().replace(" join ", " left outer join "))
							.append(" where ");
			for (int loop = 0; loop < whereConditions.size(); loop++) {
				checkQuery.append(whereConditions.get(loop));
				if (loop < whereConditions.size() - 1) {
					checkQuery.append(" or ");
				}
			}
			query = new StringBuilder("(").append(query).append(")")
					.append(targetTableName);
			LOGGER.debug("Reference check query: {}", checkQuery);
			LOGGER.debug("Valid reference query: {}", query);
			referenceChkQueries.add(targetTableName);
			referenceChkQueries.add(checkQuery.toString());
			referenceChkQueries.add(query.toString());
		}
		return referenceChkQueries;
	}

	private String getRefCondition(String targetTableName,
			String sourceTableName, EntityCharacteristics entityChar,
			String sourceChar) {
		String targetChar = null;
		String condition = null;
		CharacteristicSpecification targetCharSpec = getCharSpec(
				entityChar.getTargetChar());
		if (targetCharSpec.getDerivationFormula() != null
				&& !targetCharSpec.getDerivationFormula().isEmpty()) {
			targetChar = targetCharSpec.getDerivationFormula();
			condition = targetChar + "=" + sourceTableName + "." + sourceChar;
		} else {
			targetChar = targetCharSpec.getName();
			condition = targetTableName + "." + targetChar + "="
					+ sourceTableName + "." + sourceChar;
		}
		return condition;
	}

	private CharacteristicSpecification getCharSpec(String charId) {
		CharacteristicSpecificationQuery query = new CharacteristicSpecificationQuery();
		return (CharacteristicSpecification) query.retrieve(charId, null);
	}

	// rakekris
	@SuppressWarnings("unchecked")
	protected String getQuery(WorkFlowContext context, String sourceTable,
			String queryType) {
		String query = "";
		if (queryType.equals("checkDuplicatequery")) {
			query = QueryConstants.UNIQUE_ROWS_QUERY
					.replace(QueryConstants.TABLE_NAME, sourceTable)
					.replace(QueryConstants.UNIQUE_COLS,
							((Map<String, String>) context.getProperty(
									JobExecutionContext.UNIQUE_KEY))
											.get(sourceTable));
		} else {
			query = QueryConstants.DUPLICATE_RECORDS_QUERY
					.replace(QueryConstants.TABLE_NAME, sourceTable)
					.replace(QueryConstants.UNIQUE_COLS,
							((Map<String, String>) context.getProperty(
									JobExecutionContext.UNIQUE_KEY))
											.get(sourceTable))
					.replace(QueryConstants.TABLE_COLS,
							((Map<String, String>) context.getProperty(
									JobExecutionContext.TABLE_COLUMNS))
											.get(sourceTable));
		}
		return query;
	}

	protected void logDuplicates(WorkFlowContext context, String sourceTable) {
		String duplicateQuery = getQuery(context, sourceTable,
				"duplicateQuery");
		copyViolationsToLocal(context, duplicateQuery, sourceTable,
				DUPLICATE_VIOLATION);
	}

	protected String copyViolationsToLocal(WorkFlowContext context,
			String violationsQuery, String sourceTable, String type) {
		String adaptationName = (String) context
				.getProperty(JobExecutionContext.ADAPTATION_ID) + "_"
				+ (String) context
						.getProperty(JobExecutionContext.ADAPTATION_VERSION);
		String entityName = sourceTable.replace("EXT_", "");
		String archivePathToLog = "/mnt/staging/archive/" + adaptationName + "/"
				+ entityName + "/corrupt/";
		String sourcePath = "/tmp/" + type + "/" + entityName;
		String queryToFile = String.format(
				"INSERT OVERWRITE DIRECTORY '%s' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' %S ",
				sourcePath, violationsQuery);
		LOGGER.debug("Query for {} constraint : {}", type, queryToFile);
		context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
				queryToFile);
		String scriptPath = SDKSystemEnvironment.getschedulerBinDirectory()
				+ "/movefiles.sh";
		String fileName = entityName + "_" + type;
		String extension = "dis";
		String copiedFile = null;
		LOGGER.debug("Source HDFS  Directory is {}", sourcePath);
		LOGGER.debug(
				"Target Directory for {} constraint is {} and target file name is {}",
				type, archivePathToLog, fileName + extension);
		Process process = null;
		BufferedReader br = null;
		InputStreamReader isr = null;
		schedulerJobRunner jobRunner = null;
		try {
			jobRunner = schedulerJobRunnerfactory.getRunner(executionEngine,
					(boolean) context
							.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
			LOGGER.debug("Executing export query: {}", queryToFile);
			// Executing Hive UDF functions
			for (String hiveUDF : HiveConfigurationProvider.getInstance()
					.getQueryHints(
							(String) context
									.getProperty(JobExecutionContext.JOB_NAME),
							executionEngine)) {
				if (!hiveUDF.contains("compress")) {
					jobRunner.setQueryHint(hiveUDF);
				}
			}
			if (DUPLICATE_VIOLATION.equals(type)) {
				jobRunner.setQueryHint(QueryConstants.HIVE_ROW_NUMBER_UDF);
				jobRunner.setQueryHint(QueryConstants.MR_TASK_1);
			}
			jobRunner.runQuery(queryToFile);
			String[] command = { "/bin/bash", scriptPath, sourcePath,
					archivePathToLog, fileName, "dis" };
			LOGGER.info(
					"Copying the exported discarded file because of {} constraint violations to shared location: {}",
					type, archivePathToLog);
			if (isKubernetesEnvironment()) {
				copyToLocalFromHDFS(sourcePath, archivePathToLog, fileName + "_1." + extension);
			} else {
				ProcessBuilder processBuilder = new ProcessBuilder(command);
				process = processBuilder.start();
				isr = new InputStreamReader(process.getInputStream(),
						StandardCharsets.UTF_8);
				br = new BufferedReader(isr);
				String line;
				LOGGER.debug("Output of running " + Arrays.asList(command)
						+ " is: ");
				while ((line = br.readLine()) != null) {
					LOGGER.debug(line);
				}
			}
			copiedFile = archivePathToLog + fileName + "_1." + extension;
			LOGGER.info("Successfully copied files to {}" , copiedFile);
		} catch (IOException e) {
			LOGGER.warn("Exception while exporting violated records: {}",
					e.getMessage(), e);
		} catch (SQLException e) {
			LOGGER.warn("Exception while exporting violated records: {}",
					e.getMessage(), e);
		} catch (Exception e) {
			LOGGER.warn("Exception while exporting violated records: {}",
					e.getMessage(), e);
		} finally {
			if (jobRunner != null) {
				jobRunner.closeConnection();
			}
			if (process != null) {
				process.destroy();
			}
			close(isr);
			close(br);
		}
		return copiedFile;
	}
	
	private void copyToLocalFromHDFS(String source, String destination,
			String fileName) throws IOException {
		Configuration conf = GetDBResource.getInstance().getHdfsConfiguration();
		FileSystem fileSystem = FileSystem.get(conf);
		fileSystem.copyToLocalFile(true, new Path(source),
				new Path(destination));
		File destDirectory = StringModificationUtil
				.getFileWithCanonicalPath(destination);
		File[] files = destDirectory.listFiles();
		if (files != null && files.length > 0) {
				File newFile = new File(destination + fileName);
				if (!files[0].renameTo(newFile)) {
					throw new IOException(
							"Could not rename file in destination folder");
				}
		}
	}
	
	private boolean isKubernetesEnvironment() {
		return toBoolean(System.getenv(schedulerConstants.IS_K8S));
	}

	private void close(Closeable cl) {
		if (cl != null) {
			try {
				cl.close();
			} catch (IOException e) {
				LOGGER.warn("Exception while closing: {}", e.getMessage());
			}
		}
	}

	private void dropExtTableData(WorkFlowContext context) throws IOException {
		Configuration conf = GetDBResource.getInstance().getHdfsConfiguration();
		FileSystem fileSystem = FileSystem.get(conf);
		String entityName = "";
		String adaptationName = (String) context
				.getProperty(JobExecutionContext.ADAPTATION_ID) + "_"
				+ (String) context
						.getProperty(JobExecutionContext.ADAPTATION_VERSION);
		String sourceTableName = (String) context.getProperty("SOURCE");
		String[] sourceTables = sourceTableName.split(",");
		final String fileExtension = "."
				+ (String) context.getProperty("FILEEXTENSION");
		for (String sourceTable : sourceTables) {
			entityName = sourceTable.replace("EXT_", "");
			String availableDir = "/rithomas/es/work/" + adaptationName + "/"
					+ entityName;
			LOGGER.debug("Dropping data from external table with path {} ",
					availableDir);
			Path path = new Path(availableDir);
			FileStatus files[] = fileSystem.listStatus(path, new PathFilter() {

				@Override
				public boolean accept(Path path) {
					return path.toString().endsWith(fileExtension);
				}
			});
			if (files.length != 0) {
				for (int cnt = 0; cnt < files.length; cnt++) {
					fileSystem.delete(files[cnt].getPath(), true);
				}
			}
		}
	}

	private String getTableName(WorkFlowContext context) {
		String sqlToExecute = (String) context
				.getProperty(JobExecutionContext.HIVE_SESSION_QUERY);
		String tableName = (String) context
				.getProperty(JobExecutionContext.TARGET);
		if (sqlToExecute != null) {
			String[] sqlSplit = sqlToExecute.trim().split(" ");
			if (sqlSplit.length > 4 && "TABLE".equalsIgnoreCase(sqlSplit[2])) {
				tableName = sqlSplit[3];
			}
		}
		return tableName;
	}

	private void updateErrorStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "E");
		updateJobStatus.updateFinalJobStatus(context);
	}

	class Watcher extends TimerTask {

		@Override
		public void run() {
			LOGGER.debug("Watcher called at: {}", System.currentTimeMillis());
			LOGGER.info("Job is not responding for {} mins.", delay / 60);
		}
	}
}
