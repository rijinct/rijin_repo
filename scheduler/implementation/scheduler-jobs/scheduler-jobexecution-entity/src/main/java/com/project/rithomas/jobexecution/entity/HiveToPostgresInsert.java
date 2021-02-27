
package com.project.rithomas.jobexecution.entity;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Multimap;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.etl.common.utils.ETLCommonUtils;
import com.project.rithomas.etl.common.utils.FileUtils;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.common.CharacteristicSpecification;
import com.project.rithomas.sdk.model.corelation.EntitySpecCharUseComparator;
import com.project.rithomas.sdk.model.corelation.EntitySpecification;
import com.project.rithomas.sdk.model.corelation.EntitySpecificationCharacteristicUse;
import com.project.rithomas.sdk.model.meta.GenericDataType;
import com.project.rithomas.sdk.model.utils.ModelUtil;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class HiveToPostgresInsert extends AbstractWorkFlowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveToPostgresInsert.class);

	private static final String DOUBLE_DATA_TYPE = "Double";

	private static final String SELECT_FROM_HIVE = "select * from $TARGET_TABLE";

	private static final String DELETE_FROM_POSTGRES = "delete from $SCHEMA_NAME.$TARGET_TABLE";

	private static final String INSERT_TO_POSTGRES = "insert into $SCHEMA_NAME.$TARGET_TABLE ($COLUMN_NAMES) values ($COLUMN_VALUES)";

	private static final String TARGET_TABLE = "$TARGET_TABLE";

	private static final String SCHEMA_NAME = "$SCHEMA_NAME";

	private static final String COLUMN_NAMES = "$COLUMN_NAMES";

	private static final String COLUMN_VALUES = "$COLUMN_VALUES";

	private static final String DATA_SCHEMA = "saidata";

	private String columnNames = "";

	@SuppressWarnings("unchecked")
	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		Connection hiveConnection = null, postgresConnection = null;
		Statement hiveStatement = null, postgresStatement = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		try {
			hiveConnection = ConnectionManager
					.getConnection(schedulerConstants.HIVE_DATABASE);
			hiveStatement = hiveConnection.createStatement();
			String postgresLoadingEnabled = (String) context
					.getProperty(JobExecutionContext.POSTGRES_LOADING_ENABLED);
			postgresConnection = DBConnectionManager.getInstance()
					.getPostgresConnection(GetDBResource.getPostgreDriver(),
							GetDBResource.getPostgresUrl(),
							GetDBResource.getPostgreUserName(),
							GetDBResource.getPostgrePassword());
			postgresConnection.setAutoCommit(false);
			String jobName = (String) context
					.getProperty(JobExecutionContext.JOB_NAME);
			String targetTable = (String) context
					.getProperty(JobExecutionContext.TARGET);
			 if ("YES".equalsIgnoreCase(postgresLoadingEnabled)) {
				String selectQuery = SELECT_FROM_HIVE.replace(TARGET_TABLE,
						targetTable);
								
				preparedStatement = hiveConnection
						.prepareStatement(selectQuery);
				rs = preparedStatement.executeQuery();
				String deleteQuery = DELETE_FROM_POSTGRES
						.replace(SCHEMA_NAME, DATA_SCHEMA)
						.replace(TARGET_TABLE, targetTable);
				preparedStatement = postgresConnection
						.prepareStatement(deleteQuery);
				preparedStatement.executeUpdate();
				if (rs.next()) {
					Map<Integer, String> columnDetailsMap = getColumnsMap(
							jobName);
					String insertQuery = INSERT_TO_POSTGRES
							.replace(TARGET_TABLE, targetTable)
							.replace(SCHEMA_NAME, DATA_SCHEMA)
							.replace(COLUMN_NAMES, columnNames);
					do {
						String columnValues = getColumnValues(columnDetailsMap,
								rs);
						String finalQuery = insertQuery.replace(COLUMN_VALUES,
								columnValues);
						
						preparedStatement = postgresConnection
								.prepareStatement(finalQuery);
						preparedStatement.executeUpdate();
					} while (rs.next());
				}
				postgresConnection.commit();
			}
			success = true;
		} catch (Exception ex) {
			LOGGER.error("Exception occurred in step HiveToPostgresInsert", ex);
			updateJobStatus.updateETLErrorStatusInTable(ex, context);
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					ex.getMessage());
			throw new WorkFlowExecutionException(
					"Failed to insert data from hive to postgres: "
							+ ex.getMessage(),
					ex);
		} finally {
			try {
				ConnectionManager.closeResultSet(rs);
				ConnectionManager.closeStatement(hiveStatement);
				ConnectionManager.closeStatement(preparedStatement);
				ConnectionManager.releaseConnection(hiveConnection,
						schedulerConstants.HIVE_DATABASE);
				if (!success && postgresConnection != null) {
					postgresConnection.rollback();
				}
				DBConnectionManager.getInstance()
						.closeConnection(postgresStatement, postgresConnection);
			} catch (Exception e) {
				success = false;
				LOGGER.warn("Exception while clossing the connection {}",
						e.getMessage());
			}
		}
		if (success) {
			deleteWorkAndInputFiles(
					(Multimap<String, String>) context.getProperty(
							JobExecutionContext.ADAP_MAP_IMPORT_DIR_AND_WORK_FILE_PATH_LIST));
		}
		return success;
	}

	private void deleteWorkAndInputFiles(
			Multimap<String, String> importDirAndWorkFilePathMultimap) {
		if (importDirAndWorkFilePathMultimap != null) {
			for (Entry<String, String> entry : importDirAndWorkFilePathMultimap
					.entries()) {
				String inputFileDir = entry.getKey();
				String workingFilepath = entry.getValue();
				File workFile = deleteFiles(workingFilepath);
				LOGGER.info("Deleting the input file from Directory : {}",
						inputFileDir);
				if (!inputFileDir.endsWith(File.separator)) {
					inputFileDir = inputFileDir + File.separatorChar;
				}
				deleteFiles(inputFileDir + workFile.getName());
			}
		}
	}

	private File deleteFiles(String filepath) {
		File file = FileUtils.getFileFromPath(filepath);
		LOGGER.debug("Deleting the file : {}", file);
		boolean isFileDeleted=file.delete();
		LOGGER.debug("Removal of temporary target file {} was {}:",
				file.getName(),
				String.valueOf(isFileDeleted ? "Successful" : "Unsuccessful"));
		return file;
	}

	private String getColumnValues(Map<Integer, String> columnDetailsMap,
			ResultSet rs) throws SQLException {
		Integer columnIndex = 1;
		String columnValues = "";
		for (Integer integer = 1; integer <= columnDetailsMap
				.size(); integer++) {
			String valueType = columnDetailsMap.get(integer);
			LOGGER.debug("Column name: {}, value type: {}", integer, valueType);
			if (GenericDataType.STRING.equalsIgnoreCase(valueType)) {
				columnValues += "'" + rs.getString(columnIndex) + "'";
			} else if (GenericDataType.NUMBER.equalsIgnoreCase(valueType)) {
				columnValues += rs.getLong(columnIndex);
			} else if (GenericDataType.DECIMAL.equalsIgnoreCase(valueType)
					|| DOUBLE_DATA_TYPE.equalsIgnoreCase(valueType)) {
				columnValues += rs.getDouble(columnIndex);
			} else if (GenericDataType.INTEGER.equalsIgnoreCase(valueType)) {
				columnValues += rs.getInt(columnIndex);
			} else if (GenericDataType.DATE.equalsIgnoreCase(valueType)
					|| GenericDataType.DATETIME.equalsIgnoreCase(valueType)) {
				// In hive dimension tables, date values are stored as string
				// due to hive query limitation
				String value = rs.getString(columnIndex);
				if (value != null && !value.equals("null")) {
					columnValues += "'" + value + "'";
				} else {
					columnValues += value;
				}
			}
			if (columnIndex < columnDetailsMap.size()) {
				columnValues += ",";
			}
			columnIndex++;
		}
		LOGGER.debug("Column values: {}", columnValues);
		return columnValues;
	}

	private Map<Integer, String> getColumnsMap(String jobName) {
		Map<Integer, String> columnsMap = new HashMap<Integer, String>();
		EntitySpecification entitySpecification = ETLCommonUtils
				.getEntitySpecFromJobName(jobName);
		Collection<EntitySpecificationCharacteristicUse> entitySpecificationCharacteristicUseList = entitySpecification
				.getEntitySpecificationCharacteristicUse();
		List<EntitySpecificationCharacteristicUse> entitySpecCharUseList = new ArrayList<EntitySpecificationCharacteristicUse>(
				entitySpecificationCharacteristicUseList);
		Collections.sort(entitySpecCharUseList,
				new EntitySpecCharUseComparator());
		Integer iterator = 1;
		for (EntitySpecificationCharacteristicUse entitySpecificationCharacteristicUse : entitySpecCharUseList) {
			CharacteristicSpecification characterSpec = entitySpecificationCharacteristicUse
					.getSpecChar();
			// add only non-transient columns - derived and non-transient
			LOGGER.debug("The characteristic spec id {} is {}",
					characterSpec.getName(), characterSpec.getAbstractType());
			if (characterSpec.getAbstractType() == null
					|| ModelUtil.SPEC_CHAR_TYPE_DERIVED.equalsIgnoreCase(
							characterSpec.getAbstractType())) {
				String valueType = characterSpec.getValueType();
				if (GenericDataType.NUMBER
						.equalsIgnoreCase(characterSpec.getValueType())
						&& (characterSpec.getValueTypePrecision() != null
								&& characterSpec.getValueTypePrecision()
										.contains(","))) {
					valueType = DOUBLE_DATA_TYPE;
				}
				columnsMap.put(iterator, valueType);
				columnNames += characterSpec.getName() + ",";
				iterator++;
			}
		}
		columnNames = columnNames.substring(0, columnNames.length() - 1);
		return columnsMap;
	}
}
