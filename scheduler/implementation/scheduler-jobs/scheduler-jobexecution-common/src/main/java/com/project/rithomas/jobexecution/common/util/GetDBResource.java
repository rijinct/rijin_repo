
package com.project.rithomas.jobexecution.common.util;

import static com.project.rithomas.jobexecution.common.util.StringModificationUtil.getFileWithCanonicalPath;
import static com.project.rithomas.sdk.model.utils.SDKEncryptDecryptUtils.decryptPassword;
import static com.project.rithomas.sdk.model.utils.SDKSystemEnvironment.getSDKApplicationConfDirectory;
import static com.project.rithomas.sdk.model.utils.SDKSystemEnvironment.getschedulerConfDirectory;
import static org.apache.commons.lang3.BooleanUtils.toBoolean;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.ca4ci.pm.client.PasswordManagementClient;
import com.rijin.ca4ci.pm.helper.DbType;
import com.rijin.ca4ci.pm.helper.PasswordManagementException;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.model.meta.query.GeneratorPropertyQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public final class GetDBResource {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(GetDBResource.class);

	private static String postgresDriver = "org.postgresql.Driver";

	private static String postgresUrl = "jdbc:postgresql://localhost/sai";

	private static String postgresCacheUser = "saiws";

	private static String postgresCachePassword = "";

	private static final String POSTGRES_CONFIG_FILE = "postgres-config.properties";

	private static volatile Properties postgresProperties;

	private static volatile Properties persistenceProperties;

	private static volatile Properties clientConfigProperties;

	private static String username = "rithomas";

	private static String password = "";

	private static String postgresUrlKey = "db_url";

	private static String postgresDriverKey = "db_driver";

	private static boolean initialized = false;

	private static final String PERSISTANCE_CONFIG_FILE = "persistence-config.properties";

	private static final String CLIENT_PROPERTIES_FILE = "client.properties";

	private static GetDBResource getDBResource;

	private static final String HADOOP_CORE_SITE_XML_FILE_LOCATION = "/etc/hadoop/conf/core-site.xml";

	private static final String HADOOP_HDFS_SITE_XML_FILE_LOCATION = "/etc/hadoop/conf/hdfs-site.xml";

	private static final String KEY_PROJECT_SDK_DB_URL = "PROJECT_SDK_DB_URL";

	private static final String KEY_PROJECT_SDK_DB_USERNAME = "PROJECT_SDK_DB_USERNAME";

	private static final String DEFAULT_DB_NAME = "sai";

	private static String dbName;

	private GetDBResource() {
	}

	static {
		getDBResource = new GetDBResource();
	}

	private static void initializeConfigProperties() {
		postgresProperties = new Properties();
		loadConfigProperties(postgresProperties, getPostgresConfigFilePath());
	}

	private static void initializePersistenceProperties() {
		persistenceProperties = new Properties();
		loadConfigProperties(persistenceProperties,
				getPersistenceConfigFilePath());
	}

	private static void intializeClientConfigProperties() {
		clientConfigProperties = new Properties();
		loadConfigProperties(clientConfigProperties, getClientConfigFilePath());
		setPostgresCacheUser(clientConfigProperties
				.getProperty("postgres.cache.user", "saiws"));
		setPostgresCachePassword(decryptPassword(
				clientConfigProperties.getProperty("postgres.cache.password")));
	}

	private static void loadConfigProperties(Properties properties,
			File fileObj) {
		try (InputStream clientProp = Files.newInputStream(fileObj.toPath())) {
			properties.load(clientProp);
		} catch (FileNotFoundException e) {
			LOGGER.error("FileNotFoundException occured", e);
		} catch (IOException e) {
			LOGGER.error("IOException occured", e);
		}
	}

	public static GetDBResource getInstance() {
		if (!initialized) {
			initializeConfigProperties();
			initializePersistenceProperties();
			intializeClientConfigProperties();
			LOGGER.info("Loading postgres url from postgres.properties for {}",
					postgresUrlKey);
			postgresUrl = postgresProperties.getProperty(postgresUrlKey);
			LOGGER.info(
					"Loading postgres driver from postgres.properties for {}",
					postgresDriverKey);
			postgresDriver = postgresProperties.getProperty(postgresDriverKey);
			dbName = persistenceProperties.getProperty("db.name");
			initialized = true;
		}
		return getDBResource;
	}

	public void retrievePostgresObjectProperties(WorkFlowContext context)
			throws JobExecutionException {
		this.retrievePropertiesFromConfig();
		LOGGER.debug("dbUrl : {}, dbDriver : {}, username : {}", postgresUrl,
				postgresDriver, username);
		context.setProperty(JobExecutionContext.POSTGRESURL, postgresUrl);
		context.setProperty(JobExecutionContext.POSTGRESDRIVER, postgresDriver);
		context.setProperty(JobExecutionContext.USERNAME, username);
		context.setProperty(JobExecutionContext.PASSWORD, password);
	}

	private void retrieveUsernameAndURL() {
		if (isKubernetesEnvironment()) {
			username = System.getenv(KEY_PROJECT_SDK_DB_USERNAME);
			postgresUrl = System.getenv(KEY_PROJECT_SDK_DB_URL);
		} else {
			username = persistenceProperties.getProperty("db.username");
			postgresUrl = persistenceProperties
					.getProperty("db.connection.url");
		}
	}

	public void retrievePropertiesFromConfig() throws JobExecutionException {
		postgresDriver = persistenceProperties.getProperty("db.driver");
		retrieveUsernameAndURL();
		password = getPostgrePassword();
	}

	public static String getPostgrePassword() throws JobExecutionException {
		PasswordManagementClient passwordManagementClient = new PasswordManagementClient();
		try {
			return passwordManagementClient.readPassword(DbType.POSTGRES,
					getDBName(), username);
		} catch (PasswordManagementException e) {
			LOGGER.error("Unable to retrieve postgres password", e);
			throw new JobExecutionException(
					"error while retrieving postgres password", e);
		}
	}

	private static String getDBName() {
		return isNotEmpty(dbName) ? dbName : DEFAULT_DB_NAME;
	}

	public Configuration getHdfsConfiguration() {
		Configuration config = new Configuration();
		config.addResource(new Path(HADOOP_CORE_SITE_XML_FILE_LOCATION));
		config.addResource(new Path(HADOOP_HDFS_SITE_XML_FILE_LOCATION));
		UserGroupInformation.setConfiguration(config);
		return config;
	}

	public static String getPostgresUrl() {
		return postgresUrl;
	}

	public static void setPostgresUrl(String postgresUrl) {
		GetDBResource.postgresUrl = postgresUrl;
	}

	public static String getPostgreUserName() {
		return username;
	}

	public static String getPostgreDriver() {
		return postgresDriver;
	}

	public static void setPostgresCacheUser(String postgresCacheUser) {
		GetDBResource.postgresCacheUser = postgresCacheUser;
	}

	public static String getPostgresCacheUser() {
		return postgresCacheUser;
	}

	public static void setPostgresCachePassword(String postgresCachePassword) {
		GetDBResource.postgresCachePassword = postgresCachePassword;
	}

	public static String getPostgresCachePassword() {
		return postgresCachePassword;
	}

	protected GeneratorPropertyQuery getGeneratorProeprtyQuery() {
		return new GeneratorPropertyQuery();
	}

	private static File getPostgresConfigFilePath() {
		return getFileWithCanonicalPath(
				getschedulerConfDirectory() + POSTGRES_CONFIG_FILE);
	}

	private static File getClientConfigFilePath() {
		return getFileWithCanonicalPath(
				getschedulerConfDirectory() + CLIENT_PROPERTIES_FILE);
	}

	private static File getPersistenceConfigFilePath() {
		return getFileWithCanonicalPath(
				getSDKApplicationConfDirectory() + PERSISTANCE_CONFIG_FILE);
	}

	private static boolean isKubernetesEnvironment() {
		return toBoolean(System.getenv(schedulerConstants.IS_K8S));
	}
}