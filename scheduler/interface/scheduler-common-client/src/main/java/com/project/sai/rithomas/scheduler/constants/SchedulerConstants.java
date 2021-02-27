
package com.project.sai.rithomas.scheduler.constants;

public final class schedulerConstants {

	public static final String PAUSE_ALL = "-pause";

	public static final String RESUME_ALL = "-resume";

	public static final String START = "-start";

	public static final String SHUTDOWN = "-shutdown";

	public static final String DEBUG = "-debug";

	public static final String FILE = "-file";

	public static final String NAME = "name";

	public static final String GROUP = "group";

	public static final String JOB_DESCRIPTION = "description";

	public static final String DESCRIPTION_CONTENT = "Trigger created and registered by the JMX client.";

	public static final String JOB_CLASS = "jobClass";

	public static final String JOBDATAMAP = "jobDataMap";

	public static final String DURABILITY = "durability";

	public static final String JOB_DETAIL_CLASS = "jobDetailClass";

	public static final String JOB_NAME = "jobName";

	public static final String JOB_GROUP = "jobGroup";

	public static final String TRIGGER = "trigger";

	public static final String TRIGGER_CLASS = "triggerClass";

	public static final String REPEATCOUNT = "repeatCount";

	public static final String REPEATINTERVAL = "repeatInterval";

	public static final String STARTTIME = "startTime";

	public static final String ENDTIME = "endTime";

	public static final String CRONEXPRESSION = "cronExpression";

	public static final String HIVE_DATABASE = "HIVE";

	public static final String SPARK_DATABASE = "SPARK";

	public static final String DEFAULT = "DEFAULT";

	public static final String HIVE_CONFIG_FILE = "hive_config.properties";

	public static final String HIVE_SETTINGS_FILE = "hive_settings.xml";

	public static final String IS_K8S = "IS_K8S";

	public static final String HIVE_JDBC_URL = "HIVE_JDBC_URL";

	public static final String DB_SCHEMA = "DB_SCHEMA";

	public static final String DIMENSION_LOADING_SUCCESS_MESSAGE = "Dimension loading completed successfully";

	public static final String HIVE_TO_COUCHBASE_ERROR_MESSAGE = "Hive to couchbase loading failed due to: ";

	private schedulerConstants() {
		// hide default constructor for utility class
	}
}
