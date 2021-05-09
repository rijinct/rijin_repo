package com.nokia.jobmoniter.rest.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.nokia.jobmoniter.rest.constants.JobMonitorConstants;

public class ConfigUtil {
	
	private static String className = ConfigUtil.class.getName();

	private final static Logger logger = Logger.getLogger(className);

	static Properties properties;
	static{
		properties = new Properties();
		try {
			InputStream iStream = ConfigUtil.class.getClassLoader()
					.getResourceAsStream("config.properties");
			if (iStream != null) {
				properties.load(iStream);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static Properties getConfig() {
		return properties;
	}
	
	public static Properties loadDatabaseProperties() {
		FileInputStream postgres_config = null;
		Properties config = new Properties();
		try {
			postgres_config = new FileInputStream(JobMonitorConstants.POSTGRES_CONFIG);
			config.load(postgres_config);
		} catch (FileNotFoundException e) {
			initializeLogger(Level.SEVERE, "Properties load()",
					"Postgres config File not found : ", e);
		} catch (IOException e) {
			initializeLogger(Level.SEVERE, "Properties load()",
					"Exception during loadDatabaseProperties: ", e);
		} finally {
			closeInputFileStream(postgres_config);
		}
		return config;
	}

	private static void initializeLogger(Level level, String methodName,
			String errorText, Exception e) {
		logger.logp(level, className, methodName,
				errorText + e.getLocalizedMessage(), e);
	}

	public static void closeInputFileStream(FileInputStream input) {
		if (input != null) {
			try {
				input.close();
			} catch (IOException e) {
				initializeLogger(Level.WARNING, "loadDatabaseProperties()",
						"FileInputStream not closed : ", e);
			}
		}
	}
}
