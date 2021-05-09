package com.nokia.jobmoniter.rest.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

public class ReadPropertyFile {

	Logger logger = Logger.getLogger("nokia");

	public Map<String, List<String>> readPropertyFile(String CPName,
			String frequency) {
		Map<String, List<String>> propertyMap = new HashMap<String, List<String>>();

		Properties prop = new Properties();
		InputStream input;
		try {

			input = ReadPropertyFile.class
					.getResourceAsStream("/tableName.properties");
			prop.load(input);
			List<String> tableList = new ArrayList<String>();
			if (frequency != null) {
				if (CPName.equals("ALL")) {
					logger.info("Getting result for all CPs with " + frequency
							+ " filter");
				}
				else if (prop.getProperty(CPName + "_" + frequency + "_TABLES")
						.split(",") != null) {
					tableList = Arrays.asList(prop.getProperty(
							CPName + "_" + frequency + "_TABLES").split(","));
					propertyMap.put(CPName + "_" + frequency + "_TABLES",
							tableList);
				} else {
					logger.info("Add " + CPName + "_" + frequency
							+ "_TABLES to tableName.properties");
				}
			} else {
				if (prop.getProperty(CPName) != null) {
					List<String> frequencyNames = Arrays.asList(prop
							.getProperty(CPName).split(","));
					if (frequencyNames.size() > 0) {

						for (String cpfrequencyNames : frequencyNames) {
							tableList = Arrays.asList(prop
									.getProperty(
											CPName + "_" + cpfrequencyNames
													+ "_TABLES").split(","));
							propertyMap.put(CPName + "_" + cpfrequencyNames
									+ "_TABLES", tableList);
						}
					}
				} else {
					logger.info("Add frequencies list for " + CPName
							+ " to tableName.properties");
				}

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return propertyMap;
	}

	public HashMap<String, Integer> populateDeviationMap() {

		HashMap<String, Integer> propertyMap = new HashMap<String, Integer>();

		Properties prop = new Properties();
		InputStream input;
		try {

			input = ReadPropertyFile.class
					.getResourceAsStream("/tableName.properties");
			prop.load(input);
			Enumeration<?> e = prop.propertyNames();
			while (e.hasMoreElements()) {
				String key = (String) e.nextElement();
				String value = prop.getProperty(key);
				if (key.contains("DEVIATION_REFERENCE")) {
					logger.info("==============Deviation Reference:======================Key: "
							+ key + " and Value: " + value);
					propertyMap.put(key.substring(0, key.indexOf('_')),
							Integer.valueOf(value));
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return propertyMap;

	}

	public static Properties getPropertyFile() {
		Properties prop = new Properties();
		InputStream input;
		input = ReadPropertyFile.class
				.getResourceAsStream("/tableName.properties");
		try {
			prop.load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return prop;

	}

}
