package com.nokia.web.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {
	
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
	
}