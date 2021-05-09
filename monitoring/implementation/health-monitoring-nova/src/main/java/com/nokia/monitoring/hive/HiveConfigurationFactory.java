package com.nokia.monitoring.hive;

import com.nokia.analytics.connector.hive.client.AbstractHiveConfigurationFactory;
import com.nokia.analytics.connector.hive.configuration.BareMetalHiveConfiguration;


public class HiveConfigurationFactory extends AbstractHiveConfigurationFactory{
	
	private HiveConfigurationFactory() {
		
	}
	
	public static HiveConfigurationFactory getInstance() {
		return new HiveConfigurationFactory();
	}

	@Override
	protected BareMetalHiveConfiguration getBareMetalHiveConfiguration() {
		return null;
	}
}