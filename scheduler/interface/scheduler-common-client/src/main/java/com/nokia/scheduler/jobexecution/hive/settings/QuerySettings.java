
package com.rijin.scheduler.jobexecution.hive.settings;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("QuerySettings")
public class QuerySettings {

	@XStreamAlias("GlobalSettings")
	private GlobalSettings globalSettings;

	@XStreamAlias("Hive2Settings")
	private Hive2Settings hive2Settings;

	public GlobalSettings getGlobalSettings() {
		return globalSettings;
	}

	public void setGlobalSettings(GlobalSettings globalSettings) {
		this.globalSettings = globalSettings;
	}

	public Hive2Settings getHive2Settings() {
		return hive2Settings;
	}

	public void setHive2Settings(Hive2Settings hive2Settings) {
		this.hive2Settings = hive2Settings;
	}
}
