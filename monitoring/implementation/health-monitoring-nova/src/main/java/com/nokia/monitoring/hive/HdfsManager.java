
package com.nokia.monitoring.hive;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HdfsManager {

	private Configuration conf;

	protected String hadoopConfigurationPath;

	public HdfsManager(String hadoopConfigurationPath) {
		this.hadoopConfigurationPath = hadoopConfigurationPath;
		initializeConfiguration();
	}

	private void initializeConfiguration() {
		if (conf == null) {
			conf = new Configuration();
			conf.addResource(new Path(String.format("%s/core-site.xml",
					hadoopConfigurationPath)));
			conf.addResource(new Path(String.format("%s/hdfs-site.xml",
					hadoopConfigurationPath)));
			System.out.println(conf.toString());
			UserGroupInformation.setConfiguration(conf);
		}
	}

	protected FileSystem getFileSystem() throws IOException {
		return FileSystem.get(conf);
	}
}
