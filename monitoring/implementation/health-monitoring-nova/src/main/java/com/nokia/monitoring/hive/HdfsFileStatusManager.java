
package com.nokia.monitoring.hive;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileStatusManager {

	private String hadoopConfigurationPath;

	public HdfsFileStatusManager(String hadoopConfigurationPath) {
		this.hadoopConfigurationPath = hadoopConfigurationPath;
	}

	public String getFileStatus(String hdfsPath) throws IOException {
		HdfsManager hdfsManager = new HdfsManager(hadoopConfigurationPath);
		FileSystem fs = hdfsManager.getFileSystem();
		Path path = new Path(hdfsPath);
		if (fs.isDirectory(path)) {
			FileStatus status = fs.getFileStatus(path);
			long lastModificationTime = status.getModificationTime();
			Date lastModificationTimestamp = new Date(lastModificationTime);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return df.format(lastModificationTimestamp);
		} else {
			return "Not Valid Path";
		}
	}

	public String getPartitionDetails(String hdfsPath) throws IOException {
		HdfsManager hdfsManager = new HdfsManager(this.hadoopConfigurationPath);
		String partition = null;
		FileSystem fs = hdfsManager.getFileSystem();
		Path pattern = new Path(String.format("%s/dt=*", hdfsPath));
		FileStatus[] directories = fs.globStatus(pattern);
		if (directories.length > 0) {
			partition = directories[directories.length - 1].getPath().toString()
					.split("dt=")[1];
			return partition;
		} else {
			return "Not Valid Path";
		}
	}
}