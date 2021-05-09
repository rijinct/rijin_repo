package com.nokia.monitoring.hive;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HdfsFileStatus extends HdfsManager {

	public HdfsFileStatus(String hadoopConfigurationPath) {
		super(hadoopConfigurationPath);
	}

	public String getFileStatus(String hdfsPath) throws IOException {
		FileSystem fs = getFileSystem();
		Path path = new Path(hdfsPath);
		FileStatus status = fs.getFileStatus(path);
		long lastModificationTime = status.getModificationTime();
		Date lastModificationTimestamp = new Date(lastModificationTime);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return df.format(lastModificationTimestamp);

	}

}