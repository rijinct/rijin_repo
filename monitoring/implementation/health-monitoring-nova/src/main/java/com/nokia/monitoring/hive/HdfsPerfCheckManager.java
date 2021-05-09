package com.nokia.monitoring.hive;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsPerfCheckManager {

	private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private String hadoopConfigurationPath;
	private static FileSystem fs;
	private Path srcFile, localDst, hdfsDst;

	public HdfsPerfCheckManager(String hadoopConfigurationPath) {
		this.hadoopConfigurationPath = hadoopConfigurationPath;
	}

	public String getPerfStats(String backupFile, String localPath, String hdfsPath) throws IOException {
		HdfsManager hdfsManager = new HdfsManager(hadoopConfigurationPath);
		fs = hdfsManager.getFileSystem();
		srcFile = new Path(backupFile);
		localDst = new Path(localPath);
		hdfsDst = new Path(hdfsPath);
		fs.mkdirs(hdfsDst);
	
		int hdfsWriteTime = getWriteTime();
		int hdfsReadTime = getReadTime();
		if(fs.exists(hdfsDst)) {
			fs.delete(hdfsDst, true);
		}
		return String.join(",", SDF.format(new Date()), Integer.toString(hdfsReadTime),
				Integer.toString(hdfsWriteTime));
	}

	private int getWriteTime() throws IOException {
		Date startTime = new Date();
		fs.copyFromLocalFile(true, srcFile, hdfsDst);
		return getTimeDiffSeconds(startTime, new Date());
	}

	private int getReadTime() throws IOException {
		Date startTime = new Date();
		fs.copyToLocalFile(true, hdfsDst, localDst);
		return getTimeDiffSeconds(startTime, new Date());
	}

	private static int getTimeDiffSeconds(Date startTime, Date currentTime) throws IOException {
		return (int) ((currentTime.getTime() - startTime.getTime()) / 1000);
	}
}
