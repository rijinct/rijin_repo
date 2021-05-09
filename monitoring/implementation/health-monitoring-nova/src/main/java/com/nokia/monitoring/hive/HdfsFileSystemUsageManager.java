
package com.nokia.monitoring.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileSystemUsageManager {

	private String hadoopConfigurationPath;

	public HdfsFileSystemUsageManager(String hadoopConfigurationPath) {
		this.hadoopConfigurationPath = hadoopConfigurationPath;
	}

	public Map getFileStatus(String hdfsPath, String duration)
			throws IOException {
		HdfsManager hdfsManager = new HdfsManager(hadoopConfigurationPath);
		FileStatus[] fileStatus = null;
		FileSystem fs = hdfsManager.getFileSystem();
		Path path = new Path(hdfsPath);
		Map<String, List<Long>> contentSummaryMap = new HashMap<String, List<Long>>();
		if (path.toString().contains("ps")) {
			Path pattern = new Path(
					String.format("%s/*/*%s*/", hdfsPath, duration));
			fileStatus = fs.globStatus(pattern);
		} else {
			fileStatus = fs.listStatus(path);
		}
		for (FileStatus fileStatusObj : fileStatus) {
			if (fileStatusObj.isDirectory()) {
				ContentSummary httpContentSummary = fs
						.getContentSummary(fileStatusObj.getPath());
				List<Long> hdfsUsageList = new ArrayList<Long>();
				hdfsUsageList.add(httpContentSummary.getSpaceConsumed());
				hdfsUsageList.add(httpContentSummary.getLength());
				contentSummaryMap.put(fileStatusObj.getPath().toString(),
						hdfsUsageList);
			}
		}
		return contentSummaryMap;
	}

	public Map getNgdbTotalSummary() throws IOException {
		HdfsManager hdfsManager = new HdfsManager(hadoopConfigurationPath);
		Path path = new Path("/");
		Map<String, Long> usageSpaceMap = new HashMap<String, Long>();
		FileSystem fs = hdfsManager.getFileSystem();
		long capacity = fs.getStatus(path).getCapacity();
		long used = fs.getStatus(path).getUsed();
		long remaining = fs.getStatus(path).getRemaining();
		usageSpaceMap.put("Total Size", capacity);
		usageSpaceMap.put("Total Used", used);
		usageSpaceMap.put("Remaining", remaining);
		return usageSpaceMap;
	}
}