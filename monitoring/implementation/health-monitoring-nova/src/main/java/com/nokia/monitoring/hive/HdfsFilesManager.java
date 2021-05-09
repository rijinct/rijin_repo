
package com.nokia.monitoring.hive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.nokia.analytics.logging.AnalyticsLogger;
import com.nokia.analytics.logging.AnalyticsLoggerFactory;

public class HdfsFilesManager extends HdfsManager {

	private FileSystem fs;

	private static final SimpleDateFormat SDF = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HdfsFilesManager.class);

	public HdfsFilesManager(String hadoopConfigurationPath) throws IOException {
		super(hadoopConfigurationPath);
		this.fs = getFileSystem();
	}

	public List<Map<String, String>> getCtrAndDatFiles(String hdfsPath)
			throws IOException {
		List<Map<String, String>> output = new ArrayList<Map<String, String>>();
		output.add(getFilesWithModificationTime(hdfsPath, ".ctr"));
		output.add(getFilesWithModificationTime(hdfsPath, ".dat"));
		return output;
	}

	public Map<String, String> getFilesWithModificationTime(String hdfsPath,
			String pattern) throws IOException, FileNotFoundException {
		Map<String, String> files = new HashMap<String, String>();
		Path dirPath = new Path(hdfsPath);
		if (fs.isDirectory(dirPath)) {
			files = getFileNamesAndModifiationTimes(files, hdfsPath, pattern);
		} else {
			LOGGER.debug("Path: " + hdfsPath + " does not exist.");
			files.put(hdfsPath, "Invalid Path");
		}
		return files;
	}

	public Map<String, List<String>> getFilesWithSizes(String hdfsPath,
			String pattern) throws IOException {
		Map<String, List<String>> files = new HashMap<String, List<String>>();
		List<String> fileInfo = null;
		Path dirPath = new Path(hdfsPath);
		if (fs.isDirectory(dirPath)) {
			Path filter = new Path(String.format("%s/*%s", hdfsPath, pattern));
			FileStatus[] matches = fs.globStatus(filter);
			if (matches != null) {
				for (FileStatus match : matches) {
					fileInfo = new ArrayList<String>();
					String fileName = match.getPath().getName();
					fileInfo.add(String.format("%d", match.getLen()));
					fileInfo.add(getLastModificationTime(match));
					files.put(fileName, fileInfo);
				}
			}
		}
		return files;
	}

	private Map<String, String> getFileNamesAndModifiationTimes(
			Map<String, String> files, String hdfsPath, String pattern)
			throws IOException, FileNotFoundException {
		Path filter = new Path(String.format("%s/*%s", hdfsPath, pattern));
		FileStatus[] matches = fs.globStatus(filter);
		if (matches != null) {
			for (FileStatus match : matches) {
				String fileName = match.getPath().getName();
				files.put(fileName, getLastModificationTime(match));
			}
		}
		return files;
	}

	private String getLastModificationTime(FileStatus status) {
		long modificationTime = status.getModificationTime();
		Date modificationTimestamp = new Date(modificationTime);
		return SDF.format(modificationTimestamp);
	}
}