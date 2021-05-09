
package com.nokia.monitoring.hive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BacklogManager extends HdfsManager {

	public BacklogManager(String hadoopConfigurationPath) {
		super(hadoopConfigurationPath);
	}

	public Map<String, Object> getFileCount(String topologyName,
			List<String> filePaths) throws IOException {
		Map<String, Object> topoBacklogCount = new HashMap<String, Object>();
		Map<String, Integer> topoMap = null;
		FileSystem fs = getFileSystem();
		topoMap = new HashMap<String, Integer>();
		for (String filePath : filePaths) {
			Path filePathObj = new Path(String.format("%s/", filePath));
			if (fs.isDirectory(filePathObj) && filePath.contains("mnt")) {
				Path datFilesPath = new Path(
						String.format("%s/*dat", filePath));
				Path processingFilePath = new Path(
						String.format("%s/*processing", filePath));
				Path errorFilesPath = new Path(
						String.format("%s/*error", filePath));
				FileStatus[] datFiles = fs.globStatus(datFilesPath);
				FileStatus[] processingFiles = fs
						.globStatus(processingFilePath);
				FileStatus[] errorFiles = fs.globStatus(errorFilesPath);
				if (datFiles != null) {
					topoMap.put("datFileCountMnt", datFiles.length);
				}
				if (processingFiles != null) {
					topoMap.put("processingDatFileCountMnt",
							processingFiles.length);
				}
				if (errorFiles != null) {
					topoMap.put("errorDatFileCountMnt", errorFiles.length);
				}
			} else if (fs.isDirectory(filePathObj)
					&& filePath.contains("ngdb")) {
				getFileCountRecursively(filePath.trim(), fs, topoMap);
			}
			topoBacklogCount.put(topologyName, topoMap);
		}
		return topoBacklogCount;
	}

	private void getFileCountRecursively(String fileSystem, FileSystem fs,
			Map<String, Integer> topoMap) {
		Path pattern = null;
		String key = null;
		FileStatus[] filesDat = null;
		if (fileSystem.contains("work")) {
			pattern = new Path(String.format("%s/dt=*/tz=*/*dat", fileSystem));
			key = "datFileCountNgdbWork";
		} else {
			pattern = new Path(String.format("%s/dt=*/*dat", fileSystem));
			key = "datFileCountNgdbUsage";
		}
		try {
			filesDat = fs.globStatus(pattern);
			if (filesDat != null) {
				topoMap.put(key, filesDat.length);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
