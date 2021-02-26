package com.rijin.dqhi.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HdfsUtil extends HdfsManager {

	public HdfsUtil(String hadoopConfigurationPath) {
		super(hadoopConfigurationPath);
	}

	public List<String> getFiles(String hdfsPath) throws IOException {
		FileSystem fs = getFileSystem();
		Path path = new Path(hdfsPath);
		FileStatus[] fileStatus = fs.listStatus(path);
		List<String> fileList = new ArrayList<String>();
		for(FileStatus status : fileStatus)
		{
		    fileList.add(status.getPath().getName());
		}
	    
		return fileList;
	}
	
	public List<String> getFilesWithAbsolutePath(String hdfsPath) throws IOException {
        FileSystem fs = getFileSystem();
        Path path = new Path(hdfsPath);
        FileStatus[] fileStatus = fs.listStatus(path);
        List<String> fileList = new ArrayList<String>();
        for(FileStatus status : fileStatus)
        {
            fileList.add(status.getPath().toString());
        }
        
        return fileList;
    }
	
	public Long getFileModificationTime(String hdfsPath) throws IOException {
        FileSystem fs = getFileSystem();
        Path hdfs = new Path(hdfsPath);
        Long fileTimeStamp = null;
        if (fs.exists(hdfs))
        {
        FileStatus fileStatus = fs.getFileStatus(hdfs);
        fileTimeStamp =  fileStatus.getModificationTime();
        }
        return fileTimeStamp;
    }
	
	public void copyFilesToLocal(String localPath,String hdfsPath) throws IOException {
        FileSystem fs = getFileSystem();
        Path local = new Path(localPath);
        Path hdfs = new Path(hdfsPath);
        if (fs.exists(hdfs))
        {
            fs.copyToLocalFile(false, hdfs, local, true);
        }
    } 
	
	

}