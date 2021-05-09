package com.nokia.jobmoniter.rest.dao;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import com.nokia.jobmoniter.db.controller.JobDetails;

public interface JDBCJobDetailsDAO {

	public JobDetails findById(int id);
	public List<JobDetails> findAll();
	public List<JobDetails> findJobDetailsByTableNames(List<String> tableNames, String frequency, HashMap<String, Integer> deviationMap);
	public List<JobDetails> findJobTraceDetailsByJobName(List<String> tableNames) ;
	public List<JobDetails> findSourceTableDetails(List<String> tableNames, HashMap<String, Integer> deviationMap) ;
	public List<JobDetails> findJobDetailsForAll(HashMap<String, Integer> deviationMap);
	public List<JobDetails> findJobDetailsForAllWithFrequency(HashMap<String, Integer> deviationMap, String frequency);
}
