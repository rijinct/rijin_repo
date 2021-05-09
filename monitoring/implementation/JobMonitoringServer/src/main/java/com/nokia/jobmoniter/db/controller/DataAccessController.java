package com.nokia.jobmoniter.db.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import com.nokia.jobmoniter.rest.constants.JobMonitorConstants;
import com.nokia.jobmoniter.rest.dao.JDBCJobDetailsDAO;
import com.nokia.jobmoniter.rest.service.JobDetailsService;

@Controller
public class DataAccessController {

	Logger logger = Logger.getLogger("nokia");

	@Autowired
	JDBCJobDetailsDAO jdbcJobDetailsDAO;
	@Autowired
	JobDetailsService jobDetailsService;

	public List<JobDetails> getJobDetailsObjList(String contentPackName,
			String frequency) {

		HashMap<String, Integer> deviationMap = new HashMap<String, Integer>();
		deviationMap = jobDetailsService.getDeviation();

		if (frequency.equalsIgnoreCase(JobMonitorConstants.ALL)) {
			frequency = null;
		}
		Map<String, List<String>> jobDetailsMap = jobDetailsService
				.getJobDetailsByCPName(contentPackName, frequency);
		List<String> tableDetails = new ArrayList<String>();
		List<JobDetails> jobDetailsObjectList = new ArrayList<JobDetails>();
		if (jobDetailsMap.isEmpty()) {
			if (frequency == null) {
				jobDetailsObjectList.addAll(jdbcJobDetailsDAO
						.findJobDetailsForAll(deviationMap));
			} else {
				jobDetailsObjectList.addAll(jdbcJobDetailsDAO
						.findJobDetailsForAllWithFrequency(deviationMap,
								frequency));
			}
		} else {
			for (Entry<String, List<String>> entry : jobDetailsMap.entrySet()) {
				jobDetailsObjectList.addAll(jdbcJobDetailsDAO
						.findJobDetailsByTableNames(entry.getValue(), (Arrays
								.asList((entry.getKey()).split("_"))).get(1),
								deviationMap));
			}
		}

		return jobDetailsObjectList;
	}

	public List<JobDetails> getJobTraceDetails(String jobName) {
		List<String> jobNameList = Arrays.asList(jobName.split(","));
		List<JobDetails> jobDetailsObjectList = new ArrayList<JobDetails>();
		jobDetailsObjectList.addAll(jdbcJobDetailsDAO
				.findJobTraceDetailsByJobName(jobNameList));
		return jobDetailsObjectList;
	}

	public List<JobDetails> getSourceTableDetails(String sourceTableName) {
		HashMap<String, Integer> deviationMap = new HashMap<String, Integer>();
		deviationMap = jobDetailsService.getDeviation();
		List<String> jobNameList = Arrays.asList(sourceTableName.split(","));
		List<JobDetails> jobDetailsObjectList = new ArrayList<JobDetails>();
		jobDetailsObjectList.addAll(jdbcJobDetailsDAO.findSourceTableDetails(
				jobNameList, deviationMap));
		return jobDetailsObjectList;
	}
}