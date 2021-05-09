package com.nokia.jobmoniter.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.nokia.jobmoniter.db.controller.DataAccessController;
import com.nokia.jobmoniter.db.controller.JobDetails;
import com.nokia.jobmoniter.rest.constants.PageURL;

@RestController
public class RestServiceController {
	Logger logger = Logger.getLogger("nokia");

	@Autowired
	DataAccessController dataAccessController;
	
	@RequestMapping(value = PageURL.ROOT, method=RequestMethod.GET, produces=MediaType.APPLICATION_JSON_VALUE)
	public List<JobDetails> homepage(@RequestParam String contentPack,@RequestParam String frequency) {
		
		logger.debug("Web Service : Getting Job Table - Start");
		List<JobDetails> jobDetails= dataAccessController.getJobDetailsObjList(contentPack.toUpperCase(), frequency.toUpperCase());
		logger.debug("Web Service : Getting Job Table - End");
		return jobDetails;
	}
	
	@RequestMapping(value = PageURL.SOURCE_TABLE_DRILL_DOWN, method=RequestMethod.GET, produces=MediaType.APPLICATION_JSON_VALUE)
	public List<JobDetails> sourceTableDrillDown(@RequestParam String sourceTableName) {
		
		logger.debug("Web Service : Getting Source Table - Start");
		List<JobDetails> jobDetails= dataAccessController.getSourceTableDetails(sourceTableName);
		logger.debug("Web Service : Getting Source Table - End");
		return jobDetails;
	}
	@RequestMapping(value = PageURL.SOURCE_CHART_DRILL_DOWN, method=RequestMethod.GET, produces=MediaType.APPLICATION_JSON_VALUE)
	public List<JobDetails> sourceTableDrillDown(@RequestParam String sourceTableName,@RequestParam String jobName) {
		
		logger.debug("Web Service : Getting Source Table - Start");
		List<JobDetails> jobDetails= dataAccessController.getSourceTableDetails(sourceTableName);
		logger.debug("Web Service : Getting Source Table - End");
		return jobDetails;
	}
	
	@RequestMapping(value = PageURL.JOB_TRACE, method=RequestMethod.GET, produces=MediaType.APPLICATION_JSON_VALUE)
	public List<JobDetails> jobTrace(@RequestParam String jobName) {
		
		logger.debug("Web Service : Getting Trace Table:: - Start");
		List<JobDetails> jobDetails= dataAccessController.getJobTraceDetails(jobName);
		logger.debug("Web Service : Getting Job Table - End");
		return jobDetails;
	}
}