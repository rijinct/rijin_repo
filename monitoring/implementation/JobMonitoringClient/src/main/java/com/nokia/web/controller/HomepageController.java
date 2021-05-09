
package com.nokia.web.controller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestTemplate;

import com.nokia.web.JobDetails;
import com.nokia.web.constants.PageURL;
import com.nokia.web.constants.View;
import com.nokia.web.rest.JobMonitorWSClient;
import com.nokia.web.util.ConfigUtil;

@Controller
public class HomepageController {

	Logger logger = Logger.getLogger("nokia");

	@Autowired
	JobMonitorWSClient restClient;

	RestTemplate restTemplate;

	String uri = "https://" + ConfigUtil.getConfig().get("EndPointURL") + ":"
			+ ConfigUtil.getConfig().get("EndPointPortNumber")
			+ "/jobMonitoringServer/";
	
	@RequestMapping(value = { PageURL.ROOT, PageURL.HOME })
	public String homepage(final Model model) {
		logger.debug("In HomepageController");
		logger.debug("Creating tabs as per configuration");
		model.addAttribute("ContentPacks",
				ConfigUtil.getConfig().get("ContentPacks"));
		logger.debug("Time frequencies as per configuration");
		model.addAttribute("timeFrequency",
				ConfigUtil.getConfig().get("TimeFrequency"));
		return View.HOMEPAGE;
	}

	@SuppressWarnings("unchecked")
	@RequestMapping(value = PageURL.JOBS, method = RequestMethod.GET)
	public String getJobs(@RequestParam
	String contentPack, @RequestParam
	String frequency, Model model) throws KeyManagementException,
			UnrecoverableKeyException, KeyStoreException,
			NoSuchAlgorithmException, CertificateException,
			FileNotFoundException, IOException {
		logger.debug("Selected content pack :" + contentPack);
		logger.debug("Selected frequency :" + frequency);
		restTemplate = restClient.getRestClient();
		logger.info("Connected to End Point: " + uri);
		List<LinkedHashMap<String, Object>> result = restTemplate.getForObject(
				uri + "/?contentPack={contentPack}&frequency={frequency}",
				List.class, contentPack, frequency);
		List<JobDetails> jobDetails = getModifiedResponse(result);
		model.addAttribute("jobDetails", jobDetails);
		return View.TABLE;
	}

	@SuppressWarnings("unchecked")
	@RequestMapping(value = PageURL.TRACE, method = RequestMethod.GET)
	public String getTraceFile(@RequestParam
	String processId, Model model) throws KeyManagementException,
			UnrecoverableKeyException, KeyStoreException,
			NoSuchAlgorithmException, CertificateException,
			FileNotFoundException, IOException {
		logger.debug("Selected jobName :" + processId);
		String[] traceFileName = (processId.split(","));
		restTemplate = restClient.getRestClient();
		String trace = restTemplate.getForObject(uri
				+ "/traceFileDisplay?processId={processId}", String.class,
				traceFileName[0]);
		// List<JobDetails> jobDetails = getModifiedResponse(result);
		// String trace="Hi there this is test.!";
		model.addAttribute("jobStartTime", traceFileName[2]);
		model.addAttribute("jobEndTime", traceFileName[1]);
		model.addAttribute("traceFileDisplay", trace);
		logger.info("jobStartTime " + traceFileName[1]);
		logger.info("jobEndTime " + traceFileName[2]);
		logger.info("TRACE FILE DISPLAY : " + trace);
		return View.TRACEFILEDISPLAY;
	}

	@SuppressWarnings("unchecked")
	@RequestMapping(value = PageURL.DRILLDOWN, method = RequestMethod.GET)
	public String getJobDetails(@RequestParam
	String sourceTable, Model model) throws KeyManagementException,
			UnrecoverableKeyException, KeyStoreException,
			NoSuchAlgorithmException, CertificateException,
			FileNotFoundException, IOException {
		logger.debug("Opening Source table Drill Down.");
		model.addAttribute("sourceTable", sourceTable);
		restTemplate = restClient.getRestClient();
		List<LinkedHashMap<String, Object>> result = restTemplate.getForObject(
				uri + "/sourceTableDrillDown?sourceTableName={sourceTable}",
				List.class, sourceTable);
		List<JobDetails> jobDetails = getModifiedResponse(result);
		model.addAttribute("jobDetails", jobDetails);
		/*
		 * List<JobDetails> jobDetails = getModifiedResponse(result);
		 * model.addAttribute("jobDetails", jobDetails);
		 * logger.info("Job Details : "+jobDetails);
		 */
		return View.DRILLDOWN;
	}

	@SuppressWarnings("unchecked")
	@RequestMapping(value = PageURL.DRILLDOWN_FOR_CHART, method = RequestMethod.GET)
	public String getJobDetailsForChart(@RequestParam
	String sourceTable, @RequestParam
	String jobName, Model model) throws KeyManagementException,
			UnrecoverableKeyException, KeyStoreException,
			NoSuchAlgorithmException, CertificateException,
			FileNotFoundException, IOException {
		logger.debug("Opening Source table Drill Down.");
		model.addAttribute("sourceTable", sourceTable);
		restTemplate = restClient.getRestClient();
		List<LinkedHashMap<String, Object>> result = restTemplate
				.getForObject(
						uri
								+ "/sourceChartDrillDown?sourceTableName={sourceTable}&jobName={jobName}",
						List.class, sourceTable, jobName);
		List<JobDetails> jobDetails = getModifiedResponse(result);
		model.addAttribute("jobDetails", jobDetails);
		return View.DRILLDOWN_CHART;
	}

	@SuppressWarnings("unchecked")
	@RequestMapping(value = PageURL.JOB_TRACE, method = RequestMethod.GET)
	public String getJobs(@RequestParam
	String jobName, @RequestParam
	String contentPack, @RequestParam
	String frequency, Model model) throws KeyManagementException,
			UnrecoverableKeyException, KeyStoreException,
			NoSuchAlgorithmException, CertificateException,
			FileNotFoundException, IOException {
		logger.debug("Selected jobName :" + jobName);
		restTemplate = restClient.getRestClient();
		List<JobDetails> result = restTemplate.getForObject(uri
				+ "/jobTrace?jobName={jobName}", List.class, jobName);
		// List<JobDetails> jobDetails = getModifiedResponse(result);
		model.addAttribute("jobDetails", result);
		model.addAttribute("selectedJobName", jobName);
		model.addAttribute("contentPack", contentPack);
		model.addAttribute("frequency", "Day");
		return View.JOB_DETAILS;
	}

	private List<JobDetails> getModifiedResponse(
			List<LinkedHashMap<String, Object>> result) {
		List<JobDetails> jobDetails = new ArrayList<JobDetails>();
		if (!result.isEmpty()) {
			for (LinkedHashMap<String, Object> linkedHashMap : result) {
				JobDetails job = new JobDetails();
				job.setJobName(linkedHashMap.get("jobName").toString());
				job.setBoundary((linkedHashMap.get("boundary")) != null
						? (linkedHashMap.get("boundary").toString()) : "");
				job.setSourceTable(linkedHashMap.get("sourceTable").toString());
				job.setDeviation(new Boolean(linkedHashMap.get("deviation")
						.toString()));
				job.setTargetTable(linkedHashMap.get("targetTable").toString());
				jobDetails.add(job);
			}
		}
		return jobDetails;
	}
}
