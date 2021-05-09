package com.nokia.jobmoniter.rest.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface JobDetailsService {

	Map<String, List<String>> getJobDetailsByCPName(String CPName, String frequency);

	HashMap<String, Integer> getDeviation();

}
