package com.nokia.jobmoniter.jobdetails.service.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.jcraft.jsch.JSchException;
import com.nokia.jobmoniter.rest.dao.impl.JDBCJobDetailsDAOImpl;
import com.nokia.jobmoniter.rest.service.JobDetailsService;
import com.nokia.jobmoniter.rest.util.ReadPropertyFile;


public class JobDetailsServiceImpl implements JobDetailsService {
	JDBCJobDetailsDAOImpl jdbcJobDetailsDAOimpl=new JDBCJobDetailsDAOImpl();
	ReadPropertyFile properties= new ReadPropertyFile();
	@Override
	public Map<String, List<String>> getJobDetailsByCPName(String CPName, String frequency) {
		
		 Map<String, List<String>> propertyDetails = properties.readPropertyFile(CPName,frequency);
		
			return propertyDetails;
		
	}
	@Override
	public HashMap<String, Integer> getDeviation() {
		
		HashMap<String, Integer> deviationMap=properties.populateDeviationMap();
		return deviationMap;
	}
	
}
