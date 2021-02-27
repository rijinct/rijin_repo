package com.project.rithomas.jobexecution.common.util;

import java.util.List;

import com.beust.jcommander.Parameter;

public class PopulateReaggParamConfig {

	@Parameter(names = { "-e" })
	List<String> enableGroupList;

	@Parameter(names = { "-d" })
	List<String> disableGroupList;

	@Parameter(names = { "-start" })
	String startTime;

	@Parameter(names = { "-end" })
	String endTime;

	@Parameter(names = { "-f" })
	String file;

	@Parameter(names = { "-r" })
	String regionId;

}