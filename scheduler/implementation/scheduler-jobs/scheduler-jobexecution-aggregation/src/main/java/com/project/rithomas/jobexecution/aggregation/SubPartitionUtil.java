package com.project.rithomas.jobexecution.aggregation;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.etl.common.PlevelValues;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class SubPartitionUtil {

	private static final String PART_VALUES_QUERY = "select %s from %s where dt='%s' and tz='%s' group by %s";

	private static final String DELETE_PART_VALUES_INFO = "delete from sub_partition_values where table_name='%s' and dt=%s and tz='%s'";
	
