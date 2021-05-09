package com.nokia.jobmoniter.rest.dao.impl;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.nokia.jobmoniter.db.controller.JobDetails;
import com.nokia.jobmoniter.rest.dao.JDBCJobDetailsDAO;
import com.nokia.jobmoniter.rest.util.ConfigUtil;
import com.nokia.jobmoniter.rest.util.ReadPropertyFile;

public class JDBCJobDetailsDAOImpl implements JDBCJobDetailsDAO {
	private DataSource dataSource;
	private JdbcTemplate jdbcTemplate;

	public JDBCJobDetailsDAOImpl() {
		Properties config = ConfigUtil.loadDatabaseProperties();
		this.dataSource = new DriverManagerDataSource(
				config.getProperty("db_url"),
				config.getProperty("sairepo_username"),
				config.getProperty("sairepo_password"));
	}

	Logger logger = Logger.getLogger("nokia");
	DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public JobDetails findById(int id) {

		String sql = "SELECT * FROM JobDetails WHERE ID = ?";

		jdbcTemplate = new JdbcTemplate(dataSource);
		JobDetails JobDetails = (JobDetails) jdbcTemplate.queryForObject(sql,
				new Object[] { id },
				new BeanPropertyRowMapper(JobDetails.class));

		return JobDetails;
	}

	@SuppressWarnings("rawtypes")
	public List<JobDetails> findAll() {

		jdbcTemplate = new JdbcTemplate(dataSource);
		String sql = "select jobid,id from sairepo.boundary where JOBID like '%HOUR%' order by MAXVALUE";

		List<JobDetails> jobDetails = new ArrayList<JobDetails>();

		List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
		for (Map row : rows) {
			JobDetails jobDetail = new JobDetails();

		}

		return jobDetails;
	}

	@SuppressWarnings("rawtypes")
	public List<JobDetails> findJobDetailsByTableNames(List<String> tableNames,
			String frequency, HashMap<String, Integer> deviationMap) {
		String inClause = getInClause(tableNames);
		jdbcTemplate = new JdbcTemplate(dataSource);
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT JOB_NAME, ");
		sql.append("TO_CHAR(SAIREPO.BOUNDARY.MAXVALUE,'MM/dd/yyyy HH24:MI:SS') AS BOUNDARY, ");
		sql.append("SAIREPO.JOB_PROP.PARAMVALUE                                AS SOURCE_TABLE, ");
		sql.append("TARGET_TABLE ");
		sql.append("FROM ");
		sql.append("(SELECT SAIREPO.JOB_PROP.PARAMVALUE AS TARGET_TABLE, ");
		sql.append("SAIREPO.JOB_DICTIONARY.JOBID      AS JOB_NAME, ");
		sql.append("SAIREPO.JOB_PROP.JOBID ");
		sql.append("FROM SAIREPO.JOB_PROP ");
		sql.append("JOIN SAIREPO.JOB_DICTIONARY ");
		sql.append("ON (SAIREPO.JOB_PROP.JOBID         =SAIREPO.JOB_DICTIONARY.ID) ");
		sql.append("WHERE SAIREPO.JOB_PROP.PARAMVALUE IN ('" + inClause + "') ");
		sql.append("  and SAIREPO.JOB_DICTIONARY.JOBID like ('%" + frequency
				+ "%') ) A ");
		sql.append("JOIN SAIREPO.JOB_PROP ");
		sql.append("ON A.JOBID= SAIREPO.JOB_PROP.JOBID ");
		sql.append("JOIN SAIREPO.BOUNDARY ");
		sql.append("ON A.JOB_NAME                   =SAIREPO.BOUNDARY.JOBID ");
		sql.append("WHERE SAIREPO.JOB_PROP.PARAMNAME='SOURCE'  ");

		logger.debug("***********Data Access Layer : Generating query to get Job Details for content pack- Start***********");
		logger.debug(sql);
		logger.debug("***********Data Access Layer : Generating query to get Job Details for content pack- End***********");
		List<JobDetails> jobDetails = new ArrayList<JobDetails>();

		List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql
				.toString());

		for (Map row : rows) {
			JobDetails jobDetail = new JobDetails();
			jobDetail.setJobName((String) row.get("JOB_NAME"));
			jobDetail.setBoundary((String) row.get("BOUNDARY"));
			jobDetail.setSourceTable((String) row.get("SOURCE_TABLE"));
			jobDetail.setTargetTable((String) row.get("TARGET_TABLE"));
			jobDetail.setDeviation(getDeviation((String) row.get("BOUNDARY"),
					deviationMap, (String) row.get("JOB_NAME")));
			jobDetails.add(jobDetail);
		}

		return jobDetails;
	}

	private Boolean getDeviation(String string,
			HashMap<String, Integer> deviationMap, String job_name) {
		Boolean deviation = false;
		if (StringUtils.isBlank(string)) {
			return deviation;
		}
		if (StringUtils.contains(job_name, "HOUR")) {
			deviation = getDeviationForHour(deviationMap, string);
		} else if (StringUtils.contains(job_name, "DAY")) {
			deviation = getDeviationForDay(deviationMap, string);
		} else if (StringUtils.contains(job_name, "WEEK")) {
			deviation = getDeviationForWeek(deviationMap, string);
		} else if (StringUtils.contains(job_name, "MONTH")) {
			deviation = getDeviationForMonth(deviationMap, string);
		}
		return deviation;
	}

	private Boolean getDeviationForMonth(HashMap<String, Integer> deviationMap,
			String boundary_date) {

		Boolean deviation = false;
		String system_date = dateFormat
				.format(Calendar.getInstance().getTime());
		Date d1 = null;
		Date d2 = null;

		try {
			d1 = dateFormat.parse(system_date);
			d2 = dateFormat.parse(boundary_date);
			Calendar calDate = Calendar.getInstance();
			calDate.setTime(d1);
			calDate.set(Calendar.MILLISECOND, 0);
			calDate.set(Calendar.SECOND, 0);
			calDate.set(Calendar.MINUTE, 0);
			calDate.set(Calendar.HOUR_OF_DAY, 0);
			d2.setHours(0);
			d2.setMinutes(0);
			d2.setSeconds(0);
			calDate.add(Calendar.MONTH, -1);
			calDate.set(Calendar.DAY_OF_MONTH,
					calDate.getActualMinimum(Calendar.DAY_OF_MONTH));
			if (calDate.getTimeInMillis() - d2.getTime() != 0) {
				deviation = true;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return deviation;
	}

	private Boolean getDeviationForWeek(HashMap<String, Integer> deviationMap,
			String boundary_date) {

		Boolean deviation = false;
		String system_date = dateFormat
				.format(Calendar.getInstance().getTime());
		Date d1 = null;
		Date d2 = null;

		try {
			d1 = dateFormat.parse(system_date);
			d2 = dateFormat.parse(boundary_date);

			int deviationDays = deviationMap.get("DAY");
			int deviationHours = deviationMap.get("HOUR");
			// in milliseconds
			long comparisonDate = d1.getTime()
					+ (((deviationDays) * 24 * 3600 * 1000l) + (deviationHours * 3600 * 1000l))
					- (24 * 7 * 3600 * 1000l);
			;
			long deviatedDate = d2.getTime()
					+ (((deviationDays) * 24 * 3600 * 1000l) + (deviationHours * 3600 * 1000l));
			Calendar cal = getDateForWeekBegining(new Date(comparisonDate));
			if (((cal.getTimeInMillis()) - (deviatedDate))
					/ (24 * 60 * 60 * 1000) != 0) {
				deviation = true;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return deviation;
	}

	private Calendar getDateForWeekBegining(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		while (c.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY) {
			c.add(Calendar.DATE, -1);
		}
		return c;
	}

	private Boolean getDeviationForDay(HashMap<String, Integer> deviationMap,
			String boundary_date) {

		Boolean deviation = false;
		String system_date = dateFormat
				.format(Calendar.getInstance().getTime());
		Date d1 = null;
		Date d2 = null;

		try {
			d1 = dateFormat.parse(system_date);
			d2 = dateFormat.parse(boundary_date);

			int deviationDays = deviationMap.get("DAY");
			int deviationHours = deviationMap.get("HOUR");
			long comparisonDate = d1.getTime()
					+ (((deviationDays) * 24 * 3600 * 1000l) + (deviationHours * 3600 * 1000l))
					- (24 * 3600 * 1000l);
			
			if (((comparisonDate) - (d2.getTime())) / (24 * 60 * 60 * 1000) != 0) {
				deviation = true;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return deviation;

	}

	private Boolean getDeviationForHour(HashMap<String, Integer> deviationMap,
			String boundary_date) {
		Boolean deviation = false;
		String system_date = dateFormat
				.format(Calendar.getInstance().getTime());
		boundary_date = boundary_date.replace("-", "/");
		Date d1 = null;
		Date d2 = null;

		try {
			d1 = dateFormat.parse(system_date);
			d2 = dateFormat.parse(boundary_date);
			int deviationDays = deviationMap.get("DAY");
			int deviationHours = deviationMap.get("HOUR");
			long diff = d2.getTime() - d1.getTime();
			long diffHours = diff / (60 * 60 * 1000) % 24;
			long diffDays = diff / (24 * 60 * 60 * 1000);
			diffHours = diffDays * 24 + diffHours;
			int totalDeviationHours = deviationDays * 24 + deviationHours;
			if (diffHours - totalDeviationHours < 3
					&& diffHours - totalDeviationHours < -3) {
				deviation = true;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return deviation;

	}

	private String getInClause(List<String> tableNames) {
		String inClause;
		inClause = StringUtils.join(tableNames, "','");

		return inClause;
	}

	@SuppressWarnings("rawtypes")
	public List<JobDetails> findJobTraceDetailsByJobName(List<String> tableNames) {
		String inClause = getInClause(tableNames);

		jdbcTemplate = new JdbcTemplate(dataSource);
		StringBuffer sql = new StringBuffer();

		sql.append(" SELECT SAIREPO.ETL_STATUS.JOB_NAME AS JOB_NAME,"
				+ " TO_CHAR(SAIREPO.ETL_STATUS.START_TIME,'MM/DD/YYYY HH24:MI:SS') AS START_TIME,"
				+ " TO_CHAR(SAIREPO.ETL_STATUS.END_TIME,'MM/DD/YYYY HH24:MI:SS') AS END_TIME,"
				+ " TO_CHAR(SAIREPO.ETL_STATUS.LOAD_TIME,'MM/DD/YYYY HH24:MI:SS') AS LOAD_TIME,"
				+ " SAIREPO.ETL_STATUS.PROC_ID AS PROCESS_ID,"
				+ " SAIREPO.ETL_STATUS.TRACE_FILE AS TRACE_FILE from SAIREPO.ETL_STATUS"
				+ " where SAIREPO.ETL_STATUS.JOB_NAME = '" + inClause + "';");
		List<JobDetails> jobDetails = new ArrayList<JobDetails>();

		logger.debug("Max no of configured rows for traces: "
				+ ReadPropertyFile.getPropertyFile().getProperty(
						"NO_OF_TRACES_ROWS"));
		logger.debug("***********Data Access Layer : Generating query to get Job Trace based on job name- Start***********");
		logger.debug(sql);
		logger.debug("***********Data Access Layer : Generating query to get Job Trace based on job name- End***********");

		List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql
				.toString());

		for (Map row : rows) {
			JobDetails jobDetail = new JobDetails();
			jobDetail.setJobName((String) row.get("JOB_NAME"));
			jobDetail.setProcessId((BigDecimal) row.get("PROCESS_ID"));
			jobDetail.setStartTime((String) row.get("START_TIME"));
			jobDetail.setEndTime((String) row.get("END_TIME"));
			jobDetail.setLoadTime((String) row.get("LOAD_TIME"));
			jobDetail.setStartTime((String) row.get("START_TIME"));
			jobDetail.setEndTime((String) row.get("END_TIME"));
			jobDetail.setLoadTime((String) row.get("LOAD_TIME"));
			jobDetail.setTraceFile((String) row.get("TRACE_FILE"));
			jobDetail.setTimeTaken(getTimeTaken((String) row.get("START_TIME"),
					(String) row.get("END_TIME")));
			jobDetails.add(jobDetail);
		}

		return jobDetails;
	}

	private String getTimeTaken(String startTime, String endTime) {
		Date d1 = null;
		Date d2 = null;
		String timeTaken = new String();
		try {
			d1 = dateFormat.parse(startTime);
			d2 = dateFormat.parse(endTime);
			long diff = d2.getTime() - d1.getTime();

			long diffSeconds = diff / 1000 % 60;
			long diffMinutes = diff / (60 * 1000) % 60;
			long diffHours = diff / (60 * 60 * 1000) % 24;
			long diffDays = diff / (24 * 60 * 60 * 1000);
			timeTaken += diffDays + "d " + diffHours + "h " + diffMinutes
					+ "m " + diffSeconds + "s";
		} catch (Exception e) {
			logger.debug("Issue in getTimeTaken");
		}
		return timeTaken;
	}

	@Override
	public List<JobDetails> findSourceTableDetails(List<String> tableNames,
			HashMap<String, Integer> deviationMap) {
		String inClause = getInClause(tableNames);
		jdbcTemplate = new JdbcTemplate(dataSource);
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT JOB_NAME,");
		sql.append(" TO_CHAR(SAIREPO.BOUNDARY.MAXVALUE,'MM/dd/yyyy HH24:MI:SS') AS BOUNDARY,");
		sql.append(" SAIREPO.JOB_PROP.PARAMVALUE                                AS SOURCE_TABLE,");
		sql.append(" TARGET_TABLE");
		sql.append(" FROM");
		sql.append(" (SELECT SAIREPO.JOB_PROP.PARAMVALUE AS TARGET_TABLE,");
		sql.append(" SAIREPO.JOB_DICTIONARY.JOBID      AS JOB_NAME,");
		sql.append(" SAIREPO.JOB_PROP.JOBID");
		sql.append(" FROM SAIREPO.JOB_PROP");
		sql.append(" JOIN SAIREPO.JOB_DICTIONARY");
		sql.append(" ON (SAIREPO.JOB_PROP.JOBID=SAIREPO.JOB_DICTIONARY.ID)");
		sql.append(" WHERE SAIREPO.JOB_PROP.PARAMVALUE IN ('" + inClause
				+ "') ");
		sql.append(" AND SAIREPO.JOB_PROP.PARAMNAME='TARGET'");
		sql.append(" ) A");
		sql.append(" JOIN SAIREPO.JOB_PROP");
		sql.append(" ON A.JOBID= SAIREPO.JOB_PROP.JOBID");
		sql.append(" JOIN SAIREPO.BOUNDARY");
		sql.append(" ON A.JOB_NAME=SAIREPO.BOUNDARY.JOBID");
		sql.append(" WHERE SAIREPO.JOB_PROP.PARAMNAME='SOURCE'");

		logger.debug("***********Data Access Layer : Generating query to get source table- Start***********");
		logger.debug(sql);
		logger.debug("***********Data Access Layer : Generating query to get source table- End***********");
		List<JobDetails> jobDetails = new ArrayList<JobDetails>();

		List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql
				.toString());

		for (Map row : rows) {
			JobDetails jobDetail = new JobDetails();
			jobDetail.setJobName((String) row.get("JOB_NAME"));
			jobDetail.setBoundary((String) row.get("BOUNDARY"));
			jobDetail.setSourceTable((String) row.get("SOURCE_TABLE"));
			jobDetail.setTargetTable((String) row.get("TARGET_TABLE"));
			jobDetail.setDeviation(getDeviation((String) row.get("BOUNDARY"),
					deviationMap, (String) row.get("JOB_NAME")));
			jobDetails.add(jobDetail);
		}

		return jobDetails;
	}

	@Override
	public List<JobDetails> findJobDetailsForAll(
			HashMap<String, Integer> deviationMap) {
		jdbcTemplate = new JdbcTemplate(dataSource);

		String sql = String
				.format("SELECT JOB_NAME, TO_CHAR(SAIREPO.BOUNDARY.MAXVALUE,'MM/dd/yyyy HH24:MI:SS') AS BOUNDARY, SAIREPO.JOB_PROP.PARAMVALUE AS SOURCE_TABLE, TARGET_TABLE FROM  (SELECT SAIREPO.JOB_PROP.PARAMVALUE AS TARGET_TABLE, SAIREPO.JOB_DICTIONARY.JOBID  AS JOB_NAME, SAIREPO.JOB_PROP.JOBID FROM SAIREPO.JOB_PROP JOIN SAIREPO.JOB_DICTIONARY ON (SAIREPO.JOB_PROP.JOBID  =SAIREPO.JOB_DICTIONARY.ID)  where  SAIREPO.JOB_PROP.PARAMNAME='TARGET' ) A JOIN SAIREPO.JOB_PROP ON A.JOBID= SAIREPO.JOB_PROP.JOBID JOIN SAIREPO.BOUNDARY ON A.JOB_NAME  =SAIREPO.BOUNDARY.JOBID WHERE SAIREPO.JOB_PROP.PARAMNAME='SOURCE' AND JOB_NAME like ('%%Perf%%') ");

		logger.debug("***********Data Access Layer : Generating query to get Job Details for content pack- Start***********");
		logger.debug(sql);
		logger.debug("***********Data Access Layer : Generating query to get Job Details for content pack- End***********");
		List<JobDetails> jobDetails = new ArrayList<JobDetails>();

		List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql
				.toString());

		for (Map row : rows) {
			JobDetails jobDetail = new JobDetails();
			jobDetail.setJobName((String) row.get("JOB_NAME"));
			jobDetail.setBoundary((String) row.get("BOUNDARY"));
			jobDetail.setSourceTable((String) row.get("SOURCE_TABLE"));
			jobDetail.setTargetTable((String) row.get("TARGET_TABLE"));
			jobDetail.setDeviation(getDeviation((String) row.get("BOUNDARY"),
					deviationMap, (String) row.get("JOB_NAME")));
			jobDetails.add(jobDetail);
		}

		return jobDetails;
	}

	@Override
	public List<JobDetails> findJobDetailsForAllWithFrequency(
			HashMap<String, Integer> deviationMap, String frequency) {
		logger.debug("Inside findJobDetailsForAllWithFrequency");
		jdbcTemplate = new JdbcTemplate(dataSource);

		String sql = String
				.format("SELECT JOB_NAME, TO_CHAR(SAIREPO.BOUNDARY.MAXVALUE,'MM/dd/yyyy HH24:MI:SS') AS BOUNDARY, SAIREPO.JOB_PROP.PARAMVALUE AS SOURCE_TABLE, TARGET_TABLE FROM  (SELECT SAIREPO.JOB_PROP.PARAMVALUE AS TARGET_TABLE, SAIREPO.JOB_DICTIONARY.JOBID  AS JOB_NAME, SAIREPO.JOB_PROP.JOBID FROM SAIREPO.JOB_PROP JOIN SAIREPO.JOB_DICTIONARY ON (SAIREPO.JOB_PROP.JOBID  =SAIREPO.JOB_DICTIONARY.ID)  where  SAIREPO.JOB_PROP.PARAMNAME='TARGET' ) A JOIN SAIREPO.JOB_PROP ON A.JOBID= SAIREPO.JOB_PROP.JOBID JOIN SAIREPO.BOUNDARY ON A.JOB_NAME  =SAIREPO.BOUNDARY.JOBID WHERE SAIREPO.JOB_PROP.PARAMNAME='SOURCE' AND JOB_NAME like ('%%Perf%%') AND JOB_NAME like ('%%%s%%')",
						frequency);

		logger.debug("***********Data Access Layer : Generating query to get Job Details for content pack- Start***********");
		logger.debug(sql);
		logger.debug("***********Data Access Layer : Generating query to get Job Details for content pack- End***********");
		List<JobDetails> jobDetails = new ArrayList<JobDetails>();

		List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql
				.toString());

		for (Map row : rows) {
			JobDetails jobDetail = new JobDetails();
			jobDetail.setJobName((String) row.get("JOB_NAME"));
			jobDetail.setBoundary((String) row.get("BOUNDARY"));
			jobDetail.setSourceTable((String) row.get("SOURCE_TABLE"));
			jobDetail.setTargetTable((String) row.get("TARGET_TABLE"));
			jobDetail.setDeviation(getDeviation((String) row.get("BOUNDARY"),
					deviationMap, (String) row.get("JOB_NAME")));
			jobDetails.add(jobDetail);
		}

		return jobDetails;
	}
}
