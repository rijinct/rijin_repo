
package com.project.rithomas.jobexecution.common;

import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.common.cache.service.dataavailability.DataAvailabilityCacheService;
import com.project.rithomas.etl.exception.ETLException;
import com.project.rithomas.etl.hdfs.HDFSClient;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.DQICalculatorUtil;
import com.project.rithomas.jobexecution.common.util.DataAvailabilityCacheUtil;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { GetDBResource.class, DQICalculatorUtil.class,
DBConnectionManager.class,BoundaryQuery.class,
		ConnectionManager.class, DataAvailabilityCacheUtil.class, HiveConfigurationProvider.class })
public class DQICalculatorTest {

	WorkFlowContext context = new JobExecutionContext();

	DQICalculator calculator = new DQICalculator();

	@Mock
	QueryExecutor queryExec;

	@Mock
	ResultSet result, result1, result2, result3, result4, result5, result6,
			result7, result8;

	@Mock
	Connection connection;

	@Mock
	PreparedStatement statement;

	@Mock
	Session session;

	@Mock
	HDFSClient client;

	@Mock
	GetDBResource getDBResource;

	@Mock
	BoundaryQuery boundaryQuery;

	@Mock
	Configuration conf;

	@Mock
	DataAvailabilityCacheService cacheService;
	
	@Mock
	DBConnectionManager dbConnManager;
	
	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;

	@Before
	public void setUp() throws Exception {
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		
		
		mockStatic(GetDBResource.class);
		mockStatic(ConnectionManager.class);
		mockStatic(HiveConfigurationProvider.class);
		
		mockStatic(DataAvailabilityCacheUtil.class);
		mockStatic(DBConnectionManager.class);
		mockStatic(BoundaryQuery.class);
		PowerMockito.when(HiveConfigurationProvider.getInstance()).thenReturn(mockHiveConfigurationProvider);
		
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(getDBResource);
		Mockito.when(getDBResource.getHdfsConfiguration())
				.thenReturn(conf);
		PowerMockito.whenNew(HDFSClient.class).withArguments(conf)
				.thenReturn(client);
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(boundaryQuery);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(connection);
		Mockito.when(connection.createStatement()).thenReturn(statement);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_HOUR_ReaggregateJob");
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE, "HIVE");
		context.setProperty(JobExecutionContext.ADAPTATION_ID, "SMS");
		context.setProperty(JobExecutionContext.ADAPTATION_VERSION, "1");
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
	}

	@Test
	public void testCalculateDqiUsageSource() throws Exception {
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE,
				JobTypeDictionary.USAGE_JOB_TYPE);
		context.setProperty(JobExecutionContext.SOURCE, "US_SMS_1");
		List<String> sourceJobs = new ArrayList<String>();
		sourceJobs.add("Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobs);
		context.setProperty(
				"Usage_SMS_1_LoadJob_" + JobExecutionContext.PARTITION_COLUMN,
				"report_time;event_time");
		context.setProperty(
				"Usage_SMS_1_LoadJob_" + JobExecutionContext.ADAPTATION_ID,
				"SMS_ADAP");
		context.setProperty(
				"Usage_SMS_1_LoadJob_" + JobExecutionContext.ADAPTATION_VERSION,
				"1");
		context.setProperty(JobExecutionContext.DATA_SOURCE_COLUMN,
				"source_id");
		context.setProperty(JobExecutionContext.DQI_THRESHOLD, "10");
		context.setProperty(JobExecutionContext.PERF_JOB_NAME,
				"Perf_SMS_1_HOUR_AggregateJob");
		Object[] sourceTableNameObjArr = new String[] { "US_SMS_1" };
		Mockito.when(queryExec.executeMetadatasqlQuery(
				"select table_name from job_list where jobid ='Usage_SMS_1_LoadJob'",
				context)).thenReturn(sourceTableNameObjArr);
		Mockito.when(statement.executeQuery(
				"select ds_id,ds_name,ds_weight,min(report_time),max(report_time) from es_data_source_name_1 left outer join "
						+ "(select unix_timestamp(event_time) report_time,source_id from US_SMS_1 where dt>='100' and dt<'200') US_SMS_1"
						+ " on(es_data_source_name_1.ds_id=US_SMS_1.source_id) where adaptation_id='SMS_ADAP' and adaptation_version='1' "
						+ "and source_enabled='YES' and ds_version!='OVERALL' group by ds_id,ds_name,ds_weight"))
				.thenReturn(result);
		List<Object[]> sourceList = new ArrayList<Object[]>();
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(
				"select source, adaptation_id, adaptation_version from rithomas.data_source_mapping where adaptation_id='SMS_ADAP' and"
						+ " adaptation_version='1'",
				context)).thenReturn(sourceList);
		Mockito.when(statement.executeQuery(
				"select ds_weight from es_data_source_name_1 where adaptation_id='SMS_ADAP' and adaptation_version='1' "
						+ "and ds_version='OVERALL'"))
				.thenReturn(result1);
		Mockito.when(result1.next()).thenReturn(true, false);
		Mockito.when(result1.getLong(1)).thenReturn(5L);
		List<String[]> kpiNamesList = new ArrayList<String[]>();
		kpiNamesList.add(new String[] { "srcKPI1" });
		kpiNamesList.add(new String[] { "srcKPI2" });
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(
				"select kpi_name from perf_spec_kpi_list where job_name='Perf_SMS_1_HOUR_AggregateJob'",
				context)).thenReturn(kpiNamesList);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(connection);
		calculator.calculateDqi(context, 100L, 200L);
		System.out
				.println(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testCalculateDQIUsageMultipleSource()
			throws ClassNotFoundException, WorkFlowExecutionException,
			SQLException, JobExecutionException, ETLException {
		context.setProperty(JobExecutionContext.ADAPTATION_ID, "SGSN");
		context.setProperty(JobExecutionContext.ADAPTATION_VERSION, "1");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE,
				JobTypeDictionary.USAGE_JOB_TYPE);
		context.setProperty(JobExecutionContext.SOURCE,
				"US_GN_1,US_LTE_4G_1,US_SGSN_1");
		List<String> sourceJobs = new ArrayList<String>();
		sourceJobs.add("Usage_GN_1_LoadJob");
		sourceJobs.add("Usage_LTE_4G_1_LoadJob");
		sourceJobs.add("Usage_SGSN_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobs);
		context.setProperty(
				"Usage_GN_1_LoadJob_" + JobExecutionContext.PARTITION_COLUMN,
				"report_time");
		context.setProperty("Usage_LTE_4G_1_LoadJob_"
				+ JobExecutionContext.PARTITION_COLUMN, "report_time");
		context.setProperty(
				"Usage_SGSN_1_LoadJob_" + JobExecutionContext.PARTITION_COLUMN,
				"event_time");
		context.setProperty(
				"Usage_GN_1_LoadJob_" + JobExecutionContext.ADAPTATION_ID,
				"GN");
		context.setProperty(
				"Usage_GN_1_LoadJob_" + JobExecutionContext.ADAPTATION_VERSION,
				"1");
		context.setProperty(
				"Usage_LTE_4G_1_LoadJob_" + JobExecutionContext.ADAPTATION_ID,
				"LTE");
		context.setProperty("Usage_LTE_4G_1_LoadJob_"
				+ JobExecutionContext.ADAPTATION_VERSION, "1");
		context.setProperty(
				"Usage_SGSN_1_LoadJob_" + JobExecutionContext.ADAPTATION_ID,
				"SGSN");
		context.setProperty("Usage_SGSN_1_LoadJob_"
				+ JobExecutionContext.ADAPTATION_VERSION, "1");
		context.setProperty(JobExecutionContext.DATA_SOURCE_COLUMN,
				"source_id");
		// 1st cycle..
		Object[] sourceTableNameObjArrGN = new String[] { "US_GN_1" };
		Mockito.when(queryExec.executeMetadatasqlQuery(
				"select table_name from job_list where jobid ='Usage_GN_1_LoadJob'",
				context)).thenReturn(sourceTableNameObjArrGN);
		Mockito.when(statement.executeQuery(
				"select ds_id,ds_name,ds_weight,min(report_time),max(report_time) from es_data_source_name_1 left outer join "
						+ "(select unix_timestamp(report_time) report_time,source_id from US_GN_1 where dt>='100' and dt<'200') US_GN_1"
						+ " on(es_data_source_name_1.ds_id=US_GN_1.source_id) where adaptation_id='GN' and adaptation_version='1'"
						+ " and source_enabled='YES' and ds_version!='OVERALL' group by ds_id,ds_name,ds_weight"))
				.thenReturn(result);
		Mockito.when(result.next()).thenReturn(true, false);
		Mockito.when(result.getLong(4)).thenReturn(10L);
		Mockito.when(result.getLong(5)).thenReturn(12L);
		Mockito.when(result.getString(2)).thenReturn("source_name1");
		Mockito.when(result.getInt(3)).thenReturn(6);
		List<Object[]> sourceList = new ArrayList<Object[]>();
		sourceList.add(new String[] { "GN", "2G_3G" });
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(
				"select source, type, adaptation_id, adaptation_version from rithomas.data_source_mapping where "
						+ "adaptation_id='GN' and adaptation_version='1'",
				context)).thenReturn(sourceList);
		Mockito.when(statement.executeQuery(
				"select available from es_sgsn_available_data_1 where source='GN' and type='2G_3G'"))
				.thenReturn(result1);
		Mockito.when(result1.next()).thenReturn(true, false);
		Mockito.when(result1.getString(1)).thenReturn("YES");
		Mockito.when(statement.executeQuery(
				"select ds_weight from es_data_source_name_1 where adaptation_id='GN'"
						+ " and adaptation_version='1' and ds_version='OVERALL'"))
				.thenReturn(result2);
		Mockito.when(result2.next()).thenReturn(true, false);
		Mockito.when(result2.getLong(1)).thenReturn(5L);
		// 2nd cycle..
		Object[] sourceTableNameObjArrLTE = new String[] { "US_LTE_4G_1" };
		Mockito.when(queryExec.executeMetadatasqlQuery(
				"select table_name from job_list where jobid ='Usage_LTE_4G_1_LoadJob'",
				context)).thenReturn(sourceTableNameObjArrLTE);
		Mockito.when(statement.executeQuery(
				"select ds_id,ds_name,ds_weight,min(report_time),max(report_time) from es_data_source_name_1 left outer join "
						+ "(select report_time report_time,source_id from US_LTE_4G_1 where dt>='100' and dt<'200') US_LTE_4G_1 "
						+ "on(es_data_source_name_1.ds_id=US_LTE_4G_1.source_id) where adaptation_id='LTE' and adaptation_version='1' "
						+ "and source_enabled='YES' and ds_version!='OVERALL' group by ds_id,ds_name,ds_weight"))
				.thenReturn(result3);
		Mockito.when(result3.next()).thenReturn(true, false);
		Mockito.when(result3.getLong(4)).thenReturn(10L);
		Mockito.when(result3.getLong(5)).thenReturn(12L);
		Mockito.when(result3.getString(2)).thenReturn("source_name2");
		Mockito.when(result3.getInt(3)).thenReturn(6);
		sourceList = new ArrayList<Object[]>();
		sourceList.add(new String[] { "LTE" });
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(
				"select source, adaptation_id, adaptation_version from rithomas.data_source_mapping where "
						+ "adaptation_id='LTE' and adaptation_version='1'",
				context)).thenReturn(sourceList);
		Mockito.when(statement.executeQuery(
				"select available from es_sgsn_available_data_1 where source='LTE'"))
				.thenReturn(result4);
		Mockito.when(result4.next()).thenReturn(true, false);
		Mockito.when(result4.getString(1)).thenReturn("YES");
		Mockito.when(statement.executeQuery(
				"select ds_weight from es_data_source_name_1 where adaptation_id='LTE'"
						+ " and adaptation_version='1' and ds_version='OVERALL'"))
				.thenReturn(result5);
		Mockito.when(result5.next()).thenReturn(true, false);
		Mockito.when(result5.getLong(1)).thenReturn(5L);
		// 3rd cycle..
		Object[] sourceTableNameObjArrSGSN = new String[] { "US_SGSN_1" };
		Mockito.when(queryExec.executeMetadatasqlQuery(
				"select table_name from job_list where jobid ='Usage_SGSN_1_LoadJob'",
				context)).thenReturn(sourceTableNameObjArrSGSN);
		Mockito.when(statement.executeQuery(
				"select ds_id,ds_name,ds_weight,min(report_time),max(report_time) from es_data_source_name_1 left outer join "
						+ "(select unix_timestamp(event_time) report_time,source_id from US_SGSN_1 where dt>='100' and dt<'200') US_SGSN_1 "
						+ "on(es_data_source_name_1.ds_id=US_SGSN_1.source_id) where adaptation_id='SGSN' and adaptation_version='1' and "
						+ "source_enabled='YES' and ds_version!='OVERALL' group by ds_id,ds_name,ds_weight"))
				.thenReturn(result6);
		Mockito.when(result6.next()).thenReturn(true, false);
		Mockito.when(result6.getLong(4)).thenReturn(10L);
		Mockito.when(result6.getLong(5)).thenReturn(12L);
		Mockito.when(result6.getString(2)).thenReturn("source_name3");
		Mockito.when(result6.getInt(3)).thenReturn(6);
		sourceList = new ArrayList<Object[]>();
		sourceList.add(new String[] { "SGSN" });
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(
				"select source, adaptation_id, adaptation_version from rithomas.data_source_mapping where "
						+ "adaptation_id='SGSN' and adaptation_version='1'",
				context)).thenReturn(sourceList);
		Mockito.when(statement.executeQuery(
				"select available from es_sgsn_available_data_1 where source='SGSN'"))
				.thenReturn(result7);
		Mockito.when(result7.next()).thenReturn(true, false);
		Mockito.when(result7.getString(1)).thenReturn("YES");
		Mockito.when(statement.executeQuery(
				"select ds_weight from es_data_source_name_1 where adaptation_id='SGSN'"
						+ " and adaptation_version='1' and ds_version='OVERALL'"))
				.thenReturn(result8);
		Mockito.when(result8.next()).thenReturn(true, false);
		Mockito.when(result8.getLong(1)).thenReturn(5L);
		calculator.calculateDqi(context, 100L, 200L);
		System.out
				.println(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testCalculateDqiAggSingleSource() throws Exception {
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE,
				JobTypeDictionary.PERF_JOB_TYPE);
		context.setProperty(JobExecutionContext.SOURCE, "PS_SMS_1_15MIN");
		List<String> sourceJobs = new ArrayList<String>();
		sourceJobs.add("Perf_SMS_1_15MIN_ReaggregateJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobs);
		Object[] sourceTableNameObj = new String[] { "PS_SMS_1_15MIN" };
		String srcTableNameQuery = "select table_name from job_list where jobid ='Perf_SMS_1_15MIN_ReaggregateJob'";
		Mockito.when(
				queryExec.executeMetadatasqlQuery(srcTableNameQuery, context))
				.thenReturn(sourceTableNameObj);
		Mockito.when(statement.executeQuery(
				"select dqi,region_id from di_SMS_1_15MIN where dt>='100' and dt<'200'"))
				.thenReturn(result);
		Mockito.when(result.next()).thenReturn(true, false);
		Mockito.when(result.getDouble(1)).thenReturn(6.0);
		Mockito.when(result.getString(2)).thenReturn("Desc1");
		calculator.calculateDqi(context, 100L, 200L);
		System.out
				.println(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testCalculateDqiAggMultipleSource() throws Exception {
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE,
				JobTypeDictionary.PERF_REAGG_JOB_TYPE);
		context.setProperty(JobExecutionContext.SOURCE,
				"PS_CEI_VOICE_1_HOUR,PS_CEI_DATA_1_HOUR");
		List<String> sourceJobs = new ArrayList<String>();
		sourceJobs.add("Perf_CEI_VOICE_1_HOUR_ReaggregateJob");
		sourceJobs.add("Perf_CEI_DATA_1_HOUR_ReaggregateJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobs);
		Object[] sourceTableNameObj1 = new String[] { "PS_CEI_VOICE_1_HOUR" };
		Object[] sourceTableNameObj2 = new String[] { "PS_CEI_DATA_1_HOUR" };
		String srcTableNameQuery1 = "select table_name from job_list where jobid ='Perf_CEI_VOICE_1_HOUR_ReaggregateJob'";
		String srcTableNameQuery2 = "select table_name from job_list where jobid ='Perf_CEI_DATA_1_HOUR_ReaggregateJob'";
		Mockito.when(
				queryExec.executeMetadatasqlQuery(srcTableNameQuery1, context))
				.thenReturn(sourceTableNameObj1);
		Mockito.when(
				queryExec.executeMetadatasqlQuery(srcTableNameQuery2, context))
				.thenReturn(sourceTableNameObj2);
		Mockito.when(statement.executeQuery(
				"select dqi,region_id from di_CEI_VOICE_1_HOUR,PS_CEI_DATA_1_HOUR "
						+ "where dt>='100' and dt<'200'"))
				.thenReturn(result);
		Mockito.when(result.next()).thenReturn(true, false);
		Mockito.when(result.getDouble(1)).thenReturn(6.0);
		Mockito.when(result.getString(2)).thenReturn("Desc1");
		calculator.calculateDqi(context, 100L, 200L);
		System.out
				.println(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testCalculateDQIAggMultipleSourceWithSourceList()
			throws SQLException, ClassNotFoundException,
			WorkFlowExecutionException, JobExecutionException, ETLException {
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE,
				JobTypeDictionary.PERF_REAGG_JOB_TYPE);
		context.setProperty(JobExecutionContext.SOURCE,
				"PS_CEI_VOICE_1_HOUR,PS_CEI_DATA_1_HOUR,ES_LOCATION_1,ES_HOME_COUNTRY_LOOKUP_1");
		List<String> sourceJobs = new ArrayList<String>();
		sourceJobs.add("Perf_CEI_VOICE_1_HOUR_ReaggregateJob");
		sourceJobs.add("Perf_CEI_DATA_1_HOUR_ReaggregateJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobs);
		Object[] sourceTableNameObj1 = new String[] { "PS_CEI_VOICE_1_HOUR" };
		Object[] sourceTableNameObj2 = new String[] { "PS_CEI_DATA_1_HOUR" };
		String srcTableNameQuery1 = "select table_name from job_list where jobid ='Perf_CEI_VOICE_1_HOUR_ReaggregateJob'";
		String srcTableNameQuery2 = "select table_name from job_list where jobid ='Perf_CEI_DATA_1_HOUR_ReaggregateJob'";
		Mockito.when(
				queryExec.executeMetadatasqlQuery(srcTableNameQuery1, context))
				.thenReturn(sourceTableNameObj1);
		Mockito.when(
				queryExec.executeMetadatasqlQuery(srcTableNameQuery2, context))
				.thenReturn(sourceTableNameObj2);
		// 1st cycle..
		Mockito.when(statement.executeQuery(
				"select dqi,region_id from di_SMS_1_15MIN where dt>='100' and dt<'200'"))
				.thenReturn(result);
		Mockito.when(result.next()).thenReturn(true, false);
		Mockito.when(result.getDouble(1)).thenReturn(6.0);
		Mockito.when(result.getString(2)).thenReturn("Desc1");
		List<Object[]> sourceList = new ArrayList<Object[]>();
		sourceList.add(new String[] { "PS_SMS_1_15MIN", "SMS" });
		sourceList.add(new String[] { "ES_LOCATION_1", "COMMON_DIMENSION" });
		sourceList.add(new String[] { "ES_HOME_COUNTRY_LOOKUP_1",
				"COMMON_DIMENSION" });
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(
				"select table_name, jd.adaptationid, jd.adaptationversion from rithomas.job_list jl, rithomas.boundary b, "
						+ "rithomas.job_dictionary jd, rithomas.job_dict_dependency jdd  where b.jobid='Perf_SMS_1_HOUR_AggregateJob'"
						+ " and b.id=jdd.bound_jobid and jdd.source_jobid=jl.id and jl.jobid=jd.jobid",
				context)).thenReturn(sourceList);
		Mockito.when(statement.executeQuery(
				"select ds_weight from es_data_source_name_1 where source_enabled = 'YES' and adaptation_id='SMS' "
						+ "and adaptation_version='1' and ds_name='SMS' and ds_version!='OVERALL'"))
				.thenReturn(result1);
		Mockito.when(result1.next()).thenReturn(true, false);
		Mockito.when(result1.getInt(1)).thenReturn(3);
		// 2nd cycle..
		Mockito.when(statement.executeQuery(
				"select dqi,region_id from di_LOCATION_1 where dt>='100' and dt<'200'"))
				.thenReturn(result);
		Mockito.when(result.next()).thenReturn(true, false);
		Mockito.when(result.getDouble(1)).thenReturn(5.0);
		Mockito.when(result.getString(2)).thenReturn("Desc2");
		sourceList = new ArrayList<Object[]>();
		sourceList.add(new String[] { "PS_SMS_1_15MIN", "SMS" });
		sourceList.add(new String[] { "ES_LOCATION_1", "COMMON_DIMENSION" });
		sourceList.add(new String[] { "ES_HOME_COUNTRY_LOOKUP_1",
				"COMMON_DIMENSION" });
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(
				"select table_name, jd.adaptationid, jd.adaptationversion from rithomas.job_list jl, rithomas.boundary b, "
						+ "rithomas.job_dictionary jd, rithomas.job_dict_dependency jdd  where b.jobid='Perf_SMS_1_HOUR_AggregateJob'"
						+ " and b.id=jdd.bound_jobid and jdd.source_jobid=jl.id and jl.jobid=jd.jobid",
				context)).thenReturn(sourceList);
		Mockito.when(statement.executeQuery(
				"select ds_weight from es_data_source_name_1 where source_enabled = 'YES' and adaptation_id='SMS'"
						+ " and adaptation_version='1' and ds_name='COMMON_DIMENSION' and ds_version!='OVERALL'"))
				.thenReturn(result1);
		Mockito.when(result1.next()).thenReturn(true, false);
		Mockito.when(result1.getInt(1)).thenReturn(3);
		// 3rd cycle..
		Mockito.when(statement.executeQuery(
				"select dqi,region_id from di_HOME_COUNTRY_LOOKUP_1 where dt>='100' and dt<'200'"))
				.thenReturn(result);
		Mockito.when(result.next()).thenReturn(true, false);
		Mockito.when(result.getDouble(1)).thenReturn(4.0);
		Mockito.when(result.getString(2)).thenReturn("Desc3");
		sourceList = new ArrayList<Object[]>();
		sourceList.add(new String[] { "PS_SMS_1_15MIN", "SMS" });
		sourceList.add(new String[] { "ES_LOCATION_1", "COMMON_DIMENSION" });
		sourceList.add(new String[] { "ES_HOME_COUNTRY_LOOKUP_1",
				"COMMON_DIMENSION" });
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(
				"select table_name, jd.adaptationid, jd.adaptationversion from rithomas.job_list jl, rithomas.boundary b, "
						+ "rithomas.job_dictionary jd, rithomas.job_dict_dependency jdd  where b.jobid='Perf_SMS_1_HOUR_AggregateJob'"
						+ " and b.id=jdd.bound_jobid and jdd.source_jobid=jl.id and jl.jobid=jd.jobid",
				context)).thenReturn(sourceList);
		Mockito.when(statement.executeQuery(
				"select ds_weight from es_data_source_name_1 where source_enabled = 'YES' and adaptation_id='SMS'"
						+ " and adaptation_version='1' and ds_name='COMMON_DIMENSION' and ds_version!='OVERALL'"))
				.thenReturn(result1);
		Mockito.when(result1.next()).thenReturn(true, false);
		Mockito.when(result1.getInt(1)).thenReturn(3);
		calculator.calculateDqi(context, 100L, 200L);
		System.out
				.println(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testCalculateDqiReaggInvalid() throws Exception {
		context.setProperty(JobExecutionContext.REAGG_LAST_LB,
				"2014-01-12 00:00:00");
		calculator.calculateDqi(context, 100L, 200L);
	}
}
