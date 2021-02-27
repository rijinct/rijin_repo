
package com.rijin.analytics.scheduler.config;

import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.CA4CIHiveConfiguration;
import com.rijin.scheduler.jobexecution.hive.CA4CISparkConfiguration;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;



@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveConfigurationProvider.class, FileReader.class, SDKSystemEnvironment.class,
		Properties.class, HiveConfigurationFactory.class,
		HiveConfigurationFactory.class, CA4CISparkConfiguration.class})
public class CA4CIHiveHintsConfigurationTest {
	
	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CA4CIHiveHintsConfigurationTest.class);

	@Mock
	FileReader fileReader;

	@Mock
	Properties properties;

	@Mock
	HiveConfigurationFactory configuratorFactory;
	
	@Mock
	CA4CIHiveConfiguration hiveConfiguration;
	
	@Mock
	CA4CISparkConfiguration mockCA4CISparkConfiguration;
	

	@Before
	public void setup() throws Exception {
		PowerMockito.mockStatic(SDKSystemEnvironment.class);
		PowerMockito.mockStatic(HiveConfigurationFactory.class);
		PowerMockito.when(HiveConfigurationFactory.getConfiguration())
				.thenReturn(hiveConfiguration);
		PowerMockito.when(HiveConfigurationFactory.getSparkConfiguration())
				.thenReturn(mockCA4CISparkConfiguration);
		
		when(hiveConfiguration.getDbUrl()).thenReturn("jdbc:hive2://localhost:10000/default");
	}

	@Test
	public void getQueryHintsForHiveActualFlowTest() throws Exception {
		String configfile = this.getClass().getClassLoader()
				.getResource("hive_config.properties").getPath();
		configfile = configfile.replace("/D:", "D:");
		configfile = configfile.replace("hive_config.properties", "hive2/");
		if (configfile != null && configfile.contains("%20")) {
			configfile = configfile.replace("%20", " ");
		}
		System.out.println("hive config " + configfile);
		PowerMockito.whenNew(FileInputStream.class);
		
		PowerMockito.mockStatic(SDKSystemEnvironment.class);
		PowerMockito.when(SDKSystemEnvironment.getschedulerConfDirectory())
				.thenReturn(configfile);
		
		HiveConfigurationProvider.getInstance().getPoolConfig();
		List<String> expectedQueryHints = new ArrayList<String>(Arrays.asList(
				"CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode'",
				"CREATE TEMPORARY FUNCTION substr AS 'com.nexr.platform.hive.udf.UDFSubstrForOracle'",
				"CREATE TEMPORARY FUNCTION lpad As 'org.apache.hadoop.hive.ql.udf.UDFLpad'",
				"CREATE TEMPORARY FUNCTION trim As 'org.apache.hadoop.hive.ql.udf.UDFTrim'",
				"use HIVE_SCHEMA", "SET hive.auto.convert.join=Global_Default",
				"SET hive.auto.convert.join=true",
				"SET hive.exec.compress.output=false",
				"SET mapreduce.job.queuename=defaultjobs",
				"SET hive.auto.convert.join=Hive2_Default",
				"SET hive.auto.convert.join=Global_Pattern",
				"SET hive.auto.convert.join=Hive2_Pattern",
				"SET hive.auto.convert.join=Global_Job",
				"SET hive.auto.convert.join=Hive2_Job"));
		List<String> actualQueryHints = HiveConfigurationProvider.getInstance()
				.getQueryHints("Perf_DATA_CP_SEGG_1_HOUR_AggregateJob", "HIVE");
		Assert.assertEquals(expectedQueryHints, actualQueryHints);
	}

	@Test
	public void getQueryHintsForHiveActualAPNFlowTest() throws Exception {
		String configfile = this.getClass().getClassLoader()
				.getResource("hive_config.properties").getPath();
		configfile = configfile.replace("/D:", "D:");
		configfile = configfile.replace("hive_config.properties", "hive2/");
		if (configfile != null && configfile.contains("%20")) {
			configfile = configfile.replace("%20", " ");
		}
		LOGGER.debug("hive config {}", configfile);
		PowerMockito.whenNew(FileInputStream.class);
		PowerMockito.mockStatic(SDKSystemEnvironment.class);
		PowerMockito.when(SDKSystemEnvironment.getschedulerConfDirectory())
				.thenReturn(configfile);
		HiveConfigurationProvider.getInstance().getPoolConfig();
		List<String> expectedQueryHints = new ArrayList<String>(Arrays.asList(
				"CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode'",
				"CREATE TEMPORARY FUNCTION substr AS 'com.nexr.platform.hive.udf.UDFSubstrForOracle'",
				"CREATE TEMPORARY FUNCTION lpad As 'org.apache.hadoop.hive.ql.udf.UDFLpad'",
				"CREATE TEMPORARY FUNCTION trim As 'org.apache.hadoop.hive.ql.udf.UDFTrim'",
				"use HIVE_SCHEMA", "SET hive.auto.convert.join=Global_Default",
				"SET hive.auto.convert.join=true",
				"SET hive.exec.compress.output=false",
				"SET mapreduce.job.queuename=defaultjobs",
				"SET hive.auto.convert.join=Global_Pattern",
				"SET hive.auto.convert.join=Hive2_Pattern",
				"SET hive.auto.convert.join=Hive2_Default"));
		List<String> actualQueryHints = HiveConfigurationProvider.getInstance()
				.getQueryHints("Perf_APNHLR_AC_1_1_HOUR_AggregateJob", "HIVE");
		LOGGER.debug("{}",actualQueryHints);
		Assert.assertEquals(expectedQueryHints, actualQueryHints);
	}
}
