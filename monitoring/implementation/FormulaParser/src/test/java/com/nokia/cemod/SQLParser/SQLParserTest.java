
package com.nokia.cemod.SQLParser;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class SQLParserTest {

	SQLParser sqlParser;

	@Before
	public void setUp() {
		sqlParser = new SQLParser();
	}

	@Test
	public void testDenormFormula() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "denorm:lookup(SUBSCRIBER_DENORM-1,MSISDN,concat({\"SUBSCRIBER_DENORM,1,\",decode({IMSI,\"null\",-1,IMSI})}))";
		List<String> expectedList = Arrays.asList("msisdn", "imsi");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testMsisdnFormula() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "msisdn_normalize(upper(IMSI), upper(MSISDN),({PARAM_VALUE from HOME_COUNTRY_LOOKUP-1 where PARAM_NAME  \"HOME_COUNTRY_CODE\" }),({PARAM_VAL\r\n"
				+ "UE from HOME_COUNTRY_LOOKUP-1 where PARAM_NAME \"MSISDN_MAX_LEN_SUPPORTED\"}),\"N\")\r\n"
				+ "";
		List<String> expectedList = Arrays.asList("imsi", "msisdn");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testDenormFormulaWithMultipleFunctions()
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException,
			SecurityException {
		String formula = "denorm:lookup(APPLICATION_GROUP_DENORM-1,APP_CATEGORY_TYPE,concat({\"APPLICATION_GROUP_DENORM,1,\",decode({APPLICATION_ID,\"\",-1,APPLICATION_ID})}))";
		List<String> expectedList = Arrays.asList("app_category_type",
				"application_id");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testNvlfunction() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "nvl(HANDSET_FULL_NAME,'Unknown')";
		List<String> expectedList = Arrays.asList("handset_full_name");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testFunctionWithOperators() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "count(distinct (case when (cemod.aggregate1.delivery_time/cemod.aggregate1.total_success_sms) &gt; cemod.aggregate1.delivery_time_threshold then cemod.aggregate1.imsi_id else NULL end))";
		List<String> expectedList = Arrays.asList("delivery_time",
				"total_success_sms", "delivery_time_threshold", "imsi_id");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testRoundFunction() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "round((nvl({Measure\\Affected Subscribers Delayed SMS}/{Measure\\Active Subscribers Success SMS},0)*100),2)";
		List<String> expectedList = Arrays.asList(
				"affected_subscribers_delayed_sms",
				"active_subscribers_success_sms");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "concat({Location\\Location Name},\'[\',{Location\\City},\'/\',{Location\\Region},\']\')";
		List<String> expectedList = Arrays.asList("location_name", "city",
				"region");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula1() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "count(distinct {Location\\Loc ID})";
		List<String> expectedList = Arrays.asList("loc_id");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula2() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "count(distinct(decode(cemod.aggregate1.number_of_failures,0,NULL,{Location\\Cell Sac ID with LAC MCC MNC})))";
		List<String> expectedList = Arrays.asList("number_of_failures",
				"cell_sac_id_with_lac_mcc_mnc");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula3() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "count(distinct (case when cemod.aggregate1.number_of_sms>0 then cemod.aggregate1.imsi_id else NULL end))";
		List<String> expectedList = Arrays.asList("number_of_sms", "imsi_id");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula4() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "((floor((( {CEI2_VOBB_PHONE\\VOBB_MOS_DS_AVG} ))/(@Prompt('Enter the Size of Sessions Downgraded Bin','N',,Mono,Free)))+1) * (@Prompt('Enter the Size of Sessions Downgraded Bin','N',,Mono,Free)))";
		List<String> expectedList = Arrays.asList("vobb_mos_ds_avg");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula5() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "count(distinct(case when cemod.ps_radio_fail_1.rab_setup_failures&gt;0 then {Subscriber\\IMSI} end))";
		List<String> expectedList = Arrays.asList("rab_setup_failures", "imsi");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula6() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "cemod.es_fl_equipment_denorm_1.user_id";
		List<String> expectedList = Arrays.asList("user_id");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula7() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "round(nvl((({Measures\\Signal Quality Fair}+{Measures\\Signal Quality Good}+{Measures\\Signal Quality Excellent})/{Measures\\Signal Quality})*100,0),2)";
		List<String> expectedList = Arrays.asList("signal_quality_fair",
				"signal_quality_good", "signal_quality_excellent",
				"signal_quality");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula8() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "round(nvl(({Measures\\Inter-RAT 4G-2G Handover Failures}/{Measures\\Inter-RAT 4G-2G Handover Attempts})*100,0),2)";
		List<String> expectedList = Arrays.asList(
				"inter_rat_4g_2g_handover_failures",
				"inter_rat_4g_2g_handover_attempts");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula9() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "count(distinct(case when {Measures\\Failures Count}&gt;0 then {Location\\CGI ID} end))";
		List<String> expectedList = Arrays.asList("failures_count", "cgi_id");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula10() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "round(100 - weight_average(array({Measures\\RRC Setup Failures Percentage},{Measures\\RAB Setup Failures Percentage},{Measures\\RAB Active Failures Percentage},{Measures\\Handover Failures Percentage}),array('RRC Setup Fail','RAB Setup Fail','RAB Active Fail','Handover Fail'),'RADIO'),2)";
		List<String> expectedList = Arrays.asList(
				"rrc_setup_failures_percentage",
				"rab_setup_failures_percentage",
				"rab_active_failures_percentage",
				"handover_failures_percentage");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula11() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "round((((({Measures\\Call Drop Answered})/({Measures\\Call Answered})*100) -#CallDropFailures#) * ({Measures\\Call Drop Answered Fail Devices} * (({Measures\\Call Drop Answered})/({Measures\\Call Drop Answered Fail Devices}))))/(decode({Measures\\Call Drop Answered Devices},{Measures\\Call Drop Answered Fail Devices},1,((({Measures\\Call Drop Answered Devices}-{Measures\\Call Drop Answered Fail Devices})/{Measures\\Call Drop Answered Devices})*100))),4)";
		List<String> expectedList = Arrays.asList("call_drop_answered",
				"call_answered", "calldropfailures",
				"call_drop_answered_fail_devices",
				"call_drop_answered_devices");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}

	@Test
	public void testUNIFormula12() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "round(100-({Measures\\Stable Stability Percentage}+{Measures\\Unstable Stability Percentage}),2)";
		List<String> expectedList = Arrays.asList("stable_stability_percentage",
				"unstable_stability_percentage");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}
	
	@Test
	public void testUNIFormula13() throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		String formula = "SUM(CASE WHEN (CONTENT_TYPE = ''AUDIO'' OR CONTENT_TYPE = ''VIDEO'') THEN TIME_DURATION ELSE 0 END)";
		List<String> expectedList = Arrays.asList("content_type", "time_duration");
		assertEquals(expectedList, sqlParser.parserFormula(formula));
	}
}
