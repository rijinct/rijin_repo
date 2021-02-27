
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.ModelResources;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { ModelResources.class, AbstractUserDefinedKPIFormula.class })
public class LogisticKPIFormulaGetHigherUsageBasedFormulaTest {

	private LogisticKPIFormulaGetHigherUsageBasedFormula formula;

	private List<PerfIndiSpecFormulaProperty> properties;

	@Mock
	private StringTemplateGroup mockedStringTemplateGroup;

	@Mock
	private EntityManager mockedEntityManager;

	@SuppressWarnings("rawtypes")
	@Mock
	private TypedQuery mockedFormulaQuery, mockedPropertyQuery;

	LogisticFormulaGeneratorTest logisticFormulaGeneratorTest = new LogisticFormulaGeneratorTest();

	StringTemplate st = new StringTemplate("CASE $USAGE_OBJ:{ obj | "
			+ "$if(obj.singleKPI)$ WHEN $first(obj.usageServiceWeightMap.keys)$ > 0 THEN $first(obj.usageServiceWeightMap.keys)$ $elseif(obj.twoKPIs)$ "
			+ "WHEN ($first(obj.usageServiceWeightMap.keys)$ > 0 AND $last(obj.usageServiceWeightMap.keys)$ > 0) "
			+ "THEN ($first(obj.usageServiceWeightMap.keys)$ * $first(obj.usageServiceWeightMap.values)$ +"
			+ " $last(obj.usageServiceWeightMap.keys)$ * $last(obj.usageServiceWeightMap.values)$)/($obj.usageServiceWeightMap.values;separator=\" + \"$) $else$ "
			+ "WHEN ($trunc(obj.usageServiceWeightMap.keys);separator=\" > 0 AND \"$ > 0 AND $last(obj.usageServiceWeightMap.keys)$ > 0) THEN "
			+ "($obj.usageServiceWeightMap.keys:{ k | $k$ * $obj.usageServiceWeightMap.(k)$};separator=\" + \"$)/($obj.usageServiceWeightMap.values;separator=\" + \"$) $endif$ }$ "
			+ "ELSE NULL END");

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		MockitoAnnotations.initMocks(logisticFormulaGeneratorTest);
		formula = new LogisticKPIFormulaGetHigherUsageBasedFormula();
		properties = new ArrayList<PerfIndiSpecFormulaProperty>();
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("VOICE_CEI_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("VOICE_CEI_INDEX"));
		PowerMockito.whenNew(StringTemplateGroup.class)
				.withArguments(Mockito.anyString(), Mockito.anyString())
				.thenReturn(mockedStringTemplateGroup);
		PowerMockito
				.when(mockedStringTemplateGroup.getInstanceOf(
						"logisticKPIFormulaGetHigherUsageWeight"))
				.thenReturn(st);
	}

	@Test
	public void testGetFormulaToCalculate() throws Exception {
		setPropertiesForThreeSrc();
		properties.add(logisticFormulaGeneratorTest
				.setProperties("USAGE_ENABLE", "yes"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("VOICE_CEI_INDEX"));
		String expectedResult = "CASE  WHEN (VOICE_CEI_INDEX_WEIGHT > 0 AND SMS_CEI_INDEX_WEIGHT > 0 AND DATA_CEI_INDEX_WEIGHT > 0) "
				+ "THEN (VOICE_CEI_INDEX_WEIGHT * (CASE WHEN VOICE_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "SMS_CEI_INDEX_WEIGHT * (CASE WHEN SMS_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "DATA_CEI_INDEX_WEIGHT * (CASE WHEN DATA_CEI_INDEX IS NULL THEN 0 ELSE 1 END))/((CASE WHEN VOICE_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "(CASE WHEN SMS_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + (CASE WHEN DATA_CEI_INDEX IS NULL THEN 0 ELSE 1 END))  "
				+ " WHEN (VOICE_CEI_INDEX_WEIGHT > 0 AND SMS_CEI_INDEX_WEIGHT > 0) THEN "
				+ "(VOICE_CEI_INDEX_WEIGHT * (CASE WHEN VOICE_CEI_INDEX IS NULL THEN 0 ELSE 1 END) +"
				+ " SMS_CEI_INDEX_WEIGHT * (CASE WHEN SMS_CEI_INDEX IS NULL THEN 0 ELSE 1 END))/((CASE WHEN VOICE_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "(CASE WHEN SMS_CEI_INDEX IS NULL THEN 0 ELSE 1 END))   WHEN (VOICE_CEI_INDEX_WEIGHT > 0 AND DATA_CEI_INDEX_WEIGHT > 0) "
				+ "THEN (VOICE_CEI_INDEX_WEIGHT * (CASE WHEN VOICE_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "DATA_CEI_INDEX_WEIGHT * (CASE WHEN DATA_CEI_INDEX IS NULL THEN 0 ELSE 1 END))/((CASE WHEN VOICE_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "(CASE WHEN DATA_CEI_INDEX IS NULL THEN 0 ELSE 1 END))   WHEN (SMS_CEI_INDEX_WEIGHT > 0 AND DATA_CEI_INDEX_WEIGHT > 0) "
				+ "THEN (SMS_CEI_INDEX_WEIGHT * (CASE WHEN SMS_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "DATA_CEI_INDEX_WEIGHT * (CASE WHEN DATA_CEI_INDEX IS NULL THEN 0 ELSE 1 END))/((CASE WHEN SMS_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "(CASE WHEN DATA_CEI_INDEX IS NULL THEN 0 ELSE 1 END))   WHEN VOICE_CEI_INDEX_WEIGHT > 0 THEN VOICE_CEI_INDEX_WEIGHT   "
				+ "WHEN SMS_CEI_INDEX_WEIGHT > 0 THEN SMS_CEI_INDEX_WEIGHT   WHEN DATA_CEI_INDEX_WEIGHT > 0 THEN DATA_CEI_INDEX_WEIGHT   ELSE NULL END";
		Assert.assertEquals(expectedResult, actualResult);
	}

	private void setPropertiesForThreeSrc() {
		setPropertiesForTwoSrc();
		setPropertiesForSingleSrc();
	}

	@Test
	public void testGetFormulaToCalculateTwoSrcs() throws Exception {
		setPropertiesForTwoSrc();
		properties.add(logisticFormulaGeneratorTest
				.setProperties("USAGE_ENABLE", "yes"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("VOICE_CEI_INDEX"));
		String expectedResult = "CASE  WHEN (VOICE_CEI_INDEX_WEIGHT > 0 AND SMS_CEI_INDEX_WEIGHT > 0) THEN "
				+ "(VOICE_CEI_INDEX_WEIGHT * (CASE WHEN VOICE_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "SMS_CEI_INDEX_WEIGHT * (CASE WHEN SMS_CEI_INDEX IS NULL THEN 0 ELSE 1 END))/((CASE WHEN VOICE_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "(CASE WHEN SMS_CEI_INDEX IS NULL THEN 0 ELSE 1 END))   WHEN VOICE_CEI_INDEX_WEIGHT > 0 THEN VOICE_CEI_INDEX_WEIGHT   "
				+ "WHEN SMS_CEI_INDEX_WEIGHT > 0 THEN SMS_CEI_INDEX_WEIGHT   ELSE NULL END";
		Assert.assertEquals(expectedResult, actualResult);
	}

	private void setPropertiesForTwoSrc() {
		properties.add(logisticFormulaGeneratorTest
				.setProperties("VOICE_CEI_INDEX$WEIGHT", "1"));
		properties.add(logisticFormulaGeneratorTest
				.setProperties("SMS_CEI_INDEX$WEIGHT", "1"));
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"VOICE_CEI_INDEX"));
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"SMS_CEI_INDEX"));
		properties.add(logisticFormulaGeneratorTest
				.setProperties("VOICE_CEI_INDEX$SRC_USAGE_ENABLE", "true"));
		properties.add(logisticFormulaGeneratorTest
				.setProperties("SMS_CEI_INDEX$SRC_USAGE_ENABLE", "true"));
	}

	@Test
	public void testGetFormulaToCalculateSingleSrc() throws Exception {
		setPropertiesForSingleSrc();
		properties.add(logisticFormulaGeneratorTest
				.setProperties("USAGE_ENABLE", "yes"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("VOICE_CEI_INDEX"));
		String expectedResult = "CASE  WHEN DATA_CEI_INDEX_WEIGHT > 0 THEN DATA_CEI_INDEX_WEIGHT   ELSE NULL END";
		Assert.assertEquals(expectedResult, actualResult);
	}

	private void setPropertiesForSingleSrc() {
		properties.add(logisticFormulaGeneratorTest
				.setProperties("DATA_CEI_INDEX$WEIGHT", "1"));
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"DATA_CEI_INDEX"));
		properties.add(logisticFormulaGeneratorTest
				.setProperties("DATA_CEI_INDEX$SRC_USAGE_ENABLE", "true"));
	}

	@Test
	public void testGetFormulaToCalculateWhenUsageBasedIsNo() throws Exception {
		properties.add(logisticFormulaGeneratorTest
				.setProperties("USAGE_ENABLE", "NO"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("VOICE_CEI_INDEX"));
		String expectedResult = "(CASE WHEN TARGET_KPI IS NULL THEN NULL ELSE 1 END)";
		Assert.assertEquals(expectedResult, actualResult);
	}

	@Test
	public void testGetFormulaToCalculateWithUsageEnabledWithSingleSrc()
			throws Exception {
		setPropertiesForTwoSrc();
		properties.add(logisticFormulaGeneratorTest
				.setProperties("SMS_CEI_INDEX$SRC_USAGE_ENABLE", "false"));
		properties.add(logisticFormulaGeneratorTest
				.setProperties("USAGE_ENABLE", "yes"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("VOICE_CEI_INDEX"));
		String expectedResult = "CASE  WHEN (VOICE_CEI_INDEX_WEIGHT > 0 AND 1 > 0) THEN "
				+ "(VOICE_CEI_INDEX_WEIGHT * (CASE WHEN VOICE_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "1 * (CASE WHEN SMS_CEI_INDEX IS NULL THEN 0 ELSE 1 END))/((CASE WHEN VOICE_CEI_INDEX IS NULL THEN 0 ELSE 1 END) + "
				+ "(CASE WHEN SMS_CEI_INDEX IS NULL THEN 0 ELSE 1 END))   WHEN VOICE_CEI_INDEX_WEIGHT > 0 THEN VOICE_CEI_INDEX_WEIGHT   "
				+ "WHEN 1 > 0 THEN 1   ELSE NULL END";
		Assert.assertEquals(expectedResult, actualResult);
	}
}
