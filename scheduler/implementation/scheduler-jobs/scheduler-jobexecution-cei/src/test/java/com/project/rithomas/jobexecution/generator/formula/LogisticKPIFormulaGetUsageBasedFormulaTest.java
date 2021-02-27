
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.apache.commons.lang3.StringUtils;
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
import org.powermock.reflect.Whitebox;

import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.ModelResources;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { LogisticKPIFormulaGetCont.class, ModelResources.class,
		AbstractUserDefinedKPIFormula.class })
public class LogisticKPIFormulaGetUsageBasedFormulaTest {

	private LogisticKPIFormulaGetUsageBasedFormula formula;

	private String expectedResult;

	private List<PerfIndiSpecFormulaProperty> properties;

	@Mock
	private StringTemplateGroup mockedStringTemplateGroup;

	@Mock
	private EntityManager mockedEntityManager;

	@SuppressWarnings("rawtypes")
	@Mock
	private TypedQuery mockedFormulaQuery, mockedPropertyQuery;

	LogisticFormulaGeneratorTest logisticFormulaGeneratorTest = new LogisticFormulaGeneratorTest();

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		MockitoAnnotations.initMocks(logisticFormulaGeneratorTest);
		formula = new LogisticKPIFormulaGetUsageBasedFormula();
		new HashMap<String, String>();
		properties = new ArrayList<PerfIndiSpecFormulaProperty>();
		properties.add(logisticFormulaGeneratorTest.setProperties("USAGE_KPI",
				"SMS_USAGE"));
		properties.add(logisticFormulaGeneratorTest.setProperties("TARGET_KPI",
				"SMS_CEI_INDEX"));
		properties.add(logisticFormulaGeneratorTest
				.setProperties("USAGE_FORMULA", "(USAGE_KPI/AVG_USAGE)"));
		properties.add(logisticFormulaGeneratorTest
				.setProperties("USAGE_DEFAULT_VALUE", "1.0"));
		StringTemplate st = new StringTemplate(
				"CASE WHEN ($USAGE_KPI$) = 0 AND $TARGET_KPI$ IS NOT NULL THEN $DEFAULT_VALUE$ WHEN $TARGET_KPI$ IS NULL THEN NULL ELSE $FORMULA$ END");
		PowerMockito.whenNew(StringTemplateGroup.class)
				.withArguments(Mockito.anyString(), Mockito.anyString())
				.thenReturn(mockedStringTemplateGroup);
		PowerMockito
				.when(mockedStringTemplateGroup
						.getInstanceOf("logisticKPIFormulaGetUsageWeight"))
				.thenReturn(st);
	}

	@Test
	public void testGetFormulaToCalculate() throws Exception {
		properties.add(logisticFormulaGeneratorTest
				.setProperties("USAGE_ENABLE", "YES"));
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("SMS_CEI_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("SMS_CEI_INDEX"));
		Whitebox.setInternalState(formula, "interval", "HOUR");
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("SMS_CEI_INDEX"));
		expectedResult = "CASE WHEN SMS_CEI_INDEX IS NULL THEN NULL WHEN 111 IS NULL THEN 1.0 ELSE (CASE\r\n"
				+ "WHEN ((nvl(RECEIVE_FAILURE,0)+nvl(RECEIVE_SUCCESS,0)+nvl(SEND_FAILURE,0)+nvl(SEND_SUCCESS,0))) = 0 AND SMS_CEI_INDEX IS NOT NULL THEN 1.0\r\n"
				+ "WHEN SMS_CEI_INDEX IS NULL THEN NULL\r\n"
				+ "ELSE ((nvl(RECEIVE_FAILURE,0)+nvl(RECEIVE_SUCCESS,0)+nvl(SEND_FAILURE,0)+nvl(SEND_SUCCESS,0))/111)\r\n"
				+ "END) END";
		Assert.assertEquals(StringUtils.normalizeSpace(expectedResult),
				StringUtils.normalizeSpace(actualResult));
	}

	@Test
	public void testGetFormulaToCalculateWithoutUSAGE_ENABLE()
			throws Exception {
		properties.add(logisticFormulaGeneratorTest
				.setProperties("USAGE_ENABLE", "NO"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("SMS_CEI_INDEX"));
		expectedResult = "1";
		Assert.assertEquals(expectedResult, actualResult);
	}
}
