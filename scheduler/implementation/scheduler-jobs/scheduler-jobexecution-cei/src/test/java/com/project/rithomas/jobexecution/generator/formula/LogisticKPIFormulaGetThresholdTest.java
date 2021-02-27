
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.apache.commons.lang.StringUtils;
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
@PrepareForTest(value = { LogisticKPIFormulaGetCont.class, ModelResources.class,
		AbstractUserDefinedKPIFormula.class })
public class LogisticKPIFormulaGetThresholdTest {

	private LogisticKPIFormulaGetThreshold formula;

	private String template;

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
		formula = new LogisticKPIFormulaGetThreshold();
		Assert.assertEquals(StringUtils.normalizeSpace(expectedResult),
				StringUtils.normalizeSpace(actualResult));
	}

	@Test
	public void testGetFormulaToCalculateForGreaterTheBetter()
			throws Exception {
		properties.add(logisticFormulaGeneratorTest
				.setProperties("MOS_NB_AVG$KPITYPE", "GREATER_THE_BETTER"));
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"MOS_NB_AVG"));
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("VOICE_CS_CEI_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("VOICE_CS_CEI_INDEX"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("VOICE_CS_CEI_INDEX"));
