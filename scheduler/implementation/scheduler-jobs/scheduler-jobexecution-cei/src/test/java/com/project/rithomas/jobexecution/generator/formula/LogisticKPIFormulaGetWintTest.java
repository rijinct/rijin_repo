
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
import java.util.HashMap;
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
@PrepareForTest(value = { LogisticKPIFormulaGetWint.class, ModelResources.class,
		AbstractUserDefinedKPIFormula.class })
public class LogisticKPIFormulaGetWintTest {

	private LogisticKPIFormulaGetWint formula;

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
		formula = new LogisticKPIFormulaGetWint();
		template = "(( 1 / $TARGET_KPI_BUCKET$)*$KPI_INT$)*$WEIGHT$";
		StringTemplate st = new StringTemplate(template);
		PowerMockito.whenNew(StringTemplateGroup.class)
				.withArguments(Mockito.anyString(), Mockito.anyString())
				.thenReturn(mockedStringTemplateGroup);
		PowerMockito
				.when(mockedStringTemplateGroup
						.getInstanceOf("logisticKPIFormulaGetWintTemplate"))
				.thenReturn(st);
		new HashMap<String, String>();
		properties = new ArrayList<PerfIndiSpecFormulaProperty>();
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"RECEIVE_FAIL_SUBS_PERC"));
		properties.add(logisticFormulaGeneratorTest
				.setProperties("RECEIVE_FAIL_SUBS_PERC$WEIGHT", "3"));
		properties.add(logisticFormulaGeneratorTest.setProperties("TARGET_KPI",
				"SMS_INDEX"));
		expectedResult = "(( 1 / SMS_INDEX_BUCKET)*RECEIVE_FAIL_SUBS_PERC_INT)*1.0";
	}

	@Test
	public void testGetFormulaToCalculate() throws Exception {
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("SMS_CEI_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("SMS_CEI_INDEX"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("SMS_CEI_INDEX"));
		Assert.assertEquals(expectedResult, actualResult);
	}
}
