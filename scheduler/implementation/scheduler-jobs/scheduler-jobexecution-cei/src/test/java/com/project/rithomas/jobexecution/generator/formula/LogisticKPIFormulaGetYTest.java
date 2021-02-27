
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
@PrepareForTest(value = { LogisticKPIFormulaGetY.class, ModelResources.class,
		AbstractUserDefinedKPIFormula.class })
public class LogisticKPIFormulaGetYTest {

	private LogisticKPIFormulaGetY formula;

	private String template;

	private String expectedResult;

	private Map<String, String> defaultPropMap;

	private List<PerfIndiSpecFormulaProperty> properties;

	LogisticFormulaGeneratorTest logisticFormulaGeneratorTest = new LogisticFormulaGeneratorTest();

	@Mock
	private StringTemplateGroup mockedStringTemplateGroup;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		MockitoAnnotations.initMocks(logisticFormulaGeneratorTest);
		formula = new LogisticKPIFormulaGetY();
		template = "(1 / (0.983 + EXP(-4.074542 + $KPINAME$))) * 100;";
		StringTemplate st = new StringTemplate(template);
		PowerMockito.whenNew(StringTemplateGroup.class)
				.withArguments(Mockito.anyString(), Mockito.anyString())
				.thenReturn(mockedStringTemplateGroup);
		PowerMockito
				.when(mockedStringTemplateGroup
						.getInstanceOf("logisticKPIFormulaGetYTemplate"))
				.thenReturn(st);
		defaultPropMap = new HashMap<String, String>();
		defaultPropMap.put("DEFAULT", "default");
		properties = new ArrayList<PerfIndiSpecFormulaProperty>();
		properties.add(logisticFormulaGeneratorTest.setProperties("TARGET_KPI",
				"VOLTE_CONNECTIVITY_INDEX"));
		expectedResult = "(1 / (0.983 + EXP(-4.074542 + VOLTE_CONNECTIVITY_INDEX_INT))) * 100;";
	}

	@Test
	public void testGetFormulaToCalculate() throws Exception {
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("VOLTE_CONNECTIVITY_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("VOLTE_CONNECTIVITY_INDEX"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("VOLTE_CONNECTIVITY_INDEX"));
		Assert.assertEquals(expectedResult, actualResult);
	}
}
