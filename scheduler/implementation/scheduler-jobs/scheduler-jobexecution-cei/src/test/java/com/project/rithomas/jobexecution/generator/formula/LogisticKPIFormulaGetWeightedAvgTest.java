
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
@PrepareForTest(value = { LogisticKPIFormulaGetCont.class, ModelResources.class,
		AbstractUserDefinedKPIFormula.class })
public class LogisticKPIFormulaGetWeightedAvgTest {

	private LogisticKPIFormulaGetWeightedAvg formula;

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
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("CEI_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("CEI_INDEX"));
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("VOICE_CEI_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("VOICE_CEI_INDEX"));
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("SMS_CEI_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("SMS_CEI_INDEX"));
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("DATA_CEI_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("DATA_CEI_INDEX"));
		formula = new LogisticKPIFormulaGetWeightedAvg();
		template = "pow ( ($KPI_OBJ:{ obj | pow(( nvl( $obj.sourceKpiInt$ , ''0''))*$obj.weight$ , 3 ) $if(obj.check)$ + $endif$ }$) / $TARGET_KPI_BUCKET$ , 1/3)";
		StringTemplate st = new StringTemplate(template);
		PowerMockito.whenNew(StringTemplateGroup.class)
				.withArguments(Mockito.anyString(), Mockito.anyString())
				.thenReturn(mockedStringTemplateGroup);
		PowerMockito
				.when(mockedStringTemplateGroup.getInstanceOf(
						"logisticKPIFormulaGetWeightedAvgTemplate"))
				.thenReturn(st);
		new HashMap<String, String>();
		properties = new ArrayList<PerfIndiSpecFormulaProperty>();
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"VOICE_CEI_INDEX"));
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"SMS_CEI_INDEX"));
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"DATA_CEI_INDEX"));
		properties.add(logisticFormulaGeneratorTest.setProperties("TARGET_KPI",
				"CEI_INDEX"));
		expectedResult = "pow ( (pow(( nvl( VOICE_CEI_INDEX_INT , ''0''))*1.0 , 3 )  +  pow(( nvl( SMS_CEI_INDEX_INT , ''0''))*1.0 , 3 )  +  pow(( nvl( DATA_CEI_INDEX_INT , ''0''))*1.0 , 3 )  ) / CEI_INDEX_BUCKET , 1/3)";
	}

	@Test
	public void testGetFormulaToCalculate() throws Exception {
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("CEI_INDEX"));
		Assert.assertEquals(expectedResult, actualResult);
	}
}