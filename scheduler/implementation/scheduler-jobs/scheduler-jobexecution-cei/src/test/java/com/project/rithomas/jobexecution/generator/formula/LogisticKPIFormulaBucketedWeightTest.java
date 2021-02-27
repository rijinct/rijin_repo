
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
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
import org.powermock.reflect.Whitebox;

import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.utils.ModelResources;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { LogisticKPIFormulaGetCont.class, ModelResources.class,
		AbstractUserDefinedKPIFormula.class })
public class LogisticKPIFormulaBucketedWeightTest {

	private LogisticKPIFormulaBucketedWeight formula;

	private String template;

	private String expectedResult;

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
		formula = new LogisticKPIFormulaBucketedWeight();
		template = "$KpiObj:{ obj | $if(obj.commonDenom)$ pow( NVL( ( $endif$ $obj.kpiList:{kpiListObj | $if(!obj.commonDenom)$ pow( $endif$ if( $kpiListObj.sourceKpiInt$ is NULL or $kpiListObj.weight$ = 0, 0, $kpiListObj.weight$ ) $if(!obj.commonDenom)$ ,3) $endif$ $if(kpiListObj.check )$ + $endif$}$ $if(obj.commonDenom)$ ) / ( $obj.kpiList:{kpiListObj | if( $kpiListObj.sourceKpiInt$ is NULL or $kpiListObj.weight$ = 0, 0, 1) $if(kpiListObj.check)$ + $endif$}$ ) ,0 ),3) $endif$ $if(obj.check)$ + $endif$ }$";
		StringTemplate st = new StringTemplate(template);
		PowerMockito.whenNew(StringTemplateGroup.class)
				.withArguments(Mockito.anyString(), Mockito.anyString())
				.thenReturn(mockedStringTemplateGroup);
		PowerMockito
				.when(mockedStringTemplateGroup.getInstanceOf(
						"logisticKPIFormulaBucketedWeightTemplate"))
				.thenReturn(st);
	}

	@Test
	public void testGetFormulaForBucketedWeight() throws Exception {
		String jobName = "Perf_CEI2_BCSI_1_DAY_AggregateJob";
		Whitebox.setInternalState(
				logisticFormulaGeneratorTest.logisticFormulaGenerator,
				"jobName", jobName);
		PerformanceIndicatorSpec kpi = new PerformanceIndicatorSpec();
		kpi.setIndicatorspecId("CEI_BCSI_INDEX");
		kpi.setPiSpecName("CEI_BCSI_INDEX");
		String derivationalgorithm = new String(
				"<Formula ref=\"formula\">			\r\n" + "<Properties>\r\n"
						+ "<Property name=\"TYPE\" value=\"GET_XY\"/>\r\n"
						+ "<Property name=\"KPI\" value=\"COMPLAINTS\"/>\r\n"
						+ "<Property name=\"COMPLAINTS$KPIType\" value=\"LESSER_THE_BETTER\"/>\r\n"
						+ "<Property name=\"COMPLAINTS$Contrib\" value=\"yes\"/>\r\n"
						+ "<Property name=\"COMPLAINTS$WeightedInt\" value=\"no\"/>\r\n"
						+ "</Properties>\r\n" + "<ID>LOGISTIC</ID>\r\n"
						+ "</Formula>");
		kpi.setDerivationalgorithm(derivationalgorithm);
		kpi.setDerivationmethod("$formula$");
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("CEI_BCSI_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("CEI_BCSI_INDEX"));
		LogisticKPIFormulaBucketedWeight formula = new LogisticKPIFormulaBucketedWeight();
		StringTemplate st = new StringTemplate(template);
		List<PerfIndiSpecFormulaProperty> properties = new ArrayList<PerfIndiSpecFormulaProperty>();
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"COMPLAINTS"));
		properties.add(logisticFormulaGeneratorTest
				.setProperties("COMPLAINTS$WEIGHT", "6"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("CEI_BCSI_INDEX"));
		String expectedResult = "  pow(  if( COMPLAINTS is NULL or 777.0 = 0, 0, 777.0 )  ,3)     ";
		Assert.assertEquals(expectedResult, actualResult);
	}

	@Test
	public void testGetFormulaForCommonDenorm() throws Exception {
		String jobName = "Perf_CEI2_INDEX_1_1_HOUR_AggregateJob	";
		Whitebox.setInternalState(
				logisticFormulaGeneratorTest.logisticFormulaGenerator,
				"jobName", jobName);
		PerformanceIndicatorSpec kpi = new PerformanceIndicatorSpec();
		kpi.setIndicatorspecId("VOICE_CS_CEI_INDEX");
		kpi.setPiSpecName("VOICE_CS_CEI_INDEX");
		String derivationalgorithm = new String("<Formula ref=\"formula\">\r\n"
				+ "<Properties>\r\n"
				+ "<Property name=\"TYPE\" value=\"GET_XY\"/>\r\n"
				+ "<Property name=\"KPI\" value=\"MO_CALL_DROP_CORE_PERC\"/>\r\n"
				+ "<Property name=\"MO_CALL_DROP_CORE_PERC$KPIType\" value=\"LESSER_THE_BETTER\"/>\r\n"
				+ "<Property name=\"MO_CALL_DROP_CORE_PERC$Contrib\" value=\"yes\"/>\r\n"
				+ "<Property name=\"MO_CALL_DROP_CORE_PERC$WeightedInt\" value=\"no\"/>\r\n"
				+ "<Property name=\"MO_CALL_DROP_CORE_PERC$CommonDenorm\" value=\"MO_CALL_ANS_ATTEMPTS\"/>\r\n"
				+ "<Property name=\"KPI\" value=\"MO_CALL_DROP_RADIO_PERC\"/>\r\n"
				+ "<Property name=\"MO_CALL_DROP_RADIO_PERC$KPIType\" value=\"LESSER_THE_BETTER\"/>\r\n"
				+ "<Property name=\"MO_CALL_DROP_RADIO_PERC$Contrib\" value=\"yes\"/>\r\n"
				+ "<Property name=\"MO_CALL_DROP_RADIO_PERC$WeightedInt\" value=\"no\"/>\r\n"
				+ "<Property name=\"MO_CALL_DROP_RADIO_PERC$CommonDenorm\" value=\"MO_CALL_ANS_ATTEMPTS\"/>\r\n"
				+ "<Property name=\"KPI\" value=\"MT_CALL_DROP_CORE_PERC\"/>\r\n"
				+ "<Property name=\"MT_CALL_DROP_CORE_PERC$KPIType\" value=\"LESSER_THE_BETTER\"/>\r\n"
				+ "<Property name=\"MT_CALL_DROP_CORE_PERC$Contrib\" value=\"yes\"/>\r\n"
				+ "<Property name=\"MT_CALL_DROP_CORE_PERC$WeightedInt\" value=\"no\"/>\r\n"
				+ "<Property name=\"MT_CALL_DROP_CORE_PERC$CommonDenorm\" value=\"MT_CALL_ANS\"/>\r\n"
				+ "<Property name=\"KPI\" value=\"MT_CALL_DROP_RADIO_PERC\"/>\r\n"
				+ "<Property name=\"MT_CALL_DROP_RADIO_PERC$KPIType\" value=\"LESSER_THE_BETTER\"/>\r\n"
				+ "<Property name=\"MT_CALL_DROP_RADIO_PERC$Contrib\" value=\"yes\"/>\r\n"
				+ "<Property name=\"MT_CALL_DROP_RADIO_PERC$WeightedInt\" value=\"no\"/>\r\n"
				+ "<Property name=\"MT_CALL_DROP_RADIO_PERC$CommonDenorm\" value=\"MT_CALL_ANS\"/>\r\n"
				+ "</Properties>\r\n" + "<ID>LOGISTIC</ID>\r\n" + "</Formula>");
		kpi.setDerivationalgorithm(derivationalgorithm);
		kpi.setDerivationmethod("$formula$");
		PowerMockito
				.when(logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("VOICE_CS_CEI_INDEX"))
				.thenReturn(logisticFormulaGeneratorTest.jsonRetriever
						.getKPIResponse("VOICE_CS_CEI_INDEX"));
		LogisticKPIFormulaBucketedWeight formula = new LogisticKPIFormulaBucketedWeight();
		List<PerfIndiSpecFormulaProperty> properties = new ArrayList<PerfIndiSpecFormulaProperty>();
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"MO_CALL_DROP_CORE_PERC"));
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"MO_CALL_DROP_RADIO_PERC"));
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"MT_CALL_DROP_CORE_PERC"));
		properties.add(logisticFormulaGeneratorTest.setProperties(
				"MT_CALL_DROP_CORE_PERC$COMMONDENORM", "COMMON_DENOM_KPI"));
		properties.add(logisticFormulaGeneratorTest.setProperties("KPI",
				"MT_CALL_DROP_RADIO_PERC"));
		properties.add(logisticFormulaGeneratorTest.setProperties(
				"MT_CALL_DROP_RADIO_PERC$COMMONDENORM", "COMMON_DENOM_KPI"));
		String actualResult = formula.getFormulaToCalculate(properties,
				logisticFormulaGeneratorTest.mockIndexConfigRetriever
						.getKPIResponse("VOICE_CS_CEI_INDEX"));
		String expectedResult = "  pow(  if( MO_CALL_DROP_CORE_PERC is NULL or 1.0 = 0, 0, 1.0 )  ,3)     +    pow(  if( MO_CALL_DROP_RADIO_PERC is NULL or 1.0 = 0, 0, 1.0 )  ,3)   "
				+ "  +   pow( NVL( (   if( MT_CALL_DROP_CORE_PERC is NULL or 1.0 = 0, 0, 1.0 )   +  if( MT_CALL_DROP_RADIO_PERC is NULL or 1.0 = 0, 0, 1.0 )    ) / ( if( MT_CALL_DROP_CORE_PERC is NULL or 1.0 = 0, 0, 1)  "
				+ "+ if( MT_CALL_DROP_RADIO_PERC is NULL or 1.0 = 0, 0, 1)  ) ,0 ),3)   ";
		Assert.assertEquals(StringUtils.normalizeSpace(expectedResult),
				StringUtils.normalizeSpace(actualResult));
	}
}
