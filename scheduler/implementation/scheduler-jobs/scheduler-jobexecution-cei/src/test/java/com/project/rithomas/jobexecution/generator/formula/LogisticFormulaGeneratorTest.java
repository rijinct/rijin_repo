
package com.project.rithomas.jobexecution.generator.formula;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.rijin.analytics.hierarchy.service.KPIHierarchyIndexConfigRetreiver;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.jobexecution.generator.util.FormulaGeneratorUtil;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUse;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.PerformanceSpecInterval;
import com.project.rithomas.sdk.model.performance.PerformanceSpecification;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormula;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.performance.query.PerformanceSpecIntervalQuery;
import com.project.rithomas.sdk.model.performance.query.PerformanceSpecificationQuery;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { KPIHierarchyIndexConfigRetreiver.class,
		LogisticFormulaGenerator.class, ModelResources.class,
		SDKSystemEnvironment.class, PerformanceSpecificationQuery.class,
		PerformanceSpecIntervalQuery.class, StringTemplateGroup.class,
		AbstractUserDefinedKPIFormula.class, FormulaGeneratorUtil.class })
public class LogisticFormulaGeneratorTest {

	LogisticFormulaGenerator logisticFormulaGenerator = new LogisticFormulaGenerator();

	@Mock
	public KPIHierarchyIndexConfigRetreiver mockIndexConfigRetriever;

	@Mock
	EntityManager entityManager;

	@Mock
	TypedQuery<PerfIndiSpecFormula> query;

	String[] hierarchyGroupNames = { "usecase", "ceiDesignerProp" };

	Map<String, KPIIndexConfiguration> kpiObjectMap = new LinkedHashMap<>();

	@Mock
	PerformanceSpecificationQuery mockQuery;

	@Mock
	PerformanceSpecification latestPerfSpec;

	@Mock
	PerformanceSpecIntervalQuery mockIntervalQuery;

	IndexConfigRetriever jsonRetriever = new IndexConfigRetriever();

	@Mock
	private StringTemplateGroup mockedStringTemplateGroup;

	String kpiFormulaTemplate;

	StringTemplate kpiFormulaSt;

	Map<String, Object> nameFormulaMapping = new HashMap<String, Object>();

	@Before
	public void setUp() throws Exception {
		Whitebox.setInternalState(logisticFormulaGenerator,
				"kpiConfigRetriever", mockIndexConfigRetriever);
		PerformanceSpecInterval interval = new PerformanceSpecInterval();
		interval.setInterval("DAY");
		interval.setSpecIntervalID("DAY");
		PowerMockito.whenNew(PerformanceSpecIntervalQuery.class)
				.withNoArguments().thenReturn(mockIntervalQuery);
		PowerMockito.when(mockIntervalQuery.retrieve("DAY", null))
				.thenReturn(interval);
		PowerMockito
				.when(mockIndexConfigRetriever
						.getConfiguredHierarchyGroupAndName())
				.thenReturn(hierarchyGroupNames);
		mockStatic(ModelResources.class);
		when(ModelResources.getEntityManager()).thenReturn(entityManager);
		mockStatic(PerformanceSpecificationQuery.class);
		PowerMockito.whenNew(PerformanceSpecificationQuery.class)
				.withNoArguments().thenReturn(mockQuery);
		PowerMockito
				.when(mockQuery.retrieveLatestDeployedSpec(Mockito.anyString()))
				.thenReturn(latestPerfSpec);
		mockStatic(PerformanceSpecIntervalQuery.class);
		when(entityManager.createNamedQuery(
				PerfIndiSpecFormula.FIND_BY_FORMULAID,
				PerfIndiSpecFormula.class)).thenReturn(query);
		List<PerfIndiSpecFormula> formulae = new ArrayList<>();
		PerfIndiSpecFormula formula = new PerfIndiSpecFormula();
		formula.setFormulaID("LOGISTIC");
		formulae.add(formula);
		PowerMockito.when(query.getResultList()).thenReturn(formulae);
		PowerMockito.when(
				query.setParameter(PerfIndiSpecFormula.FORMULA_ID, "LOGISTIC"))
				.thenReturn(query);
		mockStatic(SDKSystemEnvironment.class);
		PowerMockito.when(SDKSystemEnvironment.getschedulerConfDirectory())
				.thenReturn("./src/test/resources");
		kpiFormulaTemplate = "case  \r\n"
				+ "$formula_obj_list:{ formula_obj | when $formula_obj.dimAndValList:{ dimAndVal | $dimAndVal$ };separator=\" and \"$ then\r\n"
				+ "(\r\n" + "$formula_obj.weightedAvgFormula$\r\n" + ")}$\r\n"
				+ "else\r\n" + "$defaultFormula$\r\n" + "end";
		kpiFormulaSt = new StringTemplate(kpiFormulaTemplate);
		mockStringTemplate(kpiFormulaSt);
		mockStatic(FormulaGeneratorUtil.class);
		List<PerfSpecAttributesUse> perfSpecMeasUsingList = new ArrayList<PerfSpecAttributesUse>();
		PerfSpecAttributesUse attrUse1 = new PerfSpecAttributesUse();
		PerformanceSpecInterval intervalid = new PerformanceSpecInterval();
		intervalid.setInterval("DAY");
		intervalid.setSpecIntervalID("DAY");
		attrUse1.setIntervalid(intervalid);
		PerformanceIndicatorSpec perfindicatorspecid = new PerformanceIndicatorSpec();
		perfindicatorspecid.setPiSpecName("VOLTE_CONNECTIVITY_INDEX");
		perfindicatorspecid.setIndicatorspecId("VOLTE_CONNECTIVITY_INDEX");
		PerformanceIndicatorSpec perfindicatorspecid2 = new PerformanceIndicatorSpec();
		perfindicatorspecid2.setPiSpecName("VOLTE_REG_FAIL_PERC");
		perfindicatorspecid2.setIndicatorspecId("VOLTE_REG_FAIL_PERC");
		attrUse1.setPerfindicatorspecid(perfindicatorspecid);
		PerfSpecAttributesUse attrUse2 = new PerfSpecAttributesUse();
		attrUse2.setIntervalid(intervalid);
		attrUse2.setPerfindicatorspecid(perfindicatorspecid2);
		attrUse2.setSequence(2);
		attrUse1.setSequence(1);
		perfSpecMeasUsingList.add(attrUse1);
		perfSpecMeasUsingList.add(attrUse2);
		List<PerfSpecAttributesUse> srcPerfSpecAttributeUse = new ArrayList<PerfSpecAttributesUse>();
		PerfSpecAttributesUse srcAttrUse1 = new PerfSpecAttributesUse();
		PerformanceIndicatorSpec srcPerfindicatorspecid = new PerformanceIndicatorSpec();
		srcPerfindicatorspecid.setPiSpecName("VOLTE_INDEX");
		srcPerfindicatorspecid.setIndicatorspecId("VOLTE_INDEX");
		srcAttrUse1.setPerfindicatorspecid(srcPerfindicatorspecid);
		srcAttrUse1.setIntervalid(intervalid);
		srcAttrUse1.setSequence(1);
		PerformanceIndicatorSpec srcPerfindicatorspecid2 = new PerformanceIndicatorSpec();
		srcPerfindicatorspecid2.setPiSpecName("VOLTE_USAGE_INDEX");
		srcPerfindicatorspecid2.setIndicatorspecId("VOLTE_USAGE_INDEX");
		PerfSpecAttributesUse srcAttrUse2 = new PerfSpecAttributesUse();
		srcAttrUse2.setIntervalid(intervalid);
		srcAttrUse2.setPerfindicatorspecid(srcPerfindicatorspecid2);
		srcAttrUse2.setSequence(2);
		srcPerfSpecAttributeUse.add(srcAttrUse1);
		srcPerfSpecAttributeUse.add(srcAttrUse2);
		PerformanceSpecification latestPerfSpec = new PerformanceSpecification();
		latestPerfSpec.setPerfSpecAttributesUses(perfSpecMeasUsingList);
		PowerMockito
				.when(mockQuery.retrieveLatestDeployedSpec(Mockito.anyString()))
				.thenReturn(latestPerfSpec);
		PowerMockito.when(FormulaGeneratorUtil.getAllSrcKPIs(latestPerfSpec))
				.thenReturn(srcPerfSpecAttributeUse);
	}

	public PerfIndiSpecFormulaProperty setProperties(String name,
			String value) {
		PerfIndiSpecFormulaProperty property = new PerfIndiSpecFormulaProperty();
		property.setpropertyName(name);
		property.setPropertyValue(value);
		return property;
	}

	public void mockStringTemplate(StringTemplate st) throws Exception {
		PowerMockito.whenNew(StringTemplateGroup.class)
				.withArguments(Mockito.anyString(), Mockito.anyString())
				.thenReturn(mockedStringTemplateGroup);
		PowerMockito.when(mockedStringTemplateGroup.getInstanceOf("kpiFormula"))
				.thenReturn(kpiFormulaSt);
		PowerMockito
				.when(mockedStringTemplateGroup
						.getInstanceOf("logisticKPIFormulaGetContTemplate"))
				.thenReturn(st);
		PowerMockito
				.when(mockedStringTemplateGroup
						.getInstanceOf("logisticKPIFormulaGetYTemplate"))
				.thenReturn(st);
		PowerMockito
				.when(mockedStringTemplateGroup
						.getInstanceOf("logisticKPIFormulaGetUsageWeight"))
				.thenReturn(st);
		PowerMockito
				.when(mockedStringTemplateGroup.getInstanceOf(
						"logisticKPIFormulaGetHigherUsageWeight"))
				.thenReturn(st);
	}

	@Test
	public void testGetFormulaForBCSI() throws Exception {
		String template = "CASE WHEN $KPI_WEIGHT$ = 0.0 THEN cast(NULL as double) ELSE LEAST(pow( pow(($KPI_INT$ * $KPI_WEIGHT$ ),3)/$KPI_BUCKET$ , 1/3 ), 16) END";
		StringTemplate st = new StringTemplate(template);
		mockStringTemplate(st);
		String jobName = "Perf_CEI2_BCSI_1_DAY_AggregateJob";
		Whitebox.setInternalState(logisticFormulaGenerator, "jobName", jobName);
		PerformanceIndicatorSpec kpi = new PerformanceIndicatorSpec();
		kpi.setIndicatorspecId("CEI2_BCSI_INDEX");
		String derivationalgorithm = new String("<Formula ref=\"formula\">\r\n"
				+ "    <ID>LOGISTIC</ID>\r\n" + "    <Properties>\r\n"
				+ "      <Property name=\"KPI\" value=\"DISPUTE_EVENTS\"/>\r\n"
				+ "      <Property name=\"DISPUTE_EVENTS$KPITYPE\" value=\"LESSER_THE_BETTER\"/>\r\n"
				+ "      <Property name=\"OVERALL_INT\" value=\"CEI_BCSI_INDEX\"/>\r\n"
				+ "      <Property name=\"TYPE\" value=\"GET_CONTRI\"/>\r\n"
				+ "      <Property name=\"TARGET_KPI\" value=\"CEI_BCSI_INDEX\"/>\r\n"
				+ "    </Properties>\r\n" + "  </Formula>");
		kpi.setDerivationalgorithm(derivationalgorithm);
		kpi.setDerivationmethod("$formula$");
		PowerMockito
				.when(mockIndexConfigRetriever.getKPIResponse("CEI_BCSI_INDEX"))
				.thenReturn(jsonRetriever.getKPIResponse("CEI_BCSI_INDEX"));
		assertEquals(
				"CASE WHEN 1.0 = 0.0 THEN cast(NULL as double) ELSE LEAST(pow( pow((DISPUTE_EVENTS_INT * 1.0 ),3)/CEI_BCSI_INDEX_BUCKET , 1/3 ), 16) END",
				logisticFormulaGenerator.getFormula(kpi, nameFormulaMapping));
	}

	@Test
	public void testExceptionHandling() throws Exception {
		PerformanceIndicatorSpec kpi = new PerformanceIndicatorSpec();
		kpi.setIndicatorspecId("CEI2_INDEX");
		kpi.setDerivationmethod("$formula$");
		kpi.setDerivationalgorithm("derivationalgorithm");
		try {
			Whitebox.setInternalState(logisticFormulaGenerator, "jobName",
					"Perf_CEI2_INDEX_1_1_DAY_AggregateJob");
			logisticFormulaGenerator.getFormula(kpi);
		} catch (FormulaGeneratorException e) {
			assertTrue(
					e.getMessage().contains("Error while generating formula"));
		}
	}

	@Test
	public void testGetFormulaForDimensionBasedWeights() throws Exception {
		String template = "LEAST(pow ( ($KPI_OBJ:{ obj | pow(( nvl( $obj.sourceKpiInt$ , '0'))*$obj.weight$ , 3 ) $if(obj.check)$ + $endif$ }$ ) / $TARGET_KPI_BUCKET$, 1/3), 16)";
		PowerMockito
				.when(mockedStringTemplateGroup.getInstanceOf(
						"logisticKPIFormulaGetWeightedAvgTemplate"))
				.thenAnswer(invocation -> new StringTemplate(template));
		String jobName = "Perf_CEI2_APPCAT_TYPE_2_1_DAY_AggregateJob";
		Whitebox.setInternalState(logisticFormulaGenerator, "jobName", jobName);
		PerformanceIndicatorSpec kpi = new PerformanceIndicatorSpec();
		kpi.setIndicatorspecId("USAGE_CEI_INDEX");
		kpi.setPiSpecName("USAGE_CEI_INDEX");
		String derivationalgorithm = new String("<Formula ref=\"formula\">\r\n"
				+ "  <Properties>\r\n"
				+ "    <Property name=\"TYPE\" value=\"GET_X\"/>        \r\n"
				+ "    <Property name=\"KPI\" value=\"MOS_UL\"/>\r\n"
				+ "    <Property name=\"MOS_UL$KPITYPE\" value=\"GREATER_THE_BETTER\"/>\r\n"
				+ "    <Property name=\"MOS_UL$CONTRIB\" value=\"no\"/>\r\n"
				+ "    <Property name=\"MOS_UL$WEIGHTEDINT\" value=\"no\"/>\r\n"
				+ "    <Property name=\"KPI\" value=\"PACKET_RETRANSMISSION_RATE_DL\"/>\r\n"
				+ "    <Property name=\"PACKET_RETRANSMISSION_RATE_DL$KPITYPE\" value=\"LESSER_THE_BETTER\"/>\r\n"
				+ "    <Property name=\"PACKET_RETRANSMISSION_RATE_DL$CONTRIB\" value=\"yes\"/>\r\n"
				+ "    <Property name=\"PACKET_RETRANSMISSION_RATE_DL$WEIGHTEDINT\" value=\"no\"/>   \r\n"
				+ "      <Property name=\"TARGET_KPI\" value=\"USAGE_CEI_INDEX\"/>\r\n"
				+ "  </Properties>\r\n"
				+ "  <Properties type=\"APP_CATEGORY_TYPE:COMMUNICATION_TYPE;SUBS_TYPE:2G;DEVICE_TYPE:Nokia\">\r\n"
				+ "<Property name=\"MOS_DL$Weight\" value=\"1\"/>"
				+ "<Property name=\"PACKET_RETRANSMISSION_RATE_DL$Weight\" value=\"1\"/>"
				+ "  </Properties>\r\n"
				+ "  <Properties type=\"APP_CATEGORY_TYPE:CONTENT_SHARING_TYPE;SUBS_TYPE:2G;DEVICE_TYPE:Nokia\">\r\n"
				+ "<Property name=\"MOS_DL$Weight\" value=\"1\"/>"
				+ "<Property name=\"PACKET_RETRANSMISSION_RATE_DL$Weight\" value=\"1\"/>"
				+ "  </Properties>  \r\n" + "  <ID>LOGISTIC</ID>\r\n"
				+ "</Formula>");
		kpi.setDerivationalgorithm(derivationalgorithm);
		kpi.setDerivationmethod("$formula$");
		PowerMockito
				.when(mockIndexConfigRetriever
						.getKPIResponse("USAGE_CEI_INDEX"))
				.thenReturn(jsonRetriever.getKPIResponse("USAGE_CEI_INDEX"));
		String expected = new String(
				"case  \r\n" + 
				"when APP_CATEGORY_TYPE = 'COMMUNICATION_TYPE'  and SUBS_TYPE = '3G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*1.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'CONTENT_SHARING_TYPE'  and SUBS_TYPE = '3G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'COMMUNICATION_TYPE'  and SUBS_TYPE = '4G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*1.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'CONTENT_SHARING_TYPE'  and SUBS_TYPE = '4G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'COMMUNICATION_TYPE'  and SUBS_TYPE = '2G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*1.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'CONTENT_SHARING_TYPE'  and SUBS_TYPE = '2G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'STREAMING_TYPE'  and SUBS_TYPE = '3G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'STREAMING_TYPE'  and SUBS_TYPE = '4G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'STREAMING_TYPE'  and SUBS_TYPE = '2G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'BROWSING_TYPE'  and SUBS_TYPE = '3G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'BROWSING_TYPE'  and SUBS_TYPE = '2G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'BROWSING_TYPE'  and SUBS_TYPE = '4G'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'STREAMING_TYPE'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'CONTENT_SHARING_TYPE'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'BROWSING_TYPE'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")when APP_CATEGORY_TYPE = 'COMMUNICATION_TYPE'  then\r\n" + 
				"(\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*1.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*1.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				")\r\n" + 
				"else\r\n" + 
				"LEAST(pow ( (pow(( nvl( MOS_UL_INT , '0'))*0.0 , 3 )  +  pow(( nvl( PACKET_RETRANSMISSION_RATE_DL_INT , '0'))*0.0 , 3 )   ) / USAGE_CEI_INDEX_BUCKET, 1/3), 16)\r\n" + 
				"end");
		String actualResult = logisticFormulaGenerator.getFormula(kpi,
				nameFormulaMapping);
		assertEquals(StringUtils.normalizeSpace(expected),
				StringUtils.normalizeSpace(actualResult));
	}

	@Test
	public void additionalINTandBucketTest() throws Exception {
		String template2 = "$if(GREATER_THE_BETTER)$ \r\n"
				+ "$endif$";
		StringTemplate st2 = new StringTemplate(template2);
		PowerMockito
				.when(mockedStringTemplateGroup.getInstanceOf(
						"logisticKPIFormulaGetThresholdTemplate"))
				.thenReturn(st2);
		String template3 = "$KpiObj:{ obj | $if(obj.commonDenom)$ pow( NVL( ( $endif$ $obj.kpiList:{kpiListObj | $if(!obj.commonDenom)$ pow( $endif$ if( $kpiListObj.sourceKpiInt$ is NULL or $kpiListObj.weight$ = 0, 0, $kpiListObj.weight$ ) $if(!obj.commonDenom)$ ,3) $endif$ $if(kpiListObj.check )$ + $endif$}$ $if(obj.commonDenom)$ ) / ( $obj.kpiList:{kpiListObj | if( $kpiListObj.sourceKpiInt$ is NULL or $kpiListObj.weight$ = 0, 0, 1) $if(kpiListObj.check)$ + $endif$}$ ) ,0 ),3) $endif$ $if(obj.check)$ + $endif$ }$";
		StringTemplate st3 = new StringTemplate(template3);
		PowerMockito
				.when(mockedStringTemplateGroup.getInstanceOf(
						"logisticKPIFormulaBucketedWeightTemplate"))
				.thenReturn(st3);
		String template = "(1 / (0.983 + EXP(-4.074542 + $KPINAME$))) * 100;";
		StringTemplate st = new StringTemplate(template);
		mockStringTemplate(st);
		String jobName = "Perf_CEI2_INDEX_1_1_DAY_AggregateJob";
		Whitebox.setInternalState(logisticFormulaGenerator, "jobName", jobName);
		PerformanceIndicatorSpec kpi = new PerformanceIndicatorSpec();
		kpi.setIndicatorspecId("VOLTE_CONNECTIVITY_INDEX");
		kpi.setPiSpecName("VOLTE_CONNECTIVITY_INDEX");
		String derivationAlgorithm = "<Formula ref=\"formula\">                                                          +\r\n"
				+ "     <ID>LOGISTIC</ID>                                                            +\r\n"
				+ "     <Properties>                                                                 +\r\n"
				+ "       <Property name=\"TYPE\" value=\"GET_Y\"/>                                     +\r\n"
				+ "       <Property name=\"USAGE_KPI\" value=\"VOLTE_CONNECTIVITY\"/>                    +\r\n"
				+ "       <Property name=\"USAGE_ENABLE\" value=\"yes\"/>                                +\r\n"
				+ "       <Property name=\"USAGE_FORMULA\" value=\"(USAGE_KPI/AVG_USAGE)\"/>             +\r\n"
				+ "       <Property name=\"USAGE_DEFAULT_VALUE\" value=\"1.0\"/>                         +\r\n"
				+ "       <Property name=\"KPI\" value=\"VOLTE_REG_FAIL_PERC\"/>                         +\r\n"
				+ "       <Property name=\"VOLTE_REG_FAIL_PERC$LT\" value=\"0\"/>                        +\r\n"
				+ "       <Property name=\"VOLTE_REG_FAIL_PERC$UT\" value=\"3\"/>                        +\r\n"
				+ "       <Property name=\"VOLTE_REG_FAIL_PERC$TV\" value=\"20\"/>                       +\r\n"
				+ "       <Property name=\"VOLTE_REG_FAIL_PERC$KPITYPE\" value=\"LESSER_THE_BETTER\"/>   +\r\n"
				+ "       <Property name=\"VOLTE_REG_FAIL_PERC$WEIGHT\" value=\"1\"/>                    +\r\n"
				+ "       <Property name=\"VOLTE_REG_FAIL_PERC$CONTRIB\" value=\"yes\"/>                 +\r\n"
				+ "       <Property name=\"VOLTE_REG_FAIL_PERC$WEIGHTEDINT\" value=\"no\"/>              +\r\n"
				+ "       <Property name=\"TARGET_KPI\" value=\"VOLTE_CONNECTIVITY_INDEX\"/>             +\r\n"
				+ "     </Properties>                                                                +\r\n"
				+ "   </Formula>       ";
		kpi.setDerivationalgorithm(derivationAlgorithm);
		kpi.setDerivationmethod("$formula$");
		PowerMockito
				.when(mockIndexConfigRetriever
						.getKPIResponse("VOLTE_CONNECTIVITY_INDEX"))
				.thenReturn(jsonRetriever
						.getKPIResponse("VOLTE_CONNECTIVITY_INDEX"));
		logisticFormulaGenerator.getFormula(kpi, nameFormulaMapping);
		List<String> keyList = new ArrayList<String>();
