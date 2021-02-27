
package com.project.rithomas.jobexecution.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.jobexecution.generator.formula.FormulaGeneratorFactory;
import com.project.rithomas.jobexecution.generator.formula.LogisticFormulaGenerator;
import com.project.rithomas.sdk.model.common.CharacteristicSpecification;
import com.project.rithomas.sdk.model.performance.KPIFormulaRel;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUse;
import com.project.rithomas.sdk.model.performance.PerfSpecMeasUsing;
import com.project.rithomas.sdk.model.performance.PerfUsageSpecRel;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.PerformanceSpecInterval;
import com.project.rithomas.sdk.model.performance.PerformanceSpecification;
import com.project.rithomas.sdk.model.performance.query.PerformanceIndicatorSpecQuery;
import com.project.rithomas.sdk.model.performance.query.PerformanceSpecificationQuery;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

@Ignore
@PrepareForTest(value = { AggregationUtil.class, ModelResources.class,
		LogisticFormulaGenerator.class, PerformanceSpecificationQuery.class,
		PerfSpecAttributesUse.class, PerformanceSpecification.class,
		PerfUsageSpecRel.class, FormulaGeneratorFactory.class })
@RunWith(PowerMockRunner.class)
public class AggregationUtilTest {

	WorkFlowContext context = new JobExecutionContext();

	AggregationUtil aggregationUtil;

	@Mock
	PerformanceSpecificationQuery perfSpecQuery;

	@Mock
	PerformanceIndicatorSpecQuery kpiQuery;

	@Mock
	PerformanceSpecification mockperPerformance;

	@Mock
	LogisticFormulaGenerator mockLogisticFormulaGenerator;

	@Mock
	EntityManager entityManager;

	@Mock
	TypedQuery<KPIFormulaRel> query;

	@Mock
	PerformanceIndicatorSpec mockPerformanceIndicatorSpec;

	@Mock
	PerfUsageSpecRel perfUsageSpecRel;

	@Mock
	FormulaGeneratorFactory formulaGeneratorFactory;

	@Before
	public void setUp() throws Exception {
		mockStatic(ModelResources.class, LogisticFormulaGenerator.class,
				PerformanceSpecificationQuery.class,
				PerfSpecAttributesUse.class, PerformanceSpecification.class,
				PerformanceIndicatorSpec.class);
		when(ModelResources.getEntityManager()).thenReturn(entityManager);
		when(entityManager.createNamedQuery(KPIFormulaRel.FIND_BY_ID,
				KPIFormulaRel.class)).thenReturn(query);
		context.setProperty(GeneratorWorkFlowContext.CARRYFWD, "abc");
		when(query.setParameter(KPIFormulaRel.KPI_ID, "CEI_INDEX_D1"))
				.thenReturn(query);
		when(query.setParameter(KPIFormulaRel.KPI_ID, "SMS_VKPI"))
				.thenReturn(query);
		when(query.setParameter(KPIFormulaRel.KPI_ID, "SMS_VKPI_D1"))
				.thenReturn(query);
		when(query.setParameter(KPIFormulaRel.KPI_ID, "VOICE_CEI_INDEX_D1"))
				.thenReturn(query);
		when(query.setParameter(KPIFormulaRel.KPI_ID, "VOICE_VKPI"))
				.thenReturn(query);
		whenNew(PerformanceSpecificationQuery.class).withNoArguments()
				.thenReturn(perfSpecQuery);
		whenNew(PerformanceIndicatorSpecQuery.class).withNoArguments()
				.thenReturn(kpiQuery);
		PerformanceIndicatorSpec performanceIndicatorSpec = new PerformanceIndicatorSpec();
		performanceIndicatorSpec.setIndicatorspecId("specId");
		performanceIndicatorSpec.setIndicatorcategory("x");
		performanceIndicatorSpec.setPiSpecName("CEI_INDEX");
		Collection<PerfSpecAttributesUse> collection = new ArrayList<PerfSpecAttributesUse>();
		collection.add(new PerfSpecAttributesUse());
		PerformanceSpecification testPs = new PerformanceSpecification();
		testPs.setVersion("111");
		testPs.setPerfSpecAttributesUses(collection);
		when(perfSpecQuery.retrieveLatestDeployedSpec("CEI_INDEX"))
				.thenReturn(testPs);
		when(perfSpecQuery.retrieve(Mockito.anyString(), Mockito.anyString()))
				.thenReturn(mockperPerformance);
		Collection<PerfSpecAttributesUse> performanceSpecificationsAttributes = new ArrayList<>();
		PerfSpecAttributesUse perfSpecAttributesUse = new PerfSpecAttributesUse();
		PerformanceSpecInterval performanceSpecInterval = new PerformanceSpecInterval();
		performanceSpecInterval.setSpecIntervalID("HOUR");
		perfSpecAttributesUse.setIntervalid(performanceSpecInterval);
		performanceSpecificationsAttributes.add(perfSpecAttributesUse);
		when(mockperPerformance.getPerfSpecAttributesUses())
				.thenReturn(performanceSpecificationsAttributes);
		List<PerfSpecMeasUsing> list = new ArrayList<PerfSpecMeasUsing>();
		PerfSpecMeasUsing perfSpecMeasUsing = new PerfSpecMeasUsing();
		perfSpecMeasUsing.setId(222);
		perfSpecMeasUsing.setPerfindicatorspecid(performanceIndicatorSpec);
		perfSpecMeasUsing.setIntervalid(performanceSpecInterval);
		list.add(perfSpecMeasUsing);
		when(mockperPerformance.getPerfSpecsMeasUsing()).thenReturn(list);
		Collection<PerfUsageSpecRel> collectionper = new ArrayList<>();
		PerfUsageSpecRel per = new PerfUsageSpecRel();
		per.setPerfspecidS(new PerformanceSpecification());
		collectionper.add(per);
		when(mockLogisticFormulaGenerator.getFormula(performanceIndicatorSpec,
				new HashMap<String, Object>())).thenReturn("CEI_INDEX");
		context.setProperty(GeneratorWorkFlowContext.CARRYFWD, "carry");
		PowerMockito.whenNew(PerformanceIndicatorSpec.class).withNoArguments()
				.thenReturn(mockPerformanceIndicatorSpec);
		when(kpiQuery.retrieve(performanceIndicatorSpec.getIndicatorspecId(),
				null)).thenReturn(performanceIndicatorSpec);
		PowerMockito.whenNew(LogisticFormulaGenerator.class).withNoArguments()
				.thenReturn(mockLogisticFormulaGenerator);
		PowerMockito.whenNew(PerformanceSpecificationQuery.class)
				.withNoArguments().thenReturn(perfSpecQuery);
		aggregationUtil = new AggregationUtil();
	}

	@Test
	public void testGetNameFormulaMapping() throws JobExecutionException {
		aggregationUtil.jobName = "Perf_CEI_INDEX_1_HOUR_AggregateJob";
		List<String> Kpis = new ArrayList<>();
		Kpis.add("");
		aggregationUtil.getKPINameIDFormulaMapping(Kpis);
		Map<String, Object> nameFormulaMapping = aggregationUtil.nameFormulaMapping;
		assertNotNull(nameFormulaMapping);
		assertTrue(Arrays.asList("CEI_INDEX", "CEI_INDEX_D1", "SMS_CEI_INDEX",
				"SMS_CEI_INDEX_D1", "SMS_VKPI", "SMS_VKPI_D1",
				"VOICE_CEI_INDEX", "VOICE_CEI_INDEX_D1", "VOICE_VKPI",
				"KPI_LIST").containsAll(nameFormulaMapping.keySet()));
	}

	@Test
	public void testReplaceKPIFormula()
			throws JobExecutionException, FormulaGeneratorException {
		context.setProperty(JobExecutionContext.SQL,
				actual);
	}

	@Test
	public void testReplaceTwiceKPIFormula()
			throws JobExecutionException, FormulaGeneratorException {
		context.setProperty(JobExecutionContext.SQL,
						.thenReturn("CEI_INDEX");
		String actual = aggregationUtil.replaceKPIFormula(context);
		assertEquals("select CEI_INDEX s um(test) from test", actual);
	}

	@Test
	public void testNoUserDefinedFormula() throws JobExecutionException {
		aggregationUtil.jobName = "Perf_CEI_SMS_1_1_HOUR_AggregateJob";
		List<String> Kpis = new ArrayList<>();
		Kpis.add("VOICE_VKPI");
		when(mockperPerformance.getVersion()).thenReturn("111");
		when(perfSpecQuery.retrieveLatestDeployedSpec("CEI_SMS_1"))
				.thenReturn(mockperPerformance);
		aggregationUtil.getKPINameIDFormulaMapping(Kpis);
		assertNotNull(aggregationUtil.nameFormulaMapping);
	}

	@Test
	public void testGenerateKpiFormula() throws JobExecutionException {
		aggregationUtil.jobName = "Perf_CEI_SMS_1_1_HOUR_AggregateJob";
		Collection<PerfUsageSpecRel> tgtPerfSpecRels = new ArrayList<>();
		PerfUsageSpecRel perfUsageSpecRel = new PerfUsageSpecRel();
		perfUsageSpecRel.setId(1);
		tgtPerfSpecRels.add(perfUsageSpecRel);
		PerformanceSpecification testPs = new PerformanceSpecification();
		testPs.setVersion("111");
		testPs.setId(123);
		testPs.setSpecId("abc");
		perfUsageSpecRel.setPerfspecidS(testPs);
		testPs.setTgtPerfUsageSpecRels(tgtPerfSpecRels);
		when(perfSpecQuery.retrieveLatestDeployedSpec("CEI_SMS_1"))
				.thenReturn(testPs);
		when(perfSpecQuery.retrieve(Mockito.anyString(), Mockito.anyString()))
				.thenReturn(testPs);
		List<String> Kpis = new ArrayList<>();
		Kpis.add("VOICE_VKPI");
		PerformanceIndicatorSpec performanceIndicatorSpec = new PerformanceIndicatorSpec();
		performanceIndicatorSpec.setIndicatorspecId("specId");
		performanceIndicatorSpec.setIndicatorcategory("x");
		performanceIndicatorSpec.setPiSpecName("CEI_INDEX");
		List<PerfSpecMeasUsing> list = new ArrayList<>();
		PerformanceSpecInterval performanceSpecInterval = new PerformanceSpecInterval();
		performanceSpecInterval.setSpecIntervalID("HOUR");
		PerfSpecMeasUsing perfSpecMeasUsing = new PerfSpecMeasUsing();
		perfSpecMeasUsing.setId(222);
		perfSpecMeasUsing.setPerfindicatorspecid(performanceIndicatorSpec);
		perfSpecMeasUsing.setIntervalid(performanceSpecInterval);
		perfSpecMeasUsing.setPerfindicatorspecid(mockPerformanceIndicatorSpec);
		list.add(perfSpecMeasUsing);
		testPs.setPerfSpecMeasUsingCollection(list);
		when(kpiQuery.retrieve(performanceIndicatorSpec.getIndicatorspecId(),
				null)).thenReturn(performanceIndicatorSpec);
		when(performanceIndicatorSpec.getPiSpecName()).thenReturn("VOICE_VKPI");
		PerfSpecAttributesUse perfSpec1 = new PerfSpecAttributesUse();
		CharacteristicSpecification charaSpecId = new CharacteristicSpecification();
		performanceIndicatorSpec.setId(1);
		performanceIndicatorSpec.setSpecId("cde");
		charaSpecId.setId(1);
		charaSpecId.setSpecCharId("cde");
		charaSpecId.setSpecId("jki");
		perfSpec1.setId(1);
		perfSpec1.setIntervalid(performanceSpecInterval);
		perfSpec1.setCharaSpecId(charaSpecId);
		perfSpec1.setPerfindicatorspecid(performanceIndicatorSpec);
		perfSpec1.setPerfspecid(testPs);
		List<PerfSpecAttributesUse> perfSpecAttributesUse = new ArrayList<>();
		perfSpecAttributesUse.add(perfSpec1);
		testPs.setPerfSpecAttributesUses(perfSpecAttributesUse);
		aggregationUtil.getKPINameIDFormulaMapping(Kpis);
		Map<String, Object> nameFormulaMapping = aggregationUtil.nameFormulaMapping;
		assertNotNull(nameFormulaMapping);
		assertEquals("CEI_INDEX",
