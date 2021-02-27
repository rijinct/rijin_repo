
package com.project.rithomas.jobexecution.common.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.CacheStoreMode;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.model.performance.KPIFormulaRel;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUse;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUseComparator;
import com.project.rithomas.sdk.model.performance.PerfSpecMeasUsing;
import com.project.rithomas.sdk.model.performance.PerfUsageSpecRel;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpecSequenceComparator;
import com.project.rithomas.sdk.model.performance.PerformanceSpecInterval;
import com.project.rithomas.sdk.model.performance.PerformanceSpecification;
import com.project.rithomas.sdk.model.performance.query.PerformanceIndicatorSpecQuery;
import com.project.rithomas.sdk.model.performance.query.PerformanceSpecIntervalQuery;
import com.project.rithomas.sdk.model.performance.query.PerformanceSpecificationQuery;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

public class CommonAggregationUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CommonAggregationUtil.class);

		Matcher matcher = pattern.matcher(sql);
		while (matcher.find()) {
			String subStr = matcher.group();
			LOGGER.debug("subStr : {}", subStr);
			if (!subStr.isEmpty() && !placeHolders.contains(subStr)) {
				placeHolders.add(subStr);
			}
		}
		sql = replaceOtherPlaceHolders(jobName, sql, placeHolders);
		matcher = pattern.matcher(sql);
		while (matcher.find()) {
			String subStr = matcher.group();
			if (subStr.contains(JobExecutionContext.COEFFICIENT_PREFIX)
					&& !coefficientPlaceHolders.contains(subStr)) {
				coefficientPlaceHolders.add(subStr);
			} else if (subStr
					.contains(JobExecutionContext.US_BASE_WEIGHT_PREFIX)) {
				usageBasedKpiPlaceHolders.add(subStr);
			}
		}
		LOGGER.debug(
				"usageBasedKpiPlaceHolders : {}, coefficientPlaceHolders : {}, placeHolders : {}",
				usageBasedKpiPlaceHolders, coefficientPlaceHolders,
				placeHolders);
		sql = CoefficientReplacementUtil.replaceCoefficientPlaceholders(sql,
				coefficientPlaceHolders);
		sql = UsagebasedWeightKpiUtil.replaceKpiPlaceholders(sql, context,
				usageBasedKpiPlaceHolders);
		return sql;
	}

	private static String replaceOtherPlaceHolders(String jobName, String sql,
			List<String> placeHolders) {
		List<String> replacedKPIPlaceHolders = new ArrayList<String>();
		if (placeHolders != null && !placeHolders.isEmpty()) {
			for (String placeHolderObj : placeHolders) {
				replacedKPIPlaceHolders
						.add(placeHolderObj.replaceAll(HASH_ENCLOSER, ""));
			}
		}
		Map<String, Object> nameFormulaMapping = getKPINameIDFormulaMapping(
				jobName, replacedKPIPlaceHolders);
		for (Entry<String, Object> nameFormulaObj : nameFormulaMapping
				.entrySet()) {
			LOGGER.debug("key : {}, value : {}", nameFormulaObj.getKey(),
					nameFormulaObj.getValue());
			if (nameFormulaObj.getValue() != null) {
				sql = sql.replace(nameFormulaObj.getKey(),
						(String) nameFormulaObj.getValue());
			}
		}
		LOGGER.debug("SQL After replacing KPI related placeholders : {} ", sql);
		return sql;
	}

	public static Map<String, Object> getKPINameIDFormulaMapping(String jobName,
			List<String> kpis) {
		String jobId = jobName.replace("Perf_", "").replace("_AggregateJob", "")
				.replace("_ReaggregateJob", "");
		List<String> items = Arrays.asList(jobId.split("_"));
		String interval = items.get(items.size() - 1);
		String specId = jobId.replace("_" + 1 + "_" + interval, "");
		PerformanceSpecificationQuery perfSpecQuery = new PerformanceSpecificationQuery();
		PerformanceSpecification latestPerfSpec = (PerformanceSpecification) perfSpecQuery
				.retrieveLatestDeployedSpec(specId);
		LOGGER.debug("getKPINameIDFormulaMapping version : {} ",
				latestPerfSpec.getVersion());
		return getKPINameFormulaMapping(specId, latestPerfSpec.getVersion(),
				interval, kpis);
	}

	private static Map<String, Object> getKPINameFormulaMapping(String specId,
			String version, String interval, List<String> kpis) {
		LOGGER.debug(
				"Checking for user defined kpi formula for id: {}, version: {}, interval: {}",
				new Object[] { specId, version, interval });
		Map<String, Object> nameFormulaMapping = new HashMap<String, Object>();
		Map<String, String> currentPSKPIIDNameMapping = new HashMap<String, String>();
		Map<String, String> srcTransPSKPIIDNameMapping = new HashMap<String, String>();
		Map<String, String> srcNonTransPSKPIIDNameMapping = new HashMap<String, String>();
		PerformanceSpecificationQuery perfSpecQuery = new PerformanceSpecificationQuery();
		PerformanceSpecification perfSpec = (PerformanceSpecification) perfSpecQuery
				.retrieve(specId, version);
		nameFormulaMapping.putAll(getPerfSpecKPIMapping(perfSpec, interval,
				kpis, currentPSKPIIDNameMapping));
		if (perfSpec != null) {
			Collection<PerfUsageSpecRel> rels = perfSpec
					.getTgtPerfUsageSpecRels();
			for (PerfUsageSpecRel rel : rels) {
				if (rel.getPerfspecidS() != null) {
					if (rel.getPerfspecidS().isTransient() != null
							&& rel.getPerfspecidS().isTransient()) {
						LOGGER.debug(
								"Adding the KPIs from source transient specification {} also to the map",
								rel.getPerfspecidS());
						nameFormulaMapping.putAll(getPerfSpecKPIMapping(
								rel.getPerfspecidS(), interval, kpis,
								srcTransPSKPIIDNameMapping));
					} else {
						nameFormulaMapping.putAll(getPerfSpecKPIMapping(
								rel.getPerfspecidS(), interval, kpis,
								srcNonTransPSKPIIDNameMapping));
					}
				}
			}
		}
		getFormulaForAdditionalKPIs(kpis, nameFormulaMapping,
				currentPSKPIIDNameMapping, srcTransPSKPIIDNameMapping,
				srcNonTransPSKPIIDNameMapping, interval);
		return nameFormulaMapping;
	}

	private static void getFormulaForAdditionalKPIs(List<String> kpis,
			Map<String, Object> nameFormulaMapping,
			Map<String, String> currentPSKPIIDNameMapping,
			Map<String, String> srcTransPSKPIIDNameMapping,
			Map<String, String> srcNonTransPSKPIIDNameMapping,
			String interval) {
		if (kpis != null && !kpis.isEmpty()) {
			for (String kpiObj : kpis) {
				if (kpiObj.contains(PerfIndiSpecFormulaConstants.INT_SUFFIX)) {
					checkKPIinCurrentSrcTransAndSrcPS(nameFormulaMapping,
							currentPSKPIIDNameMapping,
							srcTransPSKPIIDNameMapping,
							srcNonTransPSKPIIDNameMapping, kpiObj,
							PerfIndiSpecFormulaConstants.INT_SUFFIX, interval);
				} else if (kpiObj
						.contains(PerfIndiSpecFormulaConstants.BUCKET_SUFFIX)) {
					checkKPIinCurrentSrcTransAndSrcPS(nameFormulaMapping,
							currentPSKPIIDNameMapping,
							srcTransPSKPIIDNameMapping,
							srcNonTransPSKPIIDNameMapping, kpiObj,
							PerfIndiSpecFormulaConstants.BUCKET_SUFFIX,
							interval);
				}
			}
		}
	}

	private static void checkKPIinCurrentSrcTransAndSrcPS(
			Map<String, Object> nameFormulaMapping,
			Map<String, String> currentPSKPIIDNameMapping,
			Map<String, String> srcTransPSKPIIDNameMapping,
			Map<String, String> srcNonTransPSKPIIDNameMapping, String kpiObj,
			String suffix, String interval) {
		getNameFormulaMappingForAdditionalKPIs(nameFormulaMapping,
				currentPSKPIIDNameMapping, kpiObj, suffix, interval);
		getNameFormulaMappingForAdditionalKPIs(nameFormulaMapping,
				srcTransPSKPIIDNameMapping, kpiObj, suffix, interval);
		getNameFormulaMappingForAdditionalKPIs(nameFormulaMapping,
				srcNonTransPSKPIIDNameMapping, kpiObj, suffix, interval);
	}

	private static void getNameFormulaMappingForAdditionalKPIs(
			Map<String, Object> nameFormulaMapping,
			Map<String, String> perfSpecKPIIDNameMapping, String kpiObj,
			String suffix, String interval) {
		String kpiObjName = kpiObj.replace(suffix, "");
		LOGGER.debug("kpiObjName : {}, ID : {}", kpiObjName,
				perfSpecKPIIDNameMapping.get(kpiObjName));
		if (perfSpecKPIIDNameMapping.containsKey(kpiObjName)) {
			PerformanceSpecIntervalQuery performanceSpecIntervalQuery = new PerformanceSpecIntervalQuery();
			PerformanceSpecInterval performanceSpecInterval = (PerformanceSpecInterval) performanceSpecIntervalQuery
					.retrieve(interval, null);
			String hashEnclosedKpi = HASH_ENCLOSER + kpiObj + HASH_ENCLOSER;
			if (!nameFormulaMapping.containsKey(hashEnclosedKpi)) {
				nameFormulaMapping.put(hashEnclosedKpi, getFormula(
						new StringBuilder(
								perfSpecKPIIDNameMapping.get(kpiObjName))
										.append(suffix).toString(),
						kpiObj, performanceSpecInterval.getId()));
			}
		}
	}

	private static Map<String, Object> getPerfSpecKPIMapping(
			PerformanceSpecification perfSpec, String interval,
			List<String> kpis, Map<String, String> kpiIDNameMap) {
		Map<String, Object> nameFormulaMapping = new HashMap<String, Object>();
		List<PerfSpecMeasUsing> kpiRels = new ArrayList<PerfSpecMeasUsing>(
				perfSpec.getPerfSpecsMeasUsing());
		if (!kpiRels.isEmpty()) {
			getFormulaMappingForSpecMeasUse(interval, kpis, kpiIDNameMap,
					nameFormulaMapping, kpiRels);
		}
		LOGGER.debug("The value of perf Spec :");
		Collection<PerfSpecAttributesUse> performanceSpecificationsAttributesList = perfSpec
				.getPerfSpecAttributesUses();
		List<PerfSpecAttributesUse> perfSpecAttrUseList = new ArrayList<PerfSpecAttributesUse>(
				performanceSpecificationsAttributesList);
		Collections.sort(perfSpecAttrUseList,
				new PerfSpecAttributesUseComparator());
		for (PerfSpecAttributesUse kpiRel : perfSpecAttrUseList) {
			if (kpiRel.isKpi() && interval
					.equals(kpiRel.getIntervalid().getSpecIntervalID())) {
				getKpiFormula(kpis, kpiIDNameMap, nameFormulaMapping, kpiRel);
			}
		}
		return nameFormulaMapping;
	}

	private static void getFormulaMappingForSpecMeasUse(String interval,
			List<String> kpis, Map<String, String> kpiIDNameMap,
			Map<String, Object> nameFormulaMapping,
			List<PerfSpecMeasUsing> kpiRels) {
		LOGGER.debug("The value of PerfSpecMeasUsingList" + kpiRels.toString());
		Collections.sort(kpiRels,
				new PerformanceIndicatorSpecSequenceComparator());
		for (PerfSpecMeasUsing kpiRel : kpiRels) {
			if (interval.equals(kpiRel.getIntervalid().getSpecIntervalID())) {
				PerformanceIndicatorSpec kpi = kpiRel.getPerfindicatorspecid();
				encloseKpiWithHash(kpis, kpiIDNameMap, nameFormulaMapping,
						kpiRel, kpi);
			}
		}
	}

	private static void encloseKpiWithHash(List<String> kpis,
			Map<String, String> kpiIDNameMap,
			Map<String, Object> nameFormulaMapping, PerfSpecMeasUsing kpiRel,
			PerformanceIndicatorSpec kpi) {
		PerformanceIndicatorSpecQuery kpiQuery = new PerformanceIndicatorSpecQuery();
		kpi = (PerformanceIndicatorSpec) kpiQuery
				.retrieve(kpi.getIndicatorspecId(), null);
		if (!(GeneratorWorkFlowContext.CARRYFWD)
				.equals(kpi.getIndicatorcategory())) {
			kpiIDNameMap.put(kpi.getPiSpecName(), kpi.getIndicatorspecId());
			if (kpis.contains(kpi.getPiSpecName())) {
				kpis.remove(kpi.getPiSpecName());
				String kpiFormula = getFormula(kpi.getIndicatorspecId(),
						kpi.getPiSpecName(), kpiRel.getIntervalid().getId());
				if (kpiFormula == null) {
					kpiFormula = kpi.getDerivationmethod().replace("''", "'");
				}
				nameFormulaMapping.put(
						HASH_ENCLOSER + kpi.getPiSpecName() + HASH_ENCLOSER,
						kpiFormula);
			}
		}
	}

	private static void getKpiFormula(List<String> kpis,
			Map<String, String> kpiIDNameMap,
			Map<String, Object> nameFormulaMapping,
			PerfSpecAttributesUse kpiRel) {
		if (kpiRel.isKpi()) {
			PerformanceIndicatorSpec kpi = kpiRel.getPerfindicatorspecid();
			PerformanceIndicatorSpecQuery kpiQuery = new PerformanceIndicatorSpecQuery();
			kpi = (PerformanceIndicatorSpec) kpiQuery
					.retrieve(kpi.getIndicatorspecId(), null);
			if (!(GeneratorWorkFlowContext.CARRYFWD)
					.equals(kpi.getIndicatorcategory())) {
				kpiIDNameMap.put(kpi.getPiSpecName(), kpi.getIndicatorspecId());
				prepareMap(kpis, nameFormulaMapping, kpiRel, kpi);
			}
		}
	}

	private static void prepareMap(List<String> kpis,
			Map<String, Object> nameFormulaMapping,
			PerfSpecAttributesUse kpiRel, PerformanceIndicatorSpec kpi) {
		if (kpis.contains(kpi.getPiSpecName())) {
			kpis.remove(kpi.getPiSpecName());
			String kpiFormula = getFormula(kpi.getIndicatorspecId(),
					kpi.getPiSpecName(), kpiRel.getIntervalid().getId());
			if (kpiFormula == null) {
				kpiFormula = kpi.getDerivationmethod().replace("''", "'");
			}
			nameFormulaMapping.put(
					HASH_ENCLOSER + kpi.getPiSpecName() + HASH_ENCLOSER,
					kpiFormula);
		}
	}

	private static String getFormula(String kpiID, String kpiName,
			Integer interval) {
		EntityManager entityManager = null;
		String formula = null;
		try {
			entityManager = ModelResources.getEntityManager();
			TypedQuery<KPIFormulaRel> query = entityManager
					.createNamedQuery(KPIFormulaRel.FIND_BY_ID_INTERVAL,
							KPIFormulaRel.class)
					.setParameter(KPIFormulaRel.KPI_ID, kpiID)
					.setParameter(KPIFormulaRel.INTERVAL_ID, interval);
			query.setHint("javax.persistence.cacheStoreMode",
					CacheStoreMode.REFRESH);
			List<KPIFormulaRel> formulaRels = query.getResultList();
			if (formulaRels != null && !formulaRels.isEmpty()) {
				KPIFormulaRel formulaRel = formulaRels.get(0);
				formula = formulaRel.getFormula();
			}
		} finally {
			ModelResources.releaseEntityManager(entityManager);
		}
		LOGGER.debug("KPI ID : {}, KPI: {}, Formula: {}", kpiID, kpiName,
				formula);
		return formula;
	}

	public static List<String> setQueryHintForParallelInsert(String sql) {
		List<String> hiveSettings = new ArrayList<String>();
		if (sql.contains(JobExecutionContext.SUBPARTITION_VALUES)) {
			String hint = "SET hive.enforce.bucketing=true";
			hiveSettings.add(hint);
			LOGGER.info(
					"Parallel insert query enabled.Adding bucketing hint in list of hive hints to be executed:{}",
					hint);
		}
		return hiveSettings;
	}
	
	public static void constructMapForDependentQSJob(WorkFlowContext context, Long lb,
			Long ub) {
		Map<String, Object> mapForDependentJobs = new HashMap<>();
		mapForDependentJobs.put(JobExecutionContext.QS_LOWER_BOUND, lb);
		mapForDependentJobs.put(JobExecutionContext.QS_UPPER_BOUND, ub);
		mapForDependentJobs.put(JobExecutionContext.SOURCE,
				context.getProperty(JobExecutionContext.TARGET));
		mapForDependentJobs.put(JobExecutionContext.SOURCEJOBTYPE,
				context.getProperty(JobExecutionContext.JOBTYPE));
		context.setProperty(JobExecutionContext.MAP_FOR_DEPENDENT_JOBS,
				mapForDependentJobs);
		LOGGER.debug(
				"Adding the source table name : {} and source job type : {}",
				context.getProperty(JobExecutionContext.TARGET),
				context.getProperty(JobExecutionContext.JOBTYPE));
	}
}
