
package com.project.rithomas.jobexecution.aggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import com.project.rithomas.sdk.model.performance.Attribute;
import com.project.rithomas.sdk.model.performance.Attributes;
import com.project.rithomas.sdk.model.performance.PerfSpecMeasUsing;
import com.project.rithomas.sdk.model.performance.PerfUsageSpecRel;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.PerformanceSpecInterval;
import com.project.rithomas.sdk.model.performance.PerformanceSpecification;

public class schedulerTestUtil {

	public static PerformanceSpecification getPerfSpecWithSrcPS() {
		PerformanceSpecification performanceSpecification = getSamplePerfSpec(
				"CEI_INDEX", "1", "AGG");
		Collection<PerfUsageSpecRel> tgtPerfUsageSpecRels = new ArrayList<PerfUsageSpecRel>();
		PerfUsageSpecRel perfUsageSpecRel = new PerfUsageSpecRel();
		perfUsageSpecRel
				.setPerfspecidS(getPerformanceSpecification("CEI_SMS", "1"));
		tgtPerfUsageSpecRels.add(perfUsageSpecRel);
		perfUsageSpecRel = new PerfUsageSpecRel();
		PerformanceSpecification ceiVoice = getPerformanceSpecification(
				"CEI_SMS", "1");
		ceiVoice.setTransient(true);
		ceiVoice.setTgtPerfUsageSpecRels(new ArrayList<PerfUsageSpecRel>());
		ceiVoice.setPerfSpecMeasUsingCollection(
				new ArrayList<PerfSpecMeasUsing>());
		perfUsageSpecRel.setPerfspecidS(ceiVoice);
		tgtPerfUsageSpecRels.add(perfUsageSpecRel);
		performanceSpecification.setTgtPerfUsageSpecRels(tgtPerfUsageSpecRels);
		List<PerfSpecMeasUsing> kpiRels = new ArrayList<PerfSpecMeasUsing>();
		kpiRels.add(getPerfSpecMeasUsing(7,
				getPerformanceIndicatorSpec("SMS_CEI_INDEX", "SMS_CEI_INDEX",
						"$formula$", "Number", null),
				getPerformanceSpecInterval("HOUR")));
		kpiRels.add(getPerfSpecMeasUsing(7,
				getPerformanceIndicatorSpec("SMS_CEI_INDEX", "SMS_CEI_INDEX",
						"$formula$", "Number", null),
				getPerformanceSpecInterval("DAY")));
		kpiRels.add(getPerfSpecMeasUsing(8,
				getPerformanceIndicatorSpec("CEI_INDEX", "CEI_INDEX",
						"$formula$", "Number", null),
				getPerformanceSpecInterval("HOUR")));
		kpiRels.add(getPerfSpecMeasUsing(9,
				getPerformanceIndicatorSpec("CEI_INDEX_D1", "CEI_INDEX_D1",
						"NULL", "Number", null),
				getPerformanceSpecInterval("HOUR")));
		kpiRels.add(getPerfSpecMeasUsing(6,
				getPerformanceIndicatorSpec("SMS_CEI_INDEX_D1",
						"SMS_CEI_INDEX_D1", "$formula$", "Number", null),
				getPerformanceSpecInterval("HOUR")));
		kpiRels.add(getPerfSpecMeasUsing(
				5, getPerformanceIndicatorSpec("SMS_VKPI", "SMS_VKPI",
						"$formula$", "Number", null),
				getPerformanceSpecInterval("HOUR")));
		kpiRels.add(getPerfSpecMeasUsing(4,
				getPerformanceIndicatorSpec("SMS_VKPI_D1", "SMS_VKPI_D1",
						"$formula$", "Number", null),
				getPerformanceSpecInterval("HOUR")));
		kpiRels.add(getPerfSpecMeasUsing(3,
				getPerformanceIndicatorSpec("VOICE_CEI_INDEX",
						"VOICE_CEI_INDEX", "$formula$", "Number", null),
				getPerformanceSpecInterval("HOUR")));
		kpiRels.add(getPerfSpecMeasUsing(2,
				getPerformanceIndicatorSpec("VOICE_CEI_INDEX_D1",
						"VOICE_CEI_INDEX_D1", "NULL", "Number", null),
				getPerformanceSpecInterval("HOUR")));
		kpiRels.add(getPerfSpecMeasUsing(1,
				getPerformanceIndicatorSpec("VOICE_VKPI", "VOICE_VKPI",
						"sum(test)", "Number", null),
				getPerformanceSpecInterval("HOUR")));
		performanceSpecification.setPerfSpecMeasUsingCollection(kpiRels);
		return performanceSpecification;
	}

	public static PerformanceSpecification getPerformanceSpecification(
			String specId, String version) {
		PerformanceSpecification perfSpec = new PerformanceSpecification();
		perfSpec.setSpecId(specId);
		perfSpec.setVersion(version);
		return perfSpec;
	}

	public static PerformanceSpecification getSamplePerfSpec(String id,
			String version, String aggType) {
		PerformanceSpecification performanceSpecification = getPerformanceSpecification(
				id, version);
		performanceSpecification.setAggtype(aggType);
		// Setting perf chars list
		List<Attributes> perfCharList = new ArrayList<Attributes>();
		Attributes perfChars = new Attributes();
		perfChars.setSource("PERFORMANCE");
		perfChars.setId("CEI_SMS");
		perfChars.setVersion("1");
		LinkedHashSet<Attribute> characteristicID = new LinkedHashSet<Attribute>();
		characteristicID
				.add(new Attribute(Attribute.TYPE_DIMENSION, "REPORT_TIME"));
		characteristicID.add(new Attribute(Attribute.TYPE_DIMENSION, "IMSI"));
		characteristicID.add(new Attribute(Attribute.TYPE_DIMENSION, "IMEI"));
		perfChars.setAttribute(characteristicID);
		perfCharList.add(perfChars);
		performanceSpecification.setAttributes(perfCharList);
		return performanceSpecification;
	}

	public static PerfSpecMeasUsing getPerfSpecMeasUsing(Integer sequence,
			PerformanceIndicatorSpec perfindicatorspecid,
			PerformanceSpecInterval intervalid) {
		PerfSpecMeasUsing perfSpecMeasUsing = new PerfSpecMeasUsing();
		perfSpecMeasUsing.setSequence(sequence);
		perfSpecMeasUsing.setPerfindicatorspecid(perfindicatorspecid);
		perfSpecMeasUsing.setIntervalid(intervalid);
		return perfSpecMeasUsing;
	}

	public static PerformanceIndicatorSpec getPerformanceIndicatorSpec(
			String kpiId, String kpiName, String derivationMethod,
			String valueType, String groupingExpr) {
		PerformanceIndicatorSpec pis = new PerformanceIndicatorSpec();
		pis.setDerivationmethod(derivationMethod);
		pis.setIndicatorspecId(kpiId);
		pis.setPiSpecName(kpiName);
		pis.setValuetype(valueType);
		pis.setGroupingExpr(groupingExpr);
		return pis;
	}

	public static PerformanceSpecInterval getPerformanceSpecInterval(
			String intervalID) {
		PerformanceSpecInterval perfSpecInterval = new PerformanceSpecInterval();
		perfSpecInterval.setSpecIntervalID(intervalID);
		return perfSpecInterval;
	}
}
