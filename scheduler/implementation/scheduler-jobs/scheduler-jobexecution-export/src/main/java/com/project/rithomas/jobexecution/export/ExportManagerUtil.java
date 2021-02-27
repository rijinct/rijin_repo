
package com.project.rithomas.jobexecution.export;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.nbif.ExportSpecification;
import com.project.rithomas.sdk.model.nbif.ExportSpecificationCharacteristicUse;
import com.project.rithomas.sdk.model.nbif.ExportSpecificationIndicatorRel;
import com.project.rithomas.sdk.model.nbif.ExportSpecificationIndicatorRelComparator;
import com.project.rithomas.sdk.model.nbif.ExportSpecificationRel;
import com.project.rithomas.sdk.model.nbif.query.ExportSpecificationQuery;
import com.project.rithomas.sdk.model.others.ExportSpecCharUseComparator;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUse;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUseComparator;
import com.project.rithomas.sdk.model.performance.PerformanceSpecification;
import com.project.rithomas.sdk.model.performance.query.PerformanceSpecificationQuery;
import com.project.rithomas.sdk.model.utils.ModelUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class ExportManagerUtil {

	static final String TRUE = "true";

	private static final String SUFFIX = "_ExportJob";

	private static final String PREFIX = "Exp_";

	private static final String VERSION = "version";

	private static final String EXPORT_NAME = "export";

	private static final String UNDERSCORE = "_";

	private static final String INTERVAL = "interval";

	public static String getColumnNames(WorkFlowContext context) {
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String objectId = getExportNameVersionOrInterval(jobId, EXPORT_NAME);
		ExportSpecification exportSpecification = getExportSpecification(
				objectId);
		List<ExportSpecificationCharacteristicUse> exportCharSpecList = getExportCharSpecs(
				exportSpecification);
		List<String> exportKPIList = getExportKPIs(exportSpecification,
				getExportNameVersionOrInterval(jobId, INTERVAL));
		Set<String> exportColumns = new LinkedHashSet<>();
		for (ExportSpecificationCharacteristicUse exportCharSpec : exportCharSpecList) {
			exportColumns.add(exportCharSpec.getSpecChar().getName());
		}
		for (String exportKPI : exportKPIList) {
			exportColumns.add(exportKPI);
		}
		return StringUtils.join(exportColumns, ",");
	}

	private static List<String> getExportKPIs(
			ExportSpecification exportSpecification, String interval) {
		List<String> exportKPIHeaderList = new ArrayList<>();
		Collection<ExportSpecificationIndicatorRel> exportKPIs = exportSpecification
				.getExportSpecIndiRels();
		if (CollectionUtils.isNotEmpty(exportKPIs)) {
			List<ExportSpecificationIndicatorRel> exportKPIsList = new ArrayList<>(
					exportKPIs);
			Collections.sort(exportKPIsList,
					new ExportSpecificationIndicatorRelComparator());
			for (ExportSpecificationIndicatorRel exportKPI : exportKPIsList) {
				exportKPIHeaderList
						.add(exportKPI.getPerfIndiSpecId().getPiSpecName());
			}
		} else {
			Collection<ExportSpecificationRel> exportSpecRelsList = exportSpecification
					.getExportSpecRels();
			for (ExportSpecificationRel exportSpecRel : exportSpecRelsList) {
				if (ModelUtil.PERFORMANCE.equalsIgnoreCase(
						exportSpecification.getCharacteristics().getSource())) {
					PerformanceSpecificationQuery performanceSpecificationQuery = new PerformanceSpecificationQuery();
					PerformanceSpecification parentPerfSpec = (PerformanceSpecification) performanceSpecificationQuery
							.retrieve(exportSpecRel.getPerfSpecId().getSpecId(),
									exportSpecRel.getPerfSpecId().getVersion());
					List<PerfSpecAttributesUse> performanceSpecAttrUseList = new ArrayList<>(
							parentPerfSpec.getPerfSpecAttributesUses());
					Collections.sort(performanceSpecAttrUseList,
							new PerfSpecAttributesUseComparator());
					for (PerfSpecAttributesUse perfSpecAttrUse : performanceSpecAttrUseList) {
						if (perfSpecAttrUse.getIntervalid() != null
								&& perfSpecAttrUse.getIntervalid()
										.getSpecIntervalID().equals(interval)
								&& perfSpecAttrUse.isTargetKpi()
								&& !perfSpecAttrUse.getPerfindicatorspecid()
										.isTransient()) {
							exportKPIHeaderList.add(perfSpecAttrUse
									.getPerfindicatorspecid().getPiSpecName());
						}
					}
				}
			}
		}
		return exportKPIHeaderList;
	}

	private static List<ExportSpecificationCharacteristicUse> getExportCharSpecs(
			ExportSpecification exportSpecification) {
		Collection<ExportSpecificationCharacteristicUse> exportCharSpecs = exportSpecification
				.getExportSpecsCharUse();
		List<ExportSpecificationCharacteristicUse> exportCharSpecList = new ArrayList<>();
		for (ExportSpecificationCharacteristicUse exportCharSpec : exportCharSpecs) {
			exportCharSpecList.add(exportCharSpec);
		}
		Collections.sort(exportCharSpecList, new ExportSpecCharUseComparator());
		return exportCharSpecList;
	}

	private static ExportSpecification getExportSpecification(String objectId) {
		ExportSpecificationQuery exportSpecificationQuery = new ExportSpecificationQuery();
		return (ExportSpecification) exportSpecificationQuery
				.retrieveLatest(objectId);
	}

	private static String getExportNameVersionOrInterval(String jobId,
			String type) {
		String object = jobId.replace(PREFIX, "").replace(SUFFIX, "");
		if (INTERVAL.equals(type)) {
			object = object.substring(object.lastIndexOf(UNDERSCORE) + 1);
		} else {
			object = object.substring(0, object.lastIndexOf(UNDERSCORE));
			if (EXPORT_NAME.equals(type)) {
				object = object.substring(0, object.lastIndexOf(UNDERSCORE));
			}
			if (VERSION.equals(type)) {
				object = object.substring(object.lastIndexOf(UNDERSCORE) + 1);
			}
		}
		return object;
	}
}
