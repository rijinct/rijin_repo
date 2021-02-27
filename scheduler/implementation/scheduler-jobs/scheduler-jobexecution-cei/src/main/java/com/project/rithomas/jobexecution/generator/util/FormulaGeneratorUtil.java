
package com.project.rithomas.jobexecution.generator.util;

import static org.apache.commons.lang.StringUtils.isNotEmpty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUse;
import com.project.rithomas.sdk.model.performance.PerfUsageSpecRel;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.PerformanceSpecification;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormula;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperties;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulae;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;
import com.thoughtworks.xstream.XStream;

public class FormulaGeneratorUtil {

	public static boolean isUserDefinedFormula(String derivationMethod,
			String derivationAlgorithm) {
		boolean doesContainFormulaRef = false;
		if (derivationMethod != null && derivationMethod.contains("$")
				&& derivationAlgorithm != null
				&& derivationAlgorithm.contains("\"")
				&& derivationAlgorithm.split("\"")[1]
						.equals(derivationMethod.split("\\$")[1])) {
			doesContainFormulaRef = true;
		} else if ("null".equals(derivationMethod)
				&& derivationAlgorithm != null
				&& derivationAlgorithm.contains("<Formula")) {
			doesContainFormulaRef = true;
		}
		return doesContainFormulaRef;
	}

	public static List<PerfSpecAttributesUse> getAllSrcKPIs(
			PerformanceSpecification performanceSpecification) {
		List<PerfSpecAttributesUse> perfSpecAttributesUse = new ArrayList<PerfSpecAttributesUse>();
		Collection<PerfUsageSpecRel> rels = performanceSpecification
				.getTgtPerfUsageSpecRels();
		for (PerfUsageSpecRel rel : rels) {
			if (rel.getPerfspecidS() != null) {
				perfSpecAttributesUse.addAll(
						rel.getPerfspecidS().getPerfSpecAttributesUses());
			}
		}
		return perfSpecAttributesUse;
	}

	public static List<PerfIndiSpecFormula> retrievePerfIndiSpecFormula(
			String derivationAlgorithm) {
		List<PerfIndiSpecFormula> perfIndiSpecFormula = new ArrayList<>();
		XStream xstream = new XStream();
		xstream.processAnnotations(PerfIndiSpecFormula.class);
		xstream.processAnnotations(PerfIndiSpecFormulae.class);
		if (derivationAlgorithm != null
				&& derivationAlgorithm.trim().contains("<Formula")
				&& derivationAlgorithm.trim().contains("</Formula>")) {
			derivationAlgorithm = "<Formulae>" + derivationAlgorithm
					+ "</Formulae>";
			PerfIndiSpecFormulae perfIndiSpecFormulae = (PerfIndiSpecFormulae) xstream
					.fromXML(derivationAlgorithm);
			perfIndiSpecFormula = perfIndiSpecFormulae.getPerfIndiSpecFormula();
		}
		return perfIndiSpecFormula;
	}

	public static List<PerfIndiSpecFormulaProperties> getPerfIndiSpecFormulaPropertiesList(
			PerformanceIndicatorSpec tarPerfIndicSpec) {
		List<PerfIndiSpecFormulaProperties> perfIndiSpecFormulaPropertiesList = null;
		if (isNotEmpty(tarPerfIndicSpec.getDerivationalgorithm())) {
			List<PerfIndiSpecFormula> perfIndiSpecFormulaList = retrievePerfIndiSpecFormula(
					tarPerfIndicSpec.getDerivationalgorithm());
			for (PerfIndiSpecFormula obj : perfIndiSpecFormulaList) {
				perfIndiSpecFormulaPropertiesList = obj
						.getPerfIndiSpecFormulaProperties();
			}
		}
		return perfIndiSpecFormulaPropertiesList;
	}

	public static PerfIndiSpecFormulaProperties getDefaultPerfIndiSpecFormulaProperties(
			List<PerfIndiSpecFormulaProperties> perfIndiSpecFormulaPropertiesList) {
		PerfIndiSpecFormulaProperties defaultPISFormulaProperties = null;
		for (PerfIndiSpecFormulaProperties pisFormulaPropertiesObj : perfIndiSpecFormulaPropertiesList) {
			if (pisFormulaPropertiesObj.getType() == null
					|| PerfIndiSpecFormulaConstants.DEFAULT
							.equals(pisFormulaPropertiesObj.getType())) {
				defaultPISFormulaProperties = pisFormulaPropertiesObj;
				break;
			}
		}
		return defaultPISFormulaProperties;
	}
}
