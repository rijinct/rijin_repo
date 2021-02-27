
package com.project.rithomas.jobexecution.generator.util;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormula;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperties;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulae;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;
import com.thoughtworks.xstream.XStream;

public class UserDefinedKPIGeneratorUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(UserDefinedKPIGeneratorUtil.class);

	public static boolean isValidLogisticKPI(
			PerformanceIndicatorSpec perfindicatorspecid) {
		boolean isValid = false;
		List<PerfIndiSpecFormulaProperty> properties = getPropertiesIfUserDefined(
				perfindicatorspecid);
		if (properties != null) {
			isValid = true;
			for (PerfIndiSpecFormulaProperty property : properties) {
				if (equalsIgnoreCase(PerfIndiSpecFormulaConstants.GET_WINT,
						property.getPropertyValue())
						|| equalsIgnoreCase(
								PerfIndiSpecFormulaConstants.GET_CONTRI,
								property.getPropertyValue())
						|| equalsIgnoreCase(
								PerfIndiSpecFormulaConstants.GET_USAGE_BASED_KPI,
								property.getPropertyValue())
						|| (equalsIgnoreCase(
								PerfIndiSpecFormulaConstants.RETURN_TYPE,
								property.getpropertyName())
								&& equalsIgnoreCase(
										PerfIndiSpecFormulaConstants.GET_X,
										property.getPropertyValue()))) {
					isValid = false;
					break;
				}
			}
		}
		LOGGER.debug("{} isvalidLogistic kpi:{}", perfindicatorspecid, isValid);
		return isValid;
	}

	public static List<PerfIndiSpecFormulaProperty> getPropertiesIfUserDefined(
			PerformanceIndicatorSpec perfindicatorspecid) {
		List<PerfIndiSpecFormulaProperty> properties = null;
		if (islogisticKPI(perfindicatorspecid)) {
			List<PerfIndiSpecFormula> perfIndiSpecFormulas = getperfIndiSpecFormulas(
					perfindicatorspecid);
			List<PerfIndiSpecFormulaProperties> perfIndiSpecFormulaPropertiesList = perfIndiSpecFormulas
					.get(0).getPerfIndiSpecFormulaProperties();
			for (PerfIndiSpecFormulaProperties perfIndiSpecFormulaPropObj : perfIndiSpecFormulaPropertiesList) {
				String formulaType = perfIndiSpecFormulaPropObj.getType();
				if (formulaType == null || formulaType.isEmpty()
						|| PerfIndiSpecFormulaConstants.DEFAULT
								.equalsIgnoreCase(formulaType)) {
					properties = perfIndiSpecFormulaPropObj
							.getPerfIndiSpecFormulaProperty();
				}
			}
		}
		return properties;
	}

	public static boolean islogisticKPI(
			PerformanceIndicatorSpec perfindicatorspecid) {
		boolean isLogistic = false;
		List<PerfIndiSpecFormula> perfIndiSpecFormulas = getperfIndiSpecFormulas(
				perfindicatorspecid);
		if (isNotEmpty(perfIndiSpecFormulas)
				&& isFormulaIDLogistic(perfIndiSpecFormulas)) {
			isLogistic = true;
		}
		return isLogistic;
	}

	private static boolean isFormulaIDLogistic(
			List<PerfIndiSpecFormula> perfIndiSpecFormulas) {
		return equalsIgnoreCase(PerfIndiSpecFormulaConstants.LOGISTIC,
				perfIndiSpecFormulas.get(0).getFormulaID());
	}

	public static List<PerfIndiSpecFormula> getperfIndiSpecFormulas(
			PerformanceIndicatorSpec perfindicatorspecid) {
		XStream xstream = new XStream();
		xstream.processAnnotations(PerfIndiSpecFormula.class);
		xstream.processAnnotations(PerfIndiSpecFormulae.class);
		String derivationAlgorithm = perfindicatorspecid
				.getDerivationalgorithm();
		derivationAlgorithm = "<Formulae>" + derivationAlgorithm
				+ "</Formulae>";
		PerfIndiSpecFormulae perfIndiSpecFormulae = (PerfIndiSpecFormulae) xstream
				.fromXML(derivationAlgorithm);
		return perfIndiSpecFormulae.getPerfIndiSpecFormula();
	}

	public static boolean isUserDefinedKPI(
			PerformanceIndicatorSpec perfindicatorspecid) {
		boolean isUserDefined = false;
		List<PerfIndiSpecFormula> perfIndiSpecFormulas = getperfIndiSpecFormulas(
				perfindicatorspecid);
		if (perfIndiSpecFormulas != null && !perfIndiSpecFormulas.isEmpty()) {
			String formula = perfIndiSpecFormulas.get(0).getFormulaID();
			if (PerfIndiSpecFormulaConstants.LOGISTIC.equalsIgnoreCase(formula)
					|| PerfIndiSpecFormulaConstants.WEIGHTED_AVERAGE
							.equalsIgnoreCase(formula)) {
				isUserDefined = true;
			}
		}
		return isUserDefined;
	}

	public static boolean isWeightedAverageKPI(
			PerformanceIndicatorSpec perfindicatorspecid) {
		boolean isWeightedAverage = false;
		List<PerfIndiSpecFormula> perfIndiSpecFormulas = getperfIndiSpecFormulas(
				perfindicatorspecid);
		if (perfIndiSpecFormulas != null && !perfIndiSpecFormulas.isEmpty()) {
			if (PerfIndiSpecFormulaConstants.WEIGHTED_AVERAGE.equalsIgnoreCase(
					perfIndiSpecFormulas.get(0).getFormulaID())) {
				isWeightedAverage = true;
			}
		}
		return isWeightedAverage;
	}
}
