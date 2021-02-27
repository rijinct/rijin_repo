
package com.project.rithomas.jobexecution.generator.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperties;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;

public class FormulaGeneratorUtilTest {

	FormulaGeneratorUtil formulaGeneratorUtil = new FormulaGeneratorUtil();

	String derivationAlgorithm;

	List<PerfIndiSpecFormulaProperty> propertiesList = new ArrayList<PerfIndiSpecFormulaProperty>();

	PerfIndiSpecFormulaProperty targetPerformanceIndiSpecEntity;

	PerformanceIndicatorSpec performanceIndicatorSpecificatiIndicatorSpec = new PerformanceIndicatorSpec();

	List<PerfIndiSpecFormulaProperty> properties = new ArrayList<PerfIndiSpecFormulaProperty>();

	@Before
	public void setUp() throws Exception {
		derivationAlgorithm = new String("<Formula ref=\"formula\">\r\n"
				+ "			<Properties>\r\n"
				+ "				<Property name=\"TYPE\" value=\"GET_XY\"/>\r\n"
				+ "				<Property name=\"USAGE_ENABLE\" value=\"yes\"/>\r\n"
				+ "				<Property name=\"KPI\" value=\"VOLTE_CONNECTIVITY_INDEX\"/>\r\n"
				+ "				<Property name=\"VOLTE_CONNECTIVITY_INDEX$Weight\" value=\"1\"/>\r\n"
				+ "				<Property name=\"VOLTE_CONNECTIVITY_INDEX$Contrib\" value=\"yes\"/>\r\n"
				+ "				<Property name=\"VOLTE_CONNECTIVITY_INDEX$WeightedInt\" value=\"no\"/>\r\n"
				+ "				<Property name=\"KPI\" value=\"VOLTE_USAGE_INDEX\"/>\r\n"
				+ "				<Property name=\"VOLTE_USAGE_INDEX$Weight\" value=\"1\"/>\r\n"
				+ "				<Property name=\"VOLTE_USAGE_INDEX$Contrib\" value=\"yes\"/>\r\n"
				+ "				<Property name=\"VOLTE_USAGE_INDEX$WeightedInt\" value=\"no\"/>\r\n"
				+ "			</Properties>\r\n" + "		<ID>LOGISTIC</ID>\r\n"
				+ "</Formula>");
		getTargetPerfIndiSpec(propertiesList, "TYPE", "GET_XY");
		getTargetPerfIndiSpec(propertiesList, "SUBTYPE", "DIFF_DENOM");
		getTargetPerfIndiSpec(propertiesList, "KPI", "SMS_RECEIVE_FAILURE_X");
		getTargetPerfIndiSpec(propertiesList, "SMS_RECEIVE_FAILURE_X$Weight",
				"20");
		getTargetPerfIndiSpec(propertiesList, "SMS_RECEIVE_FAILURE_X$GetVal",
				"SMS_RECEIVE_FAILURE_X");
		getTargetPerfIndiSpec(propertiesList, "KPI", "SMS_RECEIVE_SUCCESS_X");
		getTargetPerfIndiSpec(propertiesList, "SMS_RECEIVE_SUCCESS_X$GetVal",
				"SMS_RECEIVE_FAILURE_X");
		getTargetPerfIndiSpec(propertiesList, "SMS_RECEIVE_SUCCESS_X$Weight",
				"20");
		getTargetPerfIndiSpec(propertiesList, "DEFAULT", "null");
		performanceIndicatorSpecificatiIndicatorSpec
				.setIndicatorspecId("SMS_INDEX_ID");
		performanceIndicatorSpecificatiIndicatorSpec.setPiSpecName("SMS_INDEX");
		performanceIndicatorSpecificatiIndicatorSpec
				.setDerivationmethod("$formula$");
	}

	private void getTargetPerfIndiSpec(
			List<PerfIndiSpecFormulaProperty> propertiesList, String propName,
			String propValue) {
		targetPerformanceIndiSpecEntity = new PerfIndiSpecFormulaProperty();
		targetPerformanceIndiSpecEntity.setpropertyName(propName);
		targetPerformanceIndiSpecEntity.setPropertyValue(propValue);
		propertiesList.add(targetPerformanceIndiSpecEntity);
	}

	@Test
	public void testIsUserDefinedFormula() {
		assertTrue(FormulaGeneratorUtil.isUserDefinedFormula("$formula$",
				derivationAlgorithm));
	}

	@Test
	public void testRetrievePerfIndiSpecFormula() {
		assertEquals("LOGISTIC",
				FormulaGeneratorUtil
						.retrievePerfIndiSpecFormula(derivationAlgorithm).get(0)
						.getFormulaID());
	}

	@Test
	public void testGetPerfIndiSpecFormulaPropertiesList() {
		performanceIndicatorSpecificatiIndicatorSpec
				.setDerivationalgorithm(derivationAlgorithm);
		assertEquals("GET_XY", FormulaGeneratorUtil
				.getPerfIndiSpecFormulaPropertiesList(
						performanceIndicatorSpecificatiIndicatorSpec)
				.get(0).getPerfIndiSpecFormulaProperty().get(0)
				.getPropertyValue());
	}

	@Test
	public void testGetDefaultPerfIndiSpecFormulaProperties() {
		PerfIndiSpecFormulaProperties p = FormulaGeneratorUtil
				.getDefaultPerfIndiSpecFormulaProperties(
						getFormulaProperties());
		assertEquals("KPI",
				p.getPerfIndiSpecFormulaProperty().get(0).getpropertyName());
	}

	private List<PerfIndiSpecFormulaProperties> getFormulaProperties() {
		List<PerfIndiSpecFormulaProperties> formulaPropList = new ArrayList<>();
		List<PerfIndiSpecFormulaProperty> perfIndiSpecFormulaProperty1 = new ArrayList<>();
		PerfIndiSpecFormulaProperty formulaProp = new PerfIndiSpecFormulaProperty();
		formulaProp.setpropertyName("KPI");
		formulaProp.setPropertyValue("DROPPEDCALLS");
		perfIndiSpecFormulaProperty1.add(formulaProp);
		PerfIndiSpecFormulaProperties formulaProps = new PerfIndiSpecFormulaProperties();
		formulaProps
				.setPerfIndiSpecFormulaProperty(perfIndiSpecFormulaProperty1);
		formulaPropList.add(formulaProps);
		return formulaPropList;
	}
}
