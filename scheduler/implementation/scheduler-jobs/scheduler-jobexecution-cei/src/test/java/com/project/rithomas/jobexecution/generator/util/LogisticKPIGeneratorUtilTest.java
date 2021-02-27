
package com.project.rithomas.jobexecution.generator.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;

public class LogisticKPIGeneratorUtilTest {

	UserDefinedKPIGeneratorUtil logisticKPIgeneratorUtil = new UserDefinedKPIGeneratorUtil();

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testIsValidLogisticKPI() {
		PerformanceIndicatorSpec perfindicatorspecid = new PerformanceIndicatorSpec();
		perfindicatorspecid.setDerivationalgorithm(
				"<Formula ref=\"formula\">\r\n" + "			<Properties>\r\n"
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
						+ "			</Properties>\r\n"
						+ "		<ID>LOGISTIC</ID>\r\n" + "</Formula>");
		perfindicatorspecid.setDerivationmethod("$formula$");
		assertTrue(logisticKPIgeneratorUtil
				.isValidLogisticKPI(perfindicatorspecid));
		perfindicatorspecid = new PerformanceIndicatorSpec();
		perfindicatorspecid.setDerivationmethod("(case\r\n"
