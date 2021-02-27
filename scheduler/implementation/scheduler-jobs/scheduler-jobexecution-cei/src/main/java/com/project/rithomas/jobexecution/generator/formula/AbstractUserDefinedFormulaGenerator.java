
package com.project.rithomas.jobexecution.generator.formula;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.project.rithomas.jobexecution.generator.FormulaGeneratorException;
import com.project.rithomas.jobexecution.generator.util.FormulaGeneratorUtil;
import com.project.rithomas.jobexecution.generator.util.UserDefinedKPIGeneratorUtil;
import com.project.rithomas.sdk.model.performance.KPIFormulaRel;
import com.project.rithomas.sdk.model.performance.KPIFormulaRelPK;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUse;
import com.project.rithomas.sdk.model.performance.PerfSpecAttributesUseComparator;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.PerformanceSpecInterval;
import com.project.rithomas.sdk.model.performance.PerformanceSpecification;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormula;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperties;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulaProperty;
import com.project.rithomas.sdk.model.performance.formula.PerfIndiSpecFormulae;
import com.project.rithomas.sdk.model.performance.query.PerformanceSpecIntervalQuery;
import com.project.rithomas.sdk.model.performance.query.PerformanceSpecificationQuery;
import com.project.rithomas.sdk.model.utils.PerfIndiSpecFormulaConstants;
import com.project.rithomas.sdk.model.utils.PerfIndicatorCategoryConstants;
import com.thoughtworks.xstream.XStream;

public abstract class AbstractUserDefinedFormulaGenerator
		extends AbstractFormulaGenerator {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(AbstractUserDefinedFormulaGenerator.class);

