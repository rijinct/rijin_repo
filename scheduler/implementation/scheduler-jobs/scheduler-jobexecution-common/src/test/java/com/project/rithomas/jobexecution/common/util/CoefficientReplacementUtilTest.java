
package com.project.rithomas.jobexecution.common.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.sdk.workflow.WorkFlowContext;

@PrepareForTest(value = { CoefficientReplacementUtil.class })
@RunWith(PowerMockRunner.class)
public class CoefficientReplacementUtilTest {

	WorkFlowContext context = new JobExecutionContext();

	List<String> coefficientPlaceHolders = null;

	String sql = null;

	@Before
	public void setUp() throws Exception {
		coefficientPlaceHolders = new ArrayList<>();
