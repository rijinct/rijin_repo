
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.applicability.Applicability;
import com.project.rithomas.formula.exception.InvalidApplicabilityException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ApplicabilityCheckUtil.class)
public class ApplicabilityCheckUtilTest {

	private static final String APPLICABLE = "APPLICABLE";

	private static final String UNAPPLICABLE = "UNAPPLICABLE";

	private static final String APP_1 = "app1";

	private static final String APP_2 = "app2";

	private static final String APP_3 = "app3";

	private static final String DAYS = "1-2";

	private List<Applicability> applicabilities = null;

	private List<Applicability> getAllApplicable()
			throws InvalidApplicabilityException {
		final List<Applicability> allApp = new ArrayList<Applicability>();
		final Applicability app1 = new Applicability(APP_1, APPLICABLE, DAYS,
				"01:15", "11:25");
		final Applicability app2 = new Applicability(APP_2, APPLICABLE, DAYS,
				"01:16", "11:28");
		final Applicability app3 = new Applicability(APP_3, APPLICABLE, DAYS,
				"01:17", "10:30");
		allApp.add(app1);
		allApp.add(app2);
		allApp.add(app3);
		return allApp;
	}

	private List<Applicability> getAllUnApplicable()
			throws InvalidApplicabilityException {
		final List<Applicability> allApp = new ArrayList<Applicability>();
		final Applicability app1 = new Applicability(APP_1, UNAPPLICABLE, DAYS,
				"01:18", "11:25");
		final Applicability app2 = new Applicability("App2", UNAPPLICABLE,
				DAYS, "01:19", "11:28");
		final Applicability app3 = new Applicability("App3", UNAPPLICABLE,
				DAYS, "01:20", "10:30");
		allApp.add(app1);
		allApp.add(app2);
		allApp.add(app3);
		return allApp;
	}

	private List<Applicability> getAllUnApplicableOutsideTimeRange()
			throws InvalidApplicabilityException {
		final List<Applicability> allApp = new ArrayList<Applicability>();
		final Applicability app1 = new Applicability(APP_1, UNAPPLICABLE, DAYS,
				"01:21", "11:20");
		final Applicability app2 = new Applicability(APP_2, UNAPPLICABLE, DAYS,
				"01:22", "11:18");
		final Applicability app3 = new Applicability(APP_3, UNAPPLICABLE, DAYS,
				"01:23", "10:30");
		allApp.add(app1);
		allApp.add(app2);
		allApp.add(app3);
		return allApp;
	}

	@Test
	public void testIsKpiApplicableTrue() throws InvalidApplicabilityException {
		new ApplicabilityCheckUtil();
		this.applicabilities = this.getAllApplicable();
		assertEquals(
				true,
				ApplicabilityCheckUtil.isApplicable(this.applicabilities,
						Long.valueOf(612471045155453L)));
	}

	@Test
	public void testIsKpiApplicableFalse() throws InvalidApplicabilityException {
		this.applicabilities = this.getAllUnApplicable();
		assertEquals(
				false,
				ApplicabilityCheckUtil.isApplicable(this.applicabilities,
						Long.valueOf(612471045155453L)));
	}

	@Test
	public void testIsKpiApplicable() throws InvalidApplicabilityException {
		applicabilities = this.getAllUnApplicableOutsideTimeRange();
		assertEquals(
				true,
				ApplicabilityCheckUtil.isApplicable(this.applicabilities,
						Long.valueOf(612471045155453L)));
	}

	@Test
	public void testIsKpiApplicableWhenNull() {
		assertEquals(
				true,
				ApplicabilityCheckUtil.isApplicable(this.applicabilities,
						Long.valueOf(612471045155453L)));
	}
}
