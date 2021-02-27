
package com.rijin.analytics.schedulerengine.features;

import org.junit.runner.RunWith;

import cucumber.api.CucumberOptions;
import net.serenitybdd.cucumber.CucumberWithSerenity;

@RunWith(CucumberWithSerenity.class)
@CucumberOptions(features = { "src/test/resources/features" }, glue = {
		"com.rijin.analytics.test.keywords" }, tags = { "not @ignore" })
public class AcceptanceIT {
}

