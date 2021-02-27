
package com.rijin.analytics.scheduler.k8s.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rijin.analytics.k8s.client.configmap.ConfigMapClient;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.k8s.validator.HintsSchemaValidator;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@Api(tags = "Configuration Management", description = "Read and Update hints for Hive/Spark")
@RequestMapping("/api/manager/v1/scheduler/configuration")
@Validated
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConfigurationManagementController {

	private static final String SUFFIX_SETTINGS_KEY = "_SETTINGS_KEY";

	private static final String QUERY_SETTINGS_CONFIG_MAP = "QUERY_SETTINGS_CONFIGMAP_NAME";

	private static final String NAMESPACE = "RELEASE_NAMESPACE";
	
	@Autowired
	ConfigMapClient processor;

	@Autowired
	HintsSchemaValidator hintsSchemaValidator;

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ConfigurationManagementController.class);

	@GetMapping(path = "/hints", produces = { "application/vnd.rijin-se-settings-v1+xml",
			"application/vnd.rijin-error-response-v1+json" })
	@ApiOperation(value = "${configmap.fetch}", notes = "${configmap.fetch.notes}")
	public String fetchConfigMap(
			@ApiParam(value = "${configmap.queryEngine.fetch}", required = true, type = "string", allowableValues = "HIVE,SPARK", example = "HIVE/SPARK") @RequestParam(value = "queryEngine") String queryEngine) {
		LOGGER.info("Request to fetch config-map received");
		String configMapContent = processor.readConfigMap(getNamespace(), getConfigMapName(), getDataKey(queryEngine));
		LOGGER.info("config-map fetched successfully");
		return configMapContent;
	}

	@PatchMapping(path = "/hints", produces = { "application/vnd.rijin-error-response-v1+json" }, consumes = {
			"application/vnd.rijin-se-settings-v1+xml" })
	@ApiOperation(value = "${configmap.update}", notes = "${configmap.update.notes}")
	public void updateConfigMap(
			@ApiParam(value = "${configmap.queryEngine.update}", required = true, type = "string", allowableValues = "HIVE,SPARK", example = "HIVE/SPARK") @RequestParam(value = "queryEngine") String queryEngine,
			@ApiParam(value = "${configmap.querysettings}", example = "", allowEmptyValue = false, defaultValue = "", required = true) @RequestBody String querySettingsXML) {
		LOGGER.info("Request to update config-map received");

		hintsSchemaValidator.validate(querySettingsXML, queryEngine);
		processor.patchConfigMap(getNamespace(), getConfigMapName(), getDataKey(queryEngine), querySettingsXML);
		LOGGER.info("configmap updated successfully ");
	}

	public String getNamespace() {
		return System.getenv(NAMESPACE);
	}

	public String getConfigMapName() {
		return System.getenv(QUERY_SETTINGS_CONFIG_MAP);
	}

	public String getDataKey(String queryEngine) {
		return System.getenv(queryEngine + SUFFIX_SETTINGS_KEY);
	}

	

	
}
