
package com.rijin.analytics.scheduler.k8s.converters;

import static org.apache.commons.lang3.StringEscapeUtils.escapeJson;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rijin.analytics.exceptions.InternalServerErrorException;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;

public class MapToJsonConverter {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(MapToJsonConverter.class);

	private MapToJsonConverter() {
	}

	public static String toJson(Map<String, Object> jobProperties) {
		try {
			return escapeJson(
					new ObjectMapper().writeValueAsString(jobProperties));
		} catch (JsonProcessingException exception) {
			LOGGER.error("Exception occurred when converting map to json",
					exception);
			throw new InternalServerErrorException();
		}
	}

	public static Map<String, Object> toMap(String properties) {
		try {
			return new ObjectMapper().readValue(properties,
					new TypeReference<Map<String, Object>>() {
					});
		} catch (IOException exception) {
			LOGGER.error("Exception occurred when converting json to map",
					exception);
			throw new InternalServerErrorException();
		}
	}
}
