
package com.rijin.analytics.scheduler.k8s.converters;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import org.yaml.snakeyaml.Yaml;

public class YamlToJsonConverter {

	private YamlToJsonConverter() {
	}

	public static Object toJson(String yamlString) {
		if (isEmpty(yamlString)) {
			return null;
		}
		return new Yaml().load(yamlString);
	}
}
