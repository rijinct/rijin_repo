
package com.project.rithomas.jobexecution.generator.formula;

import java.io.File;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import com.rijin.analytics.hierarchy.model.KPIIndexConfiguration;
import com.rijin.analytics.hierarchy.service.KPIHierarchyParser;

public class IndexConfigRetriever {

	String[] hierarchyGroupNames = { "usecase", "ceiDesignerProp" };

	Map<String, KPIIndexConfiguration> kpiObjectMap = new LinkedHashMap<>();

	public KPIIndexConfiguration getKPIResponse(String kpiName)
			throws Exception {
		getJSON();
		return kpiObjectMap.get(kpiName);
	}

	private void getJSON() throws Exception {
		KPIHierarchyParser kpiHierarchyParser = new KPIHierarchyParser();
		for (String hierarchyGroup : hierarchyGroupNames) {
			String hierarchyDataFromPortal = getHierarchyDataFromHardCodedValue(
					hierarchyGroup);
			Map<String, KPIIndexConfiguration> kpiResponseMap = kpiHierarchyParser
					.getKPIObjectMap(hierarchyDataFromPortal, hierarchyGroup);
			kpiObjectMap.putAll(kpiResponseMap);
		}
	}

	private String getHierarchyDataFromHardCodedValue(String groupName)
			throws Exception {
		String jsonStr = "";
		String fileName = "";
		if (groupName.equals("ceiDesignerProp")) {
			fileName = "responses/ceiDesignerProp.json";
		} else {
			fileName = "responses/usecase.json";
		}
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource(fileName).getFile());
		jsonStr = new String(Files.readAllBytes(file.toPath()));
		return jsonStr;
	}
}
