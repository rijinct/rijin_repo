
package com.project.rithomas.jobexecution.common.util;

import java.io.File;
import java.nio.file.FileSystems;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;

public class StringModificationUtil {

	public static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(StringModificationUtil.class);

	public static String replaceMultipleSubStrings(String template,
			Map<String, String> tokens) {
		LOGGER.debug("Replacing the substrings in the string : {}", template);
		String patternString = "(" + StringUtils.join(tokens.keySet(), "|")
				+ ")";
		Pattern pattern = Pattern.compile(patternString);
		Matcher matcher = pattern.matcher(template);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			matcher.appendReplacement(sb, tokens.get(matcher.group(1)));
		}
		matcher.appendTail(sb);
		LOGGER.debug("Replaced all the substrings and final string is : {}",
				sb.toString());
		return sb.toString();
	}

	public static File getFileWithCanonicalPath(String fileName) {
		return FileSystems.getDefault().getPath(fileName).normalize().toFile();
	}
}
