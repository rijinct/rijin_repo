
package com.rijin.analytics.scheduler.k8s.processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.MediaType;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STRawGroupDir;
import org.stringtemplate.v4.StringRenderer;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.k8s.client.K8sCronApi;
import com.rijin.analytics.scheduler.processor.JobProcessor;

public abstract class K8sBaseProcessor extends JobProcessor {
	
	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(K8sBaseProcessor.class);

