
package com.rijin.scheduler.jobexecution.hive;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.settings.GlobalSettings;
import com.rijin.scheduler.jobexecution.hive.settings.Hive2Settings;
import com.rijin.scheduler.jobexecution.hive.settings.Job;
import com.rijin.scheduler.jobexecution.hive.settings.QueryHints;
import com.rijin.scheduler.jobexecution.hive.settings.QuerySettings;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.XStreamException;

public class CA4CIHiveHintsConfiguration extends DbConfigurator {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CA4CIHiveHintsConfiguration.class);

	@Override
	public void getHiveQueryHintsFromFile(String dbName)
			throws WorkFlowExecutionException {
		super.getHiveQueryHintsFromFile(dbName);
		LOGGER.debug("Query Hints of Jobs from hive settting.xml for global settings "
				+ queryHintMap.toString());
		LOGGER.debug("Query Hints of Job Patterns from hive settting.xml for global settings "
				+ queryHintPatternMap.toString());
		try (FileReader fileReader = new FileReader(retriveHiveFilePath(dbName))){
			
			xstream = new XStream();
			xstream.processAnnotations(QuerySettings.class);
			xstream.processAnnotations(GlobalSettings.class);
			xstream.processAnnotations(Hive2Settings.class);
			xstream.processAnnotations(Job.class);
			xstream.processAnnotations(QueryHints.class);
			QuerySettings hivesettings = (QuerySettings) xstream
					.fromXML(fileReader);
			if (hivesettings.getHive2Settings().getJobId() != null) {
				for (Job jobName : hivesettings.getHive2Settings().getJobId()) {
					appendHiveSettings(jobName, dbName);
				}
			}
		} catch (FileNotFoundException e) {
			LOGGER.error("FileNotFoundException occured {} ", e.getMessage());
			throw new WorkFlowExecutionException(e.getMessage(), e);
		} catch (XStreamException e) {
			handleXStreamExceptions(e);
		} catch (IOException e) {
			LOGGER.error("IOException occured {} ", e.getMessage());
			throw new WorkFlowExecutionException(e.getMessage(), e);
		}
		LOGGER.debug("Query Hints of Jobs from hive settting.xml for {} : {}",
				Hive2Settings.class.getName(), queryHintMap);
		LOGGER.debug(
				"Query Hints of Job Patterns from hive settting.xml for global settings {} : {}",
				Hive2Settings.class.getName(), queryHintPatternMap);
	}
}
