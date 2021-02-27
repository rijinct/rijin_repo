
package com.rijin.analytics.scheduler.k8s.cron;

import java.time.ZoneId;

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;

import com.cronutils.mapper.CronMapper;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.rijin.analytics.exceptions.ServiceException;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.k8s.cron.converter.CronConverter;

public class CronExpressionRetriever {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CronExpressionRetriever.class);

	private static final int QUARTZ_CRON_MIN_SPACE_COUNT = 5;

	private static final int UNIX_CRON_SPACE_COUNT = 4;

	public static String getCronExpression(String cronExp) {
		String cronExpression = cronExp.trim();
		int count = StringUtils.countMatches(cronExpression, " ");
		if (count >= QUARTZ_CRON_MIN_SPACE_COUNT) {
			cronExpression = validateAndRetrieveCron(cronExpression,
					CronType.QUARTZ);
		} else if (count == UNIX_CRON_SPACE_COUNT) {
			cronExpression = validateAndRetrieveCron(cronExpression,
					CronType.UNIX);
		}
		CronConverter converter = new CronConverter();
		return converter.using(cronExpression).from(ZoneId.systemDefault())
				.to(ZoneId.of("UTC")).convert();
	}

	private static String validateAndRetrieveCron(String cronExp,
			CronType cronType) {
		CronDefinition cronDefinition = CronDefinitionBuilder
				.instanceDefinitionFor(cronType);
		CronParser parser = new CronParser(cronDefinition);
		String unixExp = "";
		try {
			Cron cron = parser.parse(cronExp);
			CronMapper mapper = getCronMapper(cronType, cronDefinition);
			Cron unixCron = mapper.map(cron);
			unixCron.validate();
			unixExp = unixCron.asString();
		} catch (IllegalArgumentException ex) {
			LOGGER.error("Cron expression validation failed", ex);
			throw new ServiceException("cronExpressionValidation",
					HttpStatus.UNPROCESSABLE_ENTITY);
		}
		return unixExp;
	}

	private static CronMapper getCronMapper(CronType crontype,
			CronDefinition cronDefinition) {
		if (crontype == CronType.QUARTZ) {
			return CronMapper.fromQuartzToUnix();
		} else {
			return CronMapper.sameCron(cronDefinition);
		}
	}
}
