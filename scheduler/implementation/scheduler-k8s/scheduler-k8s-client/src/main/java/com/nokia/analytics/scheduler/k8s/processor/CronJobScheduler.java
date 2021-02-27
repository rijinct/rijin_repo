
package com.rijin.analytics.scheduler.k8s.processor;

import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.CRON_EXPRESSION;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.JOB_PROPERTIES;
import static com.project.sai.rithomas.scheduler.constants.JobSchedulingOperation.SCHEDULE;

import java.util.List;

import org.stringtemplate.v4.ST;

import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.k8s.converters.MapToJsonConverter;
import com.rijin.analytics.scheduler.k8s.cron.CronExpressionRetriever;
import com.rijin.analytics.scheduler.k8s.cron.CronGenerator;
import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiException;

public class CronJobscheduler extends CronBaseProcessor<Trigger> {

	@Override
	protected String getTemplateName() {
		return SCHEDULE.getTemplateName();
	}

	@Override
	protected List<Trigger> getResourceGroup() {
		return getJobDetails().getSchedule().getTrigger();
	}

	@Override
	protected void fillTemplate(ST template, Trigger trigger) {
		String cronExpression = "";
		if (trigger.isCron()) {
			cronExpression = CronExpressionRetriever
					.getCronExpression(trigger.getCron().getCronExpression());
		} else {
			CronGenerator generator = new CronGenerator();
			cronExpression = generator
					.getCron(trigger.getSimple().getStartTime());
		}
		template.add(CRON_EXPRESSION, cronExpression);
		template.add(JOB_PROPERTIES, MapToJsonConverter.toJson(getJobProperties()));
	}

	@Override
	protected String getRequestKey(Trigger trigger) {
		return getJobId(trigger);
	}

	protected String getJobId(Trigger trigger) {
		return trigger.isCron() ? trigger.getCron().getJobName()
				: trigger.getSimple().getJobName();
	}

	@Override
	protected Call getCall(String cronJobName, Object cronPatch)
			throws ApiException {
		overrideHeaders();
		return getCronApiExecutor().patchCronCall(cronJobName, cronPatch);
	}

	@Override
	protected String getErrorKey() {
		return "cronJobScheduleFailed";
	}
}
