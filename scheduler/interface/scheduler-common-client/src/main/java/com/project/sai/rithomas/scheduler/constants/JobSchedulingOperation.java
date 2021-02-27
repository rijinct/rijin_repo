
package com.project.sai.rithomas.scheduler.constants;

public enum JobSchedulingOperation {
	CREATE("create"),
	DELETE(null),
	SCHEDULE("schedule"),
	UNSCHEDULE("unschedule"),
	RESCHEDULE(null),
	STANDBY(null),
	SHUTDOWN(null),
	ABORT(null),
	PAUSE("unschedule"),
	RESUME("resume"),
	ASYNCHRONOUS_SCHEDULE(null);
		
	private String templateName;

	private JobSchedulingOperation(String templateName) {
		this.templateName = templateName;
	}
	
	public String getTemplateName() {
		return templateName;
	}
		
}
