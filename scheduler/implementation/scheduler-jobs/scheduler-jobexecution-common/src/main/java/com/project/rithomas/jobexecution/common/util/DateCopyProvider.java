
package com.project.rithomas.jobexecution.common.util;

import java.sql.Timestamp;
import java.util.Date;

public class DateCopyProvider {

	public static Date copyOf(Date date) {
		return date == null ? null : new Date(date.getTime());
	}

	public static Timestamp copyOfTimeStamp(Timestamp date) {
		return (Timestamp) (date == null ? null : date.clone());
	}
}