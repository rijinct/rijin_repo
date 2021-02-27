
package com.project.rithomas.jobexecution.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.EnumUtils;
import org.joda.time.DateMidnight;
import org.joda.time.DateTime;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.etl.common.PlevelValues;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public final class DateFunctionTransformation {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DateFunctionTransformation.class);

	private static DateFunctionTransformation dateFunTrans;

	private DateFunctionTransformation() {
	}

	static {
		dateFunTrans = new DateFunctionTransformation();
	}

	public static DateFunctionTransformation getInstance() {
		return dateFunTrans;
	}

	public Long getTrunc(Long date, String alevel, String weekStartDay, String zoneId) {
		if (zoneId == null) {
			return getTrunc(date, alevel, weekStartDay);
		}
		TimeZoneConverter converter = new TimeZoneConverter();
		ZonedDateTime zdt = converter.dateTime(getFormattedDate(date))
				.from(TimeZoneConverter.UTC).to(zoneId).value();
		DateParameterType dateType = null;
		int interval = 0;
		String aggLevel = alevel;
		if (alevel.contains(TimeUnitConstants.MIN)) {
			interval = Integer
					.parseInt(alevel.replace(TimeUnitConstants.MIN, ""));
			aggLevel = TimeUnitConstants.MIN;
		}
		dateType = DateParameterType.valueOf(aggLevel);
		switch (dateType) {
		case HOUR:
			zdt = zdt.truncatedTo(ChronoUnit.HOURS);
			break;
		case DAY:
			zdt = zdt.truncatedTo(ChronoUnit.DAYS);
			break;
		case WEEK:
			zdt = zdt.truncatedTo(ChronoUnit.DAYS);
			int dayOfWeek = zdt.getDayOfWeek().getValue();
			int startDay = getStartDayOfWeek(weekStartDay).getValue();
			if (dayOfWeek > startDay) {
				zdt = zdt.minusDays(dayOfWeek - startDay);
			} else if (dayOfWeek < startDay) {
				zdt = zdt.minusDays(dayOfWeek + 7 - startDay);
			}
			break;
		case MONTH:
			zdt = zdt.truncatedTo(ChronoUnit.DAYS);
			zdt = zdt.withDayOfMonth(1);
			break;
		case MIN:
			zdt = zdt.truncatedTo(ChronoUnit.MINUTES);
			zdt = zdt.minusMinutes(zdt.getMinute() % interval);
			break;
		default:
			break;
		}
		return zdt.toInstant().toEpochMilli();
	}

	private DayOfWeek getStartDayOfWeek(String weekStartDay) {
		DayOfWeek dayOfWeek = DayOfWeek.MONDAY;
		if (EnumUtils.isValidEnum(DayOfWeek.class, weekStartDay)) {
			dayOfWeek = DayOfWeek.valueOf(weekStartDay);
		}
		return dayOfWeek;
	}
	
	
	public Long getTrunc(Long date, String alevel, String weekStartDay) {
		if (alevel != null) {
			DateParameterType dateType = null;
			Calendar calendar = new GregorianCalendar();
			calendar.setTimeInMillis(date);
			int interval = 0;
			String aggLevel = alevel;
			if (alevel.contains(TimeUnitConstants.MIN)) {
				interval = Integer
						.parseInt(alevel.replace(TimeUnitConstants.MIN, ""));
				aggLevel = TimeUnitConstants.MIN;
			}
			dateType = DateParameterType.valueOf(aggLevel);
			switch (dateType) {
			case HOUR:
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				break;
			case MIN:
				int unroundedMinutes = calendar.get(Calendar.MINUTE);
				int mod = unroundedMinutes % interval;
				calendar.add(Calendar.MINUTE, -mod);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				break;
			case DAY:
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				break;
			case WEEK:
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				int dayOfWeek = getDayOfWeek(weekStartDay);
				if (calendar.get(Calendar.DAY_OF_WEEK) < dayOfWeek) {
					calendar.add(Calendar.DATE, -6);
				}
				calendar.set(Calendar.DAY_OF_WEEK, dayOfWeek);
				break;
			case MONTH:
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				calendar.set(Calendar.DAY_OF_MONTH, 1);
				break;
			default:
				break;
			}
			return calendar.getTimeInMillis();
		} else {
			return null;
		}
	}

	public Long getTrunc(Long date, String alevel) {
		return getTrunc(date, alevel, null);
	}

	public int getDayOfWeek(String weekStartDay) {
		Days startDay = Days.MONDAY;
		if (EnumUtils.isValidEnum(Days.class, weekStartDay)) {
			startDay = Days.valueOf(weekStartDay);
		}
		return startDay.ordinal() + 1;
	}

	public String getFormattedDate(Date date) {
		SimpleDateFormat format = new SimpleDateFormat(
				JobExecutionContext.DATEFORMAT);
		return format.format(date);
	}

	public String getFormattedDate(Long dateInMilli) {
		SimpleDateFormat format = new SimpleDateFormat(
				JobExecutionContext.DATEFORMAT);
		return format.format(dateInMilli);
	}

	public Date getDate(String dateVal) {
		Date date = null;
		try {
			date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
					Locale.ENGLISH).parse(dateVal);
		} catch (ParseException e) {
			LOGGER.error("Exception :" + e.getMessage());
		}
		return date;
	}

	public Long getSystemTime() {
		Date date = new Date();
		return date.getTime();
	}

	public Calendar getNextBound(Long finalLb, String pLevel) {
		Calendar calendar = new GregorianCalendar();
		if (finalLb != null && pLevel != null && !pLevel.isEmpty()) {
			calendar.setTimeInMillis(finalLb);
			if (pLevel.contains(TimeUnitConstants.MIN)) {
				String[] pLevelValue = pLevel.split(TimeUnitConstants.MIN);
				if (pLevelValue != null && pLevelValue.length == 1) {
					calendar.add(Calendar.MINUTE,
							Integer.parseInt(pLevelValue[0]));
				}
			} else if (TimeUnitConstants.HOUR.equalsIgnoreCase(pLevel)) {
				calendar.add(Calendar.HOUR_OF_DAY, 1);
			} else if (TimeUnitConstants.DAY.equalsIgnoreCase(pLevel)) {
				calendar.add(Calendar.DAY_OF_MONTH, 1);
			} else if (TimeUnitConstants.WEEK.equalsIgnoreCase(pLevel)) {
				calendar.add(Calendar.DAY_OF_WEEK, 7);
			} else if (TimeUnitConstants.MONTH.equalsIgnoreCase(pLevel)) {
				calendar.add(Calendar.MONTH, 1);
			}
		}
		return calendar;
	}

	public Calendar getPreviousBound(Long finalLb, String pLevel) {
		Calendar calendar = new GregorianCalendar();
		if (finalLb != null && pLevel != null && !pLevel.isEmpty()) {
			calendar.setTimeInMillis(finalLb);
			if (pLevel.contains(TimeUnitConstants.MIN)) {
				String[] pLevelValue = pLevel.split(TimeUnitConstants.MIN);
				if (pLevelValue != null && pLevelValue.length == 1) {
					calendar.add(Calendar.MINUTE,
							-Integer.parseInt(pLevelValue[0]));
				}
			} else if (TimeUnitConstants.HOUR.equalsIgnoreCase(pLevel)) {
				calendar.add(Calendar.HOUR_OF_DAY, -1);
			} else if (TimeUnitConstants.DAY.equalsIgnoreCase(pLevel)) {
				calendar.add(Calendar.DAY_OF_MONTH, -1);
			} else if (TimeUnitConstants.WEEK.equalsIgnoreCase(pLevel)) {
				calendar.add(Calendar.DAY_OF_WEEK, -7);
			} else if (TimeUnitConstants.MONTH.equalsIgnoreCase(pLevel)) {
				calendar.add(Calendar.MONTH, -1);
			}
		}
		return calendar;
	}

	public Long getTruncatedDateAfterSubtractingDays(Integer days) {
		DateTime dateTime = new DateTime();
		dateTime = dateTime.minusDays(days);
		DateMidnight dateTimeMidnight = dateTime.toDateMidnight();
		return dateTimeMidnight.getMillis();
	}

	public StringBuffer getTimeDescription(long timeInSec) {
		StringBuffer stringBuffer = new StringBuffer();
		if (timeInSec > (60 * 60 * 24)) {
			long days = timeInSec / (60 * 60 * 24);
			stringBuffer.append(days).append(" day(s) ")
					.append(getTimeDescription(timeInSec % (60 * 60 * 24)));
		} else if (timeInSec > (60 * 60)) {
			long hours = timeInSec / (60 * 60);
			stringBuffer.append(hours).append(" hour(s) ")
					.append(getTimeDescription(timeInSec % (60 * 60)));
		} else if (timeInSec > 60) {
			long minutes = timeInSec / (60);
			stringBuffer.append(minutes).append(" minute(s) ")
					.append(getTimeDescription(timeInSec % (60)));
		} else {
			stringBuffer.append(timeInSec).append(" seconds");
		}
		return stringBuffer;
	}

	enum Days {
		SUNDAY,
		MONDAY,
		TUESDAY,
		WEDNESDAY,
		THURSDAY,
		FRIDAY,
		SATURDAY;
	}

	public static Calendar getDateBySubtractingDays(int days,
			WorkFlowContext context) {
		return getDateBySubtractingDays(days, context, null);
	}
	
	public static Calendar getDateBySubtractingDays(int days,
			WorkFlowContext context, String region) {
		Calendar calendar = Calendar.getInstance();
		DateFunctionTransformation dateTransformer = DateFunctionTransformation.getInstance();
		String weekStartDay = (String) context.getProperty(
				JobExecutionContext.WEEK_START_DAY);
		calendar.setTimeInMillis(dateTransformer.getTrunc(
				(new Date()).getTime(),
				(String) context.getProperty(JobExecutionContext.PLEVEL),
				weekStartDay, TimeZoneUtil.getZoneId(context, region)));
		calendar.setTimeInMillis(dateTransformer
				.getTrunc(calendar.getTimeInMillis(),
						PlevelValues.DAY.name(), weekStartDay,
						TimeZoneUtil.getZoneId(context, region)));
		calendar.setTime(DateUtils.addDays(calendar.getTime(), -days));
		return calendar;
	}
}
