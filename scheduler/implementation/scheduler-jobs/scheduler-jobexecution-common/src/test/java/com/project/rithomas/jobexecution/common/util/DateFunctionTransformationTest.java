
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

import org.junit.Test;

public class DateFunctionTransformationTest {

	@Test
	public void testGetTruncForDayZoneId() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-03 04:45:00");
		Long expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "DAY", null, "Asia/Kolkata");
		Long actual = 1354473000000L;
		assertEquals(expected, actual);
	}

	@Test
	public void testGetTruncFor15MinZoneId() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-03 04:48:00");
		Long expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "15MIN", null, "Asia/Kolkata");
		Long actual = 1354509900000L;
		assertEquals(expected, actual);
	}

	@Test
	public void testGetTruncForHourZoneId() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-03 04:48:00");
		Long expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "HOUR", null, "Asia/Kolkata");
		Long actual = 1354509000000L;
		assertEquals(expected, actual);
	}

	@Test
	public void testGetTruncForWeekZoneId() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 04:48:00");
		Long expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "WEEK", "0", "Asia/Kolkata");
		Long actual = 1354473000000L;
		expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "WEEK", "TUESDAY", "Asia/Kolkata");
		actual = 1354559400000L;
		assertEquals(expected, actual);
	}

	@Test
	public void testGetTruncForMonthZoneId() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 04:48:00");
		Long expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "MONTH", null, "Asia/Kolkata");
		Long actual = 1354300200000L;
		assertEquals(expected, actual);
	}

	@Test
	public void testGetTruncForDay() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-03 10:15:00");
		Long expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "DAY");
		Long actual = Long.parseLong("1354473000000");
		assertEquals(expected, actual);
	}

	@Test
	public void testGetTruncFor15Min() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-03 10:18:00");
		Long expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "15MIN");
		Long actual = Long.parseLong("1354509900000");
		assertEquals(expected, actual);
	}

	@Test
	public void testGetTruncForHour() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-03 10:18:00");
		Long expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "HOUR");
		Long actual = Long.parseLong("1354509000000");
		assertEquals(expected, actual);
	}

	@Test
	public void testGetTruncForWeek() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 10:18:00");
		Long expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "WEEK", "MONDAY");
		Long actual = Long.parseLong("1354473000000");
		assertEquals(expected, actual);
	}

	@Test
	public void testGetTruncForMonth() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 10:18:00");
		Long expected = DateFunctionTransformation.getInstance().getTrunc(
				date.getTime(), "MONTH");
		Long actual = Long.parseLong("1354300200000");
		assertEquals(expected, actual);
	}

	@Test
	public void testGetNextBoundFor15Min() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 10:15:00");
		Date dateNxt = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 10:30:00");
		Calendar actualCalendar = new GregorianCalendar();
		actualCalendar.setTime(dateNxt);
		Calendar expecCalendar = DateFunctionTransformation.getInstance()
				.getNextBound(date.getTime(), "15MIN");
		assertEquals(expecCalendar, actualCalendar);
	}

	@Test
	public void testGetNextBoundForHour() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 10:00:00");
		Date dateNxt = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 11:00:00");
		Calendar actualCalendar = new GregorianCalendar();
		actualCalendar.setTime(dateNxt);
		Calendar expecCalendar = DateFunctionTransformation.getInstance()
				.getNextBound(date.getTime(), "HOUR");
		assertEquals(expecCalendar, actualCalendar);
	}

	@Test
	public void testGetNextBoundForDay() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 00:00:00");
		Date dateNxt = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-07 00:00:00");
		Calendar actualCalendar = new GregorianCalendar();
		actualCalendar.setTime(dateNxt);
		Calendar expecCalendar = DateFunctionTransformation.getInstance()
				.getNextBound(date.getTime(), "DAY");
		assertEquals(expecCalendar, actualCalendar);
	}

	@Test
	public void testGetNextBoundForWeek() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-03 00:00:00");
		Date dateNxt = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-10 00:00:00");
		Calendar actualCalendar = new GregorianCalendar();
		actualCalendar.setTime(dateNxt);
		Calendar expecCalendar = DateFunctionTransformation.getInstance()
				.getNextBound(date.getTime(), "WEEK");
		assertEquals(expecCalendar, actualCalendar);
	}

	@Test
	public void testGetNextBoundForMonth() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-11-01 00:00:00");
		Date dateNxt = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-01 00:00:00");
		Calendar actualCalendar = new GregorianCalendar();
		actualCalendar.setTime(dateNxt);
		Calendar expecCalendar = DateFunctionTransformation.getInstance()
				.getNextBound(date.getTime(), "MONTH");
		assertEquals(expecCalendar, actualCalendar);
	}

	@Test
	public void testGetPreviousBoundFor15Min() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 10:15:00");
		Date datePrev = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 10:00:00");
		Calendar actualCalendar = new GregorianCalendar();
		actualCalendar.setTime(datePrev);
		Calendar expecCalendar = DateFunctionTransformation.getInstance()
				.getPreviousBound(date.getTime(), "15MIN");
		assertEquals(expecCalendar, actualCalendar);
	}

	@Test
	public void testGetPreviousBoundForHour() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 10:00:00");
		Date datePrev = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 09:00:00");
		Calendar actualCalendar = new GregorianCalendar();
		actualCalendar.setTime(datePrev);
		Calendar expecCalendar = DateFunctionTransformation.getInstance()
				.getPreviousBound(date.getTime(), "HOUR");
		assertEquals(expecCalendar, actualCalendar);
	}

	@Test
	public void testGetPreviousBoundForDay() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-06 00:00:00");
		Date datePrev = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-05 00:00:00");
		Calendar actualCalendar = new GregorianCalendar();
		actualCalendar.setTime(datePrev);
		Calendar expecCalendar = DateFunctionTransformation.getInstance()
				.getPreviousBound(date.getTime(), "DAY");
		assertEquals(expecCalendar, actualCalendar);
	}

	@Test
	public void testGetPreviousBoundForWeek() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-03 00:00:00");
		Date datePrev = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-11-26 00:00:00");
		Calendar actualCalendar = new GregorianCalendar();
		actualCalendar.setTime(datePrev);
		Calendar expecCalendar = DateFunctionTransformation.getInstance()
				.getPreviousBound(date.getTime(), "WEEK");
		assertEquals(expecCalendar, actualCalendar);
	}

	@Test
	public void testGetPreviousBoundForMonth() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-01 00:00:00");
		Date datePrev = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-11-01 00:00:00");
		Calendar actualCalendar = new GregorianCalendar();
		actualCalendar.setTime(datePrev);
		Calendar expecCalendar = DateFunctionTransformation.getInstance()
				.getPreviousBound(date.getTime(), "MONTH");
		assertEquals(expecCalendar, actualCalendar);
	}

	@Test
	public void testGetDateValid() throws ParseException {
		Date date = new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2012-12-01 00:00:00");
		Date formattedDate = DateFunctionTransformation.getInstance().getDate(
				"2012-12-01 00:00:00");
		assertEquals(date, formattedDate);
	}

	@Test
	public void testGetDateInvalid() {
		new TimeUnitConstants();
		Date formattedDate = DateFunctionTransformation.getInstance().getDate(
				"2012-12 00:00:00");
		assertNull(formattedDate);
	}
}
