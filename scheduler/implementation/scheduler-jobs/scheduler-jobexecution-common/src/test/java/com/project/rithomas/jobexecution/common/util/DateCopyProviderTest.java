
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DateCopyProvider.class)
public class DateCopyProviderTest {

	@Test
	public void copyOfTest() {
		Date date = new Date();
		date.setTime(30);
		PowerMockito.mockStatic(DateCopyProvider.class);
		when(DateCopyProvider.copyOf(date)).thenReturn(date);
		assertEquals(30, DateCopyProvider.copyOf(date).getTime());
	}

	@Test(expected = NullPointerException.class)
	public void copyOfNullTest() {
		Date date = new Date();
		date = null;
		PowerMockito.mockStatic(DateCopyProvider.class);
		when(DateCopyProvider.copyOf(date)).thenReturn(date);
		assertEquals(null, DateCopyProvider.copyOf(date).getTime());
	}

	@Test
	public void copyOfTimeStampTest() {
		Timestamp stamp = new Timestamp(5);
		PowerMockito.mockStatic(DateCopyProvider.class);
		when(DateCopyProvider.copyOfTimeStamp(stamp)).thenReturn(stamp);
		assertEquals(stamp, DateCopyProvider.copyOfTimeStamp(stamp).clone());
	}

	@Test(expected = NullPointerException.class)
	public void copyOfTimeStampNullTest() {
		Timestamp stamp = null;
		PowerMockito.mockStatic(DateCopyProvider.class);
		when(DateCopyProvider.copyOfTimeStamp(stamp)).thenReturn(stamp);
		assertEquals(null, DateCopyProvider.copyOfTimeStamp(stamp).clone());
	}
}
