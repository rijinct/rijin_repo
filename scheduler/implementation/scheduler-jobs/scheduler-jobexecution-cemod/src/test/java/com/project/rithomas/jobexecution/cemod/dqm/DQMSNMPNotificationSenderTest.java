
package com.project.rithomas.jobexecution.project.dqm;

import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.snmp4j.PDU;

import com.project.rithomas.etl.common.ETLConstants;
import com.project.rithomas.etl.notifier.snmp.util.ReadPropertyFile;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { ReadPropertyFile.class })
public class DQMSNMPNotificationSenderTest {

	DQMSNMPNotificationSender sender = new DQMSNMPNotificationSender();

	Map<String, Object> notificationData = new HashMap<String, Object>();

	@Before
	public void setUp() throws Exception {
		ReadPropertyFile readPropFile = mock(ReadPropertyFile.class);
		PowerMockito.mockStatic(ReadPropertyFile.class);
		PowerMockito.when(ReadPropertyFile.getInstance()).thenReturn(
				readPropFile);
		Mockito.when(
				readPropFile.getProperty(Mockito.anyString(),Mockito.any())).thenReturn("defaultValue");
		Mockito.when(
				readPropFile.getProperty(Mockito.anyString(),
						Mockito.anyString())).thenReturn("10");
	}

	@Test
	public void testSetTrapDetails() {
		PDU pdu = new PDU();
		sender.setTrapDetails(pdu, notificationData);
		notificationData.put(ETLConstants.SOURCE_NAME, "SMS_1");
		notificationData.put(ETLConstants.DESCRIPTION,
				"Did not receive data from SMS_1 for last 15 minutes.");
		sender.setTrapDetails(pdu, notificationData);
	}
}
