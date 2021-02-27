
package com.project.rithomas.jobexecution.reaggregation;

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
public class ReaggSnmpNotificationSenderTest {

	ReaggSnmpNotificationSender sender = new ReaggSnmpNotificationSender();

	Map<String, Object> notificationData = new HashMap<String, Object>();

	@Before
	public void setUp() throws Exception {
		ReadPropertyFile readPropFile = mock(ReadPropertyFile.class);
		PowerMockito.mockStatic(ReadPropertyFile.class);
		PowerMockito.when(ReadPropertyFile.getInstance()).thenReturn(
				readPropFile);
		Mockito.when(
				readPropFile.getProperty(Mockito.any(),
						Mockito.any())).thenReturn("10");
	}

	@Test
	public void testSetTrapDetails() {
		PDU pdu = new PDU();
		sender.setTrapDetails(pdu, notificationData);
		notificationData.put(ETLConstants.SOURCE_NAME, "SMS_1");
		notificationData.put(ETLConstants.DESCRIPTION,
				"Reaggregation completed for SMS_1 till 2013-08-01 00:00:00.");
		sender.setTrapDetails(pdu, notificationData);
	}
}
