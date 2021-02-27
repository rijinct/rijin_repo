
package com.project.rithomas.jobexecution.reaggregation;

import java.util.Map;

import org.snmp4j.PDU;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.VariableBinding;

import com.project.rithomas.etl.common.ETLConstants;
import com.project.rithomas.etl.notifier.snmp.AbstractSnmpNotificationSender;
import com.project.rithomas.etl.notifier.snmp.util.MibOidConstants;
import com.project.rithomas.etl.notifier.snmp.util.ReadPropertyFile;

public class ReaggSnmpNotificationSender
		extends AbstractSnmpNotificationSender {

	public static final String DEFAULT_OID_VALUE = "1.3.6.1.4.1.28458.1.21.1.1.7.15";

	@Override
	public void setTrapDetails(PDU pdu, Map<String, Object> notificationData) {
		String defaultValue = ReadPropertyFile.getInstance()
				.getProperty(DEFAULT_OID_VALUE, null);
		final String defaultStr = "Not Available/Not Applicable";
		String jobName = (String) notificationData
				.get(ETLConstants.SOURCE_NAME);
		String description = (String) notificationData
				.get(ETLConstants.DESCRIPTION);
		pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID,
				new OID(ReadPropertyFile.getInstance().getProperty(
						MibOidConstants.REAGG_STATUS_SNMP_TRAP_OID,
						defaultValue))));
		// Set Job Name
		if (jobName != null) {
			pdu.add(new VariableBinding(new OID(ReadPropertyFile.getInstance()
					.getProperty(MibOidConstants.REAGG_STATUS_JOB_NAME,
							DEFAULT_OID_VALUE)),
					new OctetString(jobName)));
		} else {
			pdu.add(new VariableBinding(new OID(ReadPropertyFile.getInstance()
					.getProperty(MibOidConstants.REAGG_STATUS_JOB_NAME,
							DEFAULT_OID_VALUE)),
					new OctetString(defaultStr)));
		}
		if (description != null) {
			pdu.add(new VariableBinding(new OID(ReadPropertyFile.getInstance()
					.getProperty(MibOidConstants.REAGG_STATUS_DESCRIPTION,
							DEFAULT_OID_VALUE)),
					new OctetString(description)));
		} else {
			pdu.add(new VariableBinding(new OID(ReadPropertyFile.getInstance()
					.getProperty(MibOidConstants.REAGG_STATUS_DESCRIPTION,
							DEFAULT_OID_VALUE)),
					new OctetString(defaultStr)));
		}
	}
}
