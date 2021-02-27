
package com.project.rithomas.jobexecution.common.util;

import java.util.List;

public class SubPartitionValues {

	public String value1;

	public String value2;

	public String value3;

	public String value4;

	public String value5;

	public String getSubPartitionValue1() {
		return value1;
	}

	public void setSubPartitionValue1(String subPartitionValue1) {
		this.value1 = subPartitionValue1;
	}

	public String getSubPartitionValue2() {
		return value2;
	}

	public void setSubPartitionValue2(String subPartitionValue2) {
		this.value2 = subPartitionValue2;
	}

	public String getSubPartitionValue3() {
		return value3;
	}

	public void setSubPartitionValue3(String subPartitionValue3) {
		this.value3 = subPartitionValue3;
	}

	public String getSubPartitionValue4() {
		return value4;
	}

	public void setSubPartitionValue4(String subPartitionValue4) {
		this.value4 = subPartitionValue4;
	}

	public String getSubPartitionValue5() {
		return value5;
	}

	public void setSubPartitionValue5(String subPartitionValue5) {
		this.value5 = subPartitionValue5;
	}

	public SubPartitionValues(List<String> list) {
		for (String value : list) {
			if (this.value1 == null) {
				setSubPartitionValue1(value);
			} else if (this.value2 == null) {
				setSubPartitionValue2(value);
			} else if (this.value3 == null) {
				setSubPartitionValue3(value);
			} else if (this.value4 == null) {
				setSubPartitionValue4(value);
			} else if (this.value5 == null) {
				setSubPartitionValue5(value);
			}
		}
	}
}
