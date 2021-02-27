
package com.rijin.scheduler.jobexecution.hive;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class DbConfiguratorTest {

	private DbConfigurator configurator = new CA4CIHiveHintsConfiguration();
	private Collection<String> source = ImmutableSet.of(
			"CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode'",
			"CREATE TEMPORARY FUNCTION substr AS 'com.nexr.platform.hive.udf.UDFSubstrForOracle'",
			"SET file.reader=FILE_READER_CLASS");
	private Collection<String> expected = ImmutableSet.of(
			"CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode'",
			"CREATE TEMPORARY FUNCTION substr AS 'com.nexr.platform.hive.udf.UDFSubstrForOracle'",
			"SET file.reader=com.project.rithomas.common.hive.io.reader.ParquetFileReader");

	@Test
	public void testReplace_with_Key() {
		assertEquals(expected, configurator
				.replace(source, "FILE_READER_CLASS", "com.project.rithomas.common.hive.io.reader.ParquetFileReader").toSet());
	}

	@Test
	public void testReplace_without_Key() {
		assertEquals(source, configurator
				.replace(source, "FILE_READER_CLASS_UNAVAILBLE", "com.project.rithomas.common.hive.io.reader.ParquetFileReader")
				.toSet());

	}

	@Test
	public void testReplace_with_Null_Key() {
		assertEquals(source,
				configurator.replace(source, null, "com.project.rithomas.common.hive.io.reader.ParquetFileReader").toSet());

	}

	@Test(expected = NullPointerException.class)
	public void testReplace_Null_source() {
		configurator
				.replace(null, "FILE_READER_CLASS_UNAVAILBLE", "com.project.rithomas.common.hive.io.reader.ParquetFileReader")
				.toSet();

	}

	@Test(expected = NullPointerException.class)
	public void testReplace_Null_Value() {
		configurator.replace(source, "FILE_READER_CLASS", null).toSet();

	}

}
