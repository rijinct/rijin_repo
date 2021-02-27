
package com.project.rithomas.jobexecution.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.project.rithomas.jobexecution.common.util.JobExecutionContext;

public class SparkMainClass {

	public static void main(String[] args) throws Exception {
		SparkSession spark = null;
		try {
			spark = SparkSession.builder().enableHiveSupport().getOrCreate();
			String dirName = spark.conf()
					.get(JobExecutionContext.SPARK_RESULT_DIR);
			for (String query : args) {
				Dataset<Row> sqlDF = spark.sql(query);
				if (!dirName.isEmpty()
						&& !StringUtils.containsIgnoreCase(query, "SET ")) {
					sqlDF.repartition(1).write()
							.format("com.databricks.spark.csv")
							.save("/spark/result/".concat(dirName));
				}
			}
		} finally {
			if (spark != null) {
				spark.stop();
			}
		}
	}
}