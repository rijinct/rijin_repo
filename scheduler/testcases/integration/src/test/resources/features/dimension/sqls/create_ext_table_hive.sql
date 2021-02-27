DROP TABLE IF EXISTS `ext_sample_location_1`;
CREATE TABLE `ext_sample_location_1`(
  `cell_id` bigint COMMENT 'SAMPLE_TYPE_ID',
  `sac_id` bigint COMMENT 'SAMPLE_TYPE',
  `cell_sac_id` bigint COMMENT 'DESCRIPTION',
  `lac_id` bigint COMMENT '',
  `location_name` string COMMENT '',
  `latitude` decimal(10,2) COMMENT '',
  `longitude` decimal(10,2) COMMENT '',
  `city` string COMMENT '',
  `region` string COMMENT '',
  `mcc` int COMMENT '',
  `mnc` int COMMENT '',
  `test_time` string COMMENT '')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
WITH SERDEPROPERTIES (
  'colelction.delim'='$',
  'mapkey.delim'=':')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
LOCATION
  '/rithomas/es/work/SAMPLE_1/SAMPLE_LOCATION_1'
TBLPROPERTIES (
  'transient_lastDdlTime'='1547715559')