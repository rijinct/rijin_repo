DROP TABLE IF EXISTS `es_sample_location_1`;
CREATE TABLE `es_sample_location_1`(
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
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'colelction.delim'='$',
  'mapkey.delim'=':')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/rithomas/es/SAMPLE_1/SAMPLE_LOCATION_1'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'transient_lastDdlTime'='1547715555')