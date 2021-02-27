Feature: Certify the Dimension loading 

	Scenario Outline: Cleanup the directories created in previous run
		Given the ingress /se-test-enabler
		And append request parameter foldername with value <directory>
		When api post /test-enabler/api/nfs/deletefolder
		Then print response
		Then the status code is 200
		Examples: 
		| directory                    |
		| /mnt/staging/import/SAMPLE_1 | 
		| /mnt/staging/dqm/SAMPLE_1    |
		| /mnt/staging/test/dimension  |
		
	Scenario Outline: Create Pre-requisite configurations for dimension loading
		Given the ingress /se-test/schedulerengine
		And using json content type application/vnd.rijin-se-schedules-v1+json and request body <jobdefinition>
		When api post /api/manager/v1/scheduler/jobs
		Then the status code is 200
		Given the ingress /se-test-enabler
		And using multipart file content from file <hivetable>
		When api post /test-enabler/api/hive/v1/tables/command/multiple
		Then the status code is 200
		Then print response
		Given the ingress /se-test-enabler
		And using multipart file content from file <extTable>
		When api post /test-enabler/api/hive/v1/tables/command/multiple
		Then the status code is 200
		Then print response
		Given the ingress /se-test-enabler
		And append request parameter foldername with value <dqmFolder>
		When api post /test-enabler/api/nfs/createfolder
		Then print response
		Then the status code is 200
		Given the ingress /se-test/schedulerengine
		And using json content type application/vnd.rijin-se-schedules-v1+json and request body <jobMetadata>
		When api patch /api/manager/v1/scheduler/jobs/simple
		Then the status code is 200
		Then wait for 500 seconds
		
		Examples: 
		| jobdefinition                                                                   |  inuputfile                                                                  | inputPath                                       | dqmFolder                                   |  hivetable                                                                  | postgrestable | dbConf | jobMetadata | extTable | testOutputFolder | metadataSqls |defaultentry|
		| [{"jobGroup": "SAMPLE_1","jobName": "Entity_SAMPLE_LOCATION_1_CorrelationJob"}] | ./src/test/resources/features/dimension/data/input/EXT_SAMPLE_LOCATION_1.zip | /mnt/staging/import/SAMPLE_1/SAMPLE_LOCATION_1/ | /mnt/staging/dqm/SAMPLE_1/SAMPLE_LOCATION_1 | ./src/test/resources/features/dimension/sqls/create_location_table_Hive.sql |  ./src/test/resources/features/dimension/sqls/create_location_table_postgres.sql | { "configuration": { "dbName": "sai", "dbPassword": "rithomas", "dbPort": "5432", "dbService": "se-test-db", "dbUser": "rithomas", "nameSpace": "default" } } | [{"jobGroup": "SAMPLE_1","jobName": "Entity_SAMPLE_LOCATION_1_CorrelationJob","startTime": ""}] | ./src/test/resources/features/dimension/sqls/create_ext_table_hive.sql | /mnt/staging/test/dimension | ./src/test/resources/features/dimension/sqls/rithomas_data_sample_insert.sql | ./src/test/resources/features/dimension/sqls/updateExistingKeyDefaultValue.sql |
	
	Scenario Outline: Validate whether the field exceeding configured length is rejected
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
		
		Examples: 
		| dbRequest                                                                            | actualFilePath | expectedFilePath |
		| { "configuration": { "dbName": "sai", "dbPassword": "rithomas", "dbPort": "5432", "dbService": "se-test-db", "dbUser": "rithomas", "nameSpace": "default" } }| /mnt/staging/dqm/SAMPLE_1/SAMPLE_LOCATION_1/LocationGroups_String_Length_Validation.csv.log | ./src/test/resources/features/dimension/data/expected/validation-output/LocationGroups_String_Length_Validation.csv.log |
		
	Scenario Outline: Validate whether the field with data type mismatch is rejected
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
	
		Examples: 
		| dbRequest                                                                            | actualFilePath | expectedFilePath |
		| { "configuration": { "dbName": "sai", "dbPassword": "rithomas", "dbPort": "5432", "dbService": "se-test-db", "dbUser": "rithomas", "nameSpace": "default" } }| /mnt/staging/dqm/SAMPLE_1/SAMPLE_LOCATION_1/LocationGroups_DataType_Mismatch_Validation.csv.log | ./src/test/resources/features/dimension/data/expected/validation-output/LocationGroups_DataType_Mismatch_Validation.csv.log |
	
	Scenario Outline: Validate whether mandatory field with null is rejected
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
	
		Examples: 
		| dbRequest                                                                            | actualFilePath | expectedFilePath |
		| { "configuration": { "dbName": "sai", "dbPassword": "rithomas", "dbPort": "5432", "dbService": "se-test-db", "dbUser": "rithomas", "nameSpace": "default" } }| /mnt/staging/dqm/SAMPLE_1/SAMPLE_LOCATION_1/LocationGroups_Mandatory_Field_Not_Null_Validation.csv.log | ./src/test/resources/features/dimension/data/expected/validation-output/LocationGroups_Mandatory_Field_Not_Null_Validation.csv.log |
	
	Scenario Outline: Validate whether the field with invalid date format is rejected
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
	
		Examples: 
		| dbRequest                                                                            | actualFilePath | expectedFilePath |
		| { "configuration": { "dbName": "sai", "dbPassword": "rithomas", "dbPort": "5432", "dbService": "se-test-db", "dbUser": "rithomas", "nameSpace": "default" } }| /mnt/staging/dqm/SAMPLE_1/SAMPLE_LOCATION_1/LocationGroups_Invalid_Date_Format_Validation.csv.log | ./src/test/resources/features/dimension/data/expected/validation-output/LocationGroups_Invalid_Date_Format_Validation.csv.log |
	
	Scenario Outline: Validate whether the data is loaded to Hive
		Given the ingress /se-test-enabler
		And using json request body <hiveDbRequest>
		When api post /test-enabler/api/hive/v1/tables/command/single/withresult
		Then print response
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
		
		Examples: 
		| hiveDbRequest | actualFilePath | expectedFilePath |
		| { "query": "select * from project.es_sample_location_1" , "outputFilePath": "/mnt/staging/test/dimension/hiveSampleLocationOutput.csv" } | /mnt/staging/test/dimension/hiveSampleLocationOutput.csv |./src/test/resources/features/dimension/data/expected/hive-dimension-output/hiveSampleLocationOutput.csv|
	
	Scenario Outline: Validate whether the data with Special Character is loaded with UTF encoding.
		Given the ingress /se-test-enabler
		And using json request body <hiveDbRequest>
		When api post /test-enabler/api/hive/v1/tables/command/single/withresult
		Then print response
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
		
		Examples: 
		| hiveDbRequest | actualFilePath | expectedFilePath |
		| { "query": "select * from project.es_sample_location_1 where lac_id=2" , "outputFilePath": "/mnt/staging/test/dimension/hiveSpecialCharacterOutput.csv" } | /mnt/staging/test/dimension/hiveSpecialCharacterOutput.csv |./src/test/resources/features/dimension/data/expected/hive-dimension-output/hiveSpecialCharacterOutput.csv|
		
	Scenario Outline: Validate whether the Comma separated field is loaded to single column.
		Given the ingress /se-test-enabler
		And using json request body <hiveDbRequest>
		When api post /test-enabler/api/hive/v1/tables/command/single/withresult
		Then print response
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
		
		Examples: 
		| hiveDbRequest | actualFilePath | expectedFilePath |
		| { "query": "select * from project.es_sample_location_1 where lac_id=4" , "outputFilePath": "/mnt/staging/test/dimension/hiveDataWithCommaOutput.csv" } | /mnt/staging/test/dimension/hiveDataWithCommaOutput.csv |./src/test/resources/features/dimension/data/expected/hive-dimension-output/hiveDataWithCommaOutput.csv|	
	
	Scenario Outline: Validate whether the duplicate data is rejected and not loaded to Hive
		Given the ingress /se-test-enabler
		And using json request body <hiveDbRequest>
		When api post /test-enabler/api/hive/v1/tables/command/single/withresult
		Then print response
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
		
		Examples: 
		| hiveDbRequest | actualFilePath | expectedFilePath |
		| { "query": "select * from project.es_sample_location_1 where lac_id=3" , "outputFilePath": "/mnt/staging/test/dimension/hiveDuplicateDataOutput.csv" } | /mnt/staging/test/dimension/hiveDuplicateDataOutput.csv |./src/test/resources/features/dimension/data/expected/hive-dimension-output/hiveDuplicateDataOutput.csv|
	
	Scenario Outline: Validate whether all the file names with pattern are loaded.
		Given the ingress /se-test-enabler
		And using json request body <hiveDbRequest>
		When api post /test-enabler/api/hive/v1/tables/command/single/withresult
		Then print response
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
		
		Examples: 
		| hiveDbRequest | actualFilePath | expectedFilePath |
		| { "query": "select * from project.es_sample_location_1" , "outputFilePath": "/mnt/staging/test/dimension/hiveSampleLocationOutput.csv" } | /mnt/staging/test/dimension/hiveSampleLocationOutput.csv |./src/test/resources/features/dimension/data/expected/hive-dimension-output/hiveSampleLocationOutput.csv|
	
	Scenario Outline: Validate whether existing key is updated with the new value.
		Given the ingress /se-test-enabler
		And using json request body <hiveDbRequest>
		When api post /test-enabler/api/hive/v1/tables/command/single/withresult
		Then print response
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
		
		Examples: 
		| hiveDbRequest | actualFilePath | expectedFilePath |
		| { "query": "select * from project.es_sample_location_1 where lac_id=5" , "outputFilePath": "/mnt/staging/test/dimension/hiveExistingKeyUpdateOutput.csv" } | /mnt/staging/test/dimension/hiveExistingKeyUpdateOutput.csv |./src/test/resources/features/dimension/data/expected/hive-dimension-output/hiveExistingKeyUpdateOutput.csv|
	
	Scenario Outline: Validate whether the data is loaded to Postgres
		Given the ingress /se-test-enabler
		And using json request body <postgresDbRequest>
		When api post /test-enabler/api/postgres/v1/tables/command/single/withresult
		Then print response
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
		
		Examples: 
		| postgresDbRequest | actualFilePath | expectedFilePath |
		| { "configuration": { "dbName": "sai", "dbPassword": "rithomas", "dbPort": "5432", "dbService": "se-test-db", "dbUser": "rithomas", "nameSpace": "default" } , "query": "select * from saidata.es_sample_location_1" , "outputFilePath": "/mnt/staging/test/dimension/postgresOutput.csv" }| /mnt/staging/test/dimension/postgresOutput.csv |./src/test/resources/features/dimension/data/expected/postgres-dimension-output/postgresOutput.csv|	
	
		
	Scenario Outline: Check if the dimension input file is deleted after the job is successful
		Given the ingress /se-test-enabler
		And append request parameter foldername with value <inputFolder>
		When api post /test-enabler/api/nfs/checkfolderempty
		Then Complete response must be true
		
		Examples: 
		| inputFolder                                    |
		| /mnt/staging/import/SAMPLE_1/SAMPLE_LOCATION_1 | 
	
	Scenario Outline: Verify the previous job already running scenario
		Given the ingress /se-test-enabler
		And using json request body <dbConf>
		When api post /test-enabler/api/postgres/v1/tables/command/single
		Then the status code is 200
		Given the ingress /se-test/schedulerengine
		And using json content type application/vnd.rijin-se-schedules-v1+json and request body <jobMetadata>
		When api patch /api/manager/v1/scheduler/jobs/simple
		Then the status code is 200
		Then wait for 300 seconds
		Given the ingress /se-test-enabler
		And using json request body <dbConf2>
		When api post /test-enabler/api/postgres/v1/tables/command/single/withresult
		Then print response
		Given the ingress /se-test-enabler
		And append request parameter actualFileLocation with value <actualFilePath>
		And using multipart file content from file <expectedFilePath>
		When api post /test-enabler/api/nfs/comparefiles
		Then Complete response must be true
		
		Examples: 
		| dbConf                                    | dbConf2 | actualFilePath | expectedFilePath | jobMetadata |
		| { "configuration": { "dbName": "sai", "dbPassword": "rithomas", "dbPort": "5432", "dbService": "se-test-db", "dbUser": "rithomas", "nameSpace": "default" }, "query": "INSERT INTO rithomas.ETL_STATUS (PROC_ID, JOB_NAME, TYPE, LOAD_TIME, START_TIME, END_TIME, STATUS, DESCRIPTION) values(1000, 'Entity_SAMPLE_LOCATION_1_CorrelationJob', 'Correlation', LOCALTIMESTAMP(0), LOCALTIMESTAMP(0) , LOCALTIMESTAMP(0) , 'R' , 'SE Test Job');" }  | { "configuration": { "dbName": "sai", "dbPassword": "rithomas", "dbPort": "5432", "dbService": "se-test-db", "dbUser": "rithomas", "nameSpace": "default" }, "query": "select status from etl_status where job_name='Entity_SAMPLE_LOCATION_1_CorrelationJob' and status='W';", "outputFilePath": "/mnt/staging/test/dimension/postgresOutputRunningJob.csv" } | /mnt/staging/test/dimension/postgresOutputRunningJob.csv |./src/test/resources/features/dimension/data/expected/postgres-dimension-output/postgresOutputRunningJob.csv| [{"jobGroup": "SAMPLE_1","jobName": "Entity_SAMPLE_LOCATION_1_CorrelationJob","startTime": ""}] |
		