TAG_XPATH_LOC = './row/formula'
BUSINESS_METADATA_TAG_XPATH_LOC = './Universe/Attributes/Attribute/LogicalColumn'
SUBPROC_SCRIPT = 'sql_parser.sh'
EXECUTOR = 'sh'
#To filter all US, SEGG Tables, DATA_CP_CELL tables, below pattern should be used
#TABLE_PATTERN = ['US_%','PS_%SEGG%', '%DATA_CP_CELL%']
TABLE_PATTERN = ['ES_%','US_%','PS_%']
#Enter the directory containing .uni files
UNI_FILES_DIR="/mnt/querybuilder/"
#Enter the file name which has list of kpi's to be exported for Business Metadata
kpi_file = ""
csv_columns = ['Kpi_Name','Definition','Descriptive_Example','Business_Rule','Calculation_Rule','LogicalTable','LogicalColumn','Asset_Type','DOMAIN_NAME','DOMAIN_TYPE','COMMUNITY']
XML_DF_COLUMNS = ['tablename','table_description','columnname','level','definition', 'datatype', 'formula','sequence', 'readbylist', 'writtenbylist', 'jobidlist', 'interval']
monitoringOutputDir='/var/local/monitoring/output/'
#Enter the absolute path with file name that contains list of jobs to export for operational metadata
EXPORT_JOBS_LIST=""
technical_meta_xsl_custom = '/opt/nsn/ngdb/monitoring/metadata/technical/custom/technicalMetadata.xsl'
technical_meta_xsl_default = '/opt/nsn/ngdb/monitoring/metadata/technical/technicalMetadata.xsl'
operational_meta_xsl_custom = '/opt/nsn/ngdb/monitoring/metadata/operational/custom/operationalMetadata.xsl'
operational_meta_xsl_default = '/opt/nsn/ngdb/monitoring/metadata/operational/operationalMetadata.xsl'