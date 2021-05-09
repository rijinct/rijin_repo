from enum import Enum


class ETLTopologyStatus(Enum):
    DELAY_WHEN_BOUNDARY_NULL = -1


class CheckConnectors(Enum):
    STATUS_OK, STATUS_PARTIAL_OK, STATUS_NOK = range(3)


class TableCounts(Enum):
    COULUMN_COUNT_FOR_NA, VAR_FACTOR_FOR_NA = -1, -1


class AlarmKeys(Enum):

    MEDIATION_ALARM = 'MEDIATION_NOT_SENDING_DATA'
    BACKLOG_ALARM = 'ETL_BACKLOG_HIGH'
    USAGE_JOBS_ALARM = 'USAGE_JOB_FAILED'
    AGGREGATION_JOBS_ALARM = 'AGGREGATION_JOB_FAILED'
    DELAYED_USAGE_JOBS_ALARM = 'DELAYED_USAGE_JOBS'
    LONG_RUNNING_JOBS_ALARM = 'LONG_RUNNING_JOBS'
    POSTGRES_USERS_CONNECTION_ALARM = 'POSTGRES_USER_CONNECTIONS_HIGH'
    HDFS_USED_PERC_ALARM = 'TOTAL_HDFS_USED_PERC_HIGH'
    ETL_LAG_ALARM = 'ETL_LAG_HIGH'
    CEI_INDEX_CHANGE_ALARM = 'CEI_INDEX_CHANGE'
    MISSING_DAT_AND_CTR_ALARM = 'MISSING_DAT_AND_CTR_FILES'
    DIMENSION_JOBS_ALARM = 'DIMENSION_JOB_FAILED'
    QS_JOBS_ALARM = 'QS_JOB_FAILED'
    DIMENSION_FILES_NOT_ARRIVING = 'DIMENSION_FILES_NOT_ARRIVING'
    ETL_INPUT_FILE_SIZE_LARGE = 'ETL_INPUT_FILE_SIZE_LARGE'
    APPLICATION_NODE_MNT_USAGE = 'MNT_VOLUME_BREACHED'
    SUMMARY_REPORT = 'SUMMARY_REPORT'
    SERVICES_DOWN = 'SERVICES_DOWN'
    SELF_HEALED_HUNG_SERVICES = 'SELF_HEALED_HUNG_SERVICES'
    HDFS_WRITE_ALARM = 'HDFS_WRITE_SPEED_NOT_OK'
    HDFS_READ_ALARM = 'HDFS_READ_SPEED_NOT_OK'
    DQM_FILECOUNT_ALARM = 'DQM_FILECOUNT_ALARM'
    MEPH_ALARM = 'MEPH_CONFIG_ALARM'
    IMSI_ID_GENERATE_EXCEPTION = 'IMSI_ID_GENERATE_EXCEPTION'


class TypeOfUtils(Enum):
    MNT_STATS_UTIL= 'mntStats'
    LONG_RUNING_JOB_UTIL = 'longRunningJob'
    SERVICE_DOWN_UTIL = 'sparkSelfHeal'
    DATA_TRAFFIC = 'dataTraffic'
    RADIO_TABLE_UTIL = 'radioSeggTable'
