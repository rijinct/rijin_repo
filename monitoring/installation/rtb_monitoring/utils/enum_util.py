from enum import Enum


class CheckConnectors(Enum):
    RUNNING_STATUS_OK, RUNNING_STATUS_PARTIAL_OK, RUNNING_STATUS_NOK = range(3)


class ETLTopologyStatus(Enum):
    DELAY_WHEN_BOUNDARY_NULL = -1


class TableCounts(Enum):
    COULUMN_COUNT_FOR_NA, VAR_FACTOR_FOR_NA = -1, -1
