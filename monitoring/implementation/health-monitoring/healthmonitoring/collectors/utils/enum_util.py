'''
Created on 27-Apr-2020

@author: deerakum
'''
from enum import Enum


class CheckConnectors(Enum):
    RUNNING_STATUS_OK, RUNNING_STATUS_PARTIAL_OK, RUNNING_STATUS_NOK = range(3)


class ETLTopologyStatus(Enum):
    DELAY_WHEN_BOUNDARY_NULL = -1
    SOURCE_AND_SINK_UP, SOURCE_UP, SINK_UP, SOURCE_AND_SINK_DOWN = range(4)
    NO_DELAY_TIME, ALL_NODES_DOWN = [-1, -2]


class TableCounts(Enum):
    COULUMN_COUNT_FOR_NA, VAR_FACTOR_FOR_NA = -1, -1
