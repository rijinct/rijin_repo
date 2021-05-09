"""
Copyright (C) 2018 Nokia. All rights reserved.
"""
from enum import Enum,unique


@unique
class AlarmTask(Enum):
    '''
    The enum for Alarm tasks
    '''
    NOTIFY = 'notify'
    ACK = 'acknowledge'
    UNACK = 'unacknowledge'
