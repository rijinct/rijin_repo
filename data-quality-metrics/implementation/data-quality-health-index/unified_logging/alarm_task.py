'''
Created on 16-Sep-2021

@author: rithomas
'''
from enum import Enum,unique


@unique
class AlarmTask(Enum):
    '''
    The enum for Alarm tasks
    '''
    NOTIFY = 'notify'
    ACK = 'acknowledge'
    UNACK = 'unacknowledge'
