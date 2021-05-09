"""
Copyright (C) 2018 Nokia. All rights reserved.
"""


class MissingMandatoryField(Exception):
    '''
    The exception of MissingMandatoryField
    '''
    def __init__(self, filed):
        '''
        Constructs an instance of MissingMandatoryField with thespecified detail message
        :param filed: the detail message
        '''
        self.message = 'missing mandatory filed %s' % str(filed)
