'''
Created on 16-Sep-2021

@author: rithomas
'''
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
