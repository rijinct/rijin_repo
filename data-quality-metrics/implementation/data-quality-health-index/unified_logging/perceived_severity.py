from enum import Enum,unique

@unique
class PerceivedSeverity(Enum):
    '''
    The enum of ITU Perceived Severity, according to RFC5674(https://tools.ietf.org/html/rfc5674)
        ITU Perceived Severity      syslog SEVERITY (Name)
        Critical                    1 (Alert)
        Major                       2 (Critical)
        Minor                       3 (Error)
        Warning                     4 (Warning)
        Indeterminate               5 (Notice)
        Cleared                     5 (Notice)
        Table 1. ITUPerceivedSeverity to Syslog SEVERITY Mapping
    '''
    CRIT = 'critical'
    MAJOR = 'major'
    MINOR = 'minor'
    WARNING = 'warning'
    INDET = 'indeterminate'
    CLEARED = 'cleared'
