

DATA_SRC_CONCORD_REDCAP = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_CONCORD_REDCAP = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'fields[0]': 'record_id',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

JOB_LIST = [29,30,31,33,34,35]

#it is case insensitive so, any pattern is ok
COLUMN_PATTERN_CONCORD_REDCAP = ['Participant ID']

SOURCE_CONCORD_REDCAP = 'CONCORD_Redcap'

CP_OUTPUT_DIR = '/nfs/mnt1/staging/import/{}/origins_data'

INSTR_NAME = 'instrument_name'

MODEL_ENTITY = 'model_entity'

PERMISSION = 'permission'

DATE_FORMAT = '%Y-%m-%d'