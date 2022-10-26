SOURCE_URL = ''

CONCORD_SOURCE_URL = ''

DATA_SRC_DEFAULT_PROPERTY = {
    'token': '',
    'content': 'instrument',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_SRC_AES_PROPERTY = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_AES_PROPERTY = {
    'token': '',
    'content': 'record',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'fields[0]': 'opn_ouid',
    'fields[1]': 'opn',
    'fields[2]': 'ouid',
    'forms[0]': 'instrument_name',
    'events[0]': 'events_name',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_AGES_AND_STAGES_PROPERTY = {
    'token': '',
    'content': 'instrument',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_AGES_AND_STAGES_PROPERTY = {
    'token': '',
    'content': 'record',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'fields[1]': 'asq_opn',
    'fields[2]': 'asq_ouid',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_EXT_IDS_PROPERTY = {
    'token': '',
    'content': 'record',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'forms[0]': 'instrument_name',
    'fields[1]': 'oid',
    'fields[1]': 'part_urn',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

IDS_INSTRUMENT_NAME = 'origins_id_number_form'

DATA_SRC_MNS_PROPERTY = {
    'token': '',
    'content': 'instrument',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_MNS_PROPERTY = {
    'token': '',
    'content': 'record',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'fields[0]': 'or_opn',
    'fields[1]': 'or_m_ouid',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_SCREENING_PROPERTY = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_SCREENING_PROPERTY = {
    'token': '',
    'content': 'record',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'fields[0]': 'screening_id',
    'forms[0]': 'instrument_name',
    'events[0]': 'event_name',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_QUESTIONAIRES_PROPERTY = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_QUESTIONAIRES_PROPERTY = {
    'token': '',
    'content': 'record',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'forms[0]': 'instrument_name',
    'events[0]': 'event_name',
    'fields[0]': 'rm_ed_m_c1_qid1357',
    'fields[1]': 'rm_ed_m_c1_qid1356',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_METADATA_SRC_QUESTIONAIRES_PROPERTY = {
    'token': '',
    'content': 'metadata',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_SRC_DB_PROPERTY = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_DB_PROPERTY = {
    'token': '',
    'content': 'record'
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'fields[0]': 'or_preg_nmbr',
    'fields[1]': 'or_scr_id',
    'fields[2]': 'or_mat_oid',
    'forms[0]': 'instrument_name',
    'events[0]': 'event_name',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

ORIGINS_DB_VARIANTS = ["", "_aes", "_as", "_ec"]

DATE_FORMAT = '%Y-%m-%d'

DATA_SRC_ECONSENT_WITHDRAW_PATERNAL_PROPERTY = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_ECONSENT_WITHDRAW_PATERNAL_PROPERTY = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'forms[0]': 'basic_information "Basic Information"',
    'fields[0]': 'mat_rd_ouid',
    'fields[1]': 'mat_rd_opn',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_ECONSENT_WITHDRAW_MATERNAL_PROPERTY = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_ECONSENT_WITHDRAW_MATERNAL_PROPERTY = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'forms[0]': 'basic_information "Basic Information"',
    'fields[0]': 'mat_rd_ouid',
    'fields[1]': 'mat_rd_opn',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_JHC_ORIGINS_MOTHER_DIGI_QUEST_PROPERTY = {
    'token': '',
    'content': 'instrument',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_JHC_ORIGINS_MOTHER_DIGI_QUEST_PROPERTY = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'fields[0]': 'm_mrn',
    'forms[0]': 'instrument_name',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_JHC_ORIGINS_PARTNER_DIGI_QUEST_PROPERTY = {
    'token': '',
    'content': 'instrument',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_JHC_ORIGINS_PARTNER_DIGI_QUEST_PROPERTY = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'fields[0]': 'f_mrn',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_DIGI_CONSENT_PATERNAL_FULL_ORIGINS_PARTICIPATION_PROP = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_DIGI_CONSENT_PATERNAL_FULL_ORIGINS_PARTICIPATION_PROP = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'json',
    'type': 'flat',
    'forms[0]': 'instrument_name',
    'fields[0]': 'pat_full_opn',
    'fields[1]': 'pat_full_ouid',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_PATERNAL_ROUTINE_DATA_CONSENT_PROP = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_PATERNAL_ROUTINE_DATA_CONSENT_PROP = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'fields[0]': 'pat_rd_opn',
    'fields[1]': 'pat_rd_ouid',
    'forms[0]': 'instrument_name',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_MATERNAL_ROUTINE_DATA_CONSENT_PROP = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_MATERNAL_ROUTINE_DATA_CONSENT_PROP = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'fields[0]': 'mat_rd_opn',
    'fields[1]': 'mat_rd_ouid',
    'fields[2]': 'or_preg_nmbr',
    'forms[0]': 'instrument_name',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_MATERNAL_ROUTINE_DATA_CONSENT_PROP = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_MATERNAL_ROUTINE_DATA_CONSENT_PROP = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'forms[0]': 'instrument_name',
    'fields[0]': 'or_preg_nmbr',
    'fields[1]': 'mat_rd_opn',
    'fields[2]': 'mat_rd_ouid',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_MATERNAL_CONSENT_FULL_ORIGINS_PROP = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_MATERNAL_CONSENT_FULL_ORIGINS_PROP = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'fields[0]': 'mat_full_opn',
    'fields[1]': 'mat_full_oud',
    'forms[0]': 'instrument_name',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_PATERNAL_CONSENT_FULL_ORIGINS_PROP = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_PATERNAL_CONSENT_FULL_ORIGINS_PROP = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'fields[0]': 'pat_full_opn',
    'fields[1]': 'pat_full_ouid',
    'forms[0]': 'paternal_consent_full_origins_participation_and_bi "paternal_consent_full_basic_information"',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}


DATA_SRC_CHILD_ROUTINE_DATA_CONSENT = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_CHILD_ROUTINE_DATA_CONSENT = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'fields[0]': 'child_rd_opn',
    'fields[1]': 'child_rd_oud',
    'fields[2]': 'or_preg_nmbr',
    'forms[0]': 'instrument_name',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_CHILD_CONSENT_FULL_PARTICIPATION = {
    'token': '',
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_CHILD_CONSENT_FULL_PARTICIPATION = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'fields[0]': 'child_full_opn',
    'fields[1]': 'child_full_oud',
    'forms[0]': 'instrument_name',
    'type': 'flat',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_FATHERS_PAPER_Q = {
    'token': '',
    'content': 'instrument',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_FATHERS_PAPER_Q = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'fields[0]': 'f_mrn_idf',
    'forms[0]': 'instrument_name',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

DATA_SRC_MOTHERS_PAPER_Q = {
    'token': '',
    'content': 'instrument',
    'format': 'csv',
    'returnFormat': 'json'
}

DATA_EXT_MOTHERS_PAPER_Q = {
    'token': '',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'fields[0]': 'mrn_id',
    'csvDelimiter': '',
    'rawOrLabel': 'label',
    'rawOrLabelHeaders': 'label',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}

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
COLUMN_PATTERN_QUESTIONAIRES = ['pregnancy ID','Questionnaire ID']

COLUMN_PATTERN_DB = ['Pregnancy Number','Screening ID','Unique ID']

COLUMN_PATTERN_JHC_MOTHER_DIGI_QUEST = ['Medical Reference Number']

COLUMN_PATTERN_ECONSENT_WITHDRAW_PATERNAL = ['Participants OPN', 'Participants OUID']

COLUMN_PATTERN_ECONSENT_WITHDRAW_MATERNAL = ['Participants OPN', 'Participants OUID']

COLUMN_PATTERN_PATERNAL_ROUTINE_DATA_CONSENT = ['Participants OPN', 'Participants OUID']

COLUMN_PATTERN_MATERNAL_ROUTINE_DATA_CONSENT = ['Participants OPN', 'Participants OUID']

COLUMN_PATTERN_MATERNAL_CONSENT_FULL_ORIGINS = ['Participants OPN', 'Participants OUID']

COLUMN_PATTERN_CHILD_ROUTINE_DATE_CONSENT = ['Participants OPN', 'Participants OUID']

COLUMN_PATTERN_CHILD_CONSENT_FULL_ORIGINS = ['Participants OPN', 'Participants OUID']

COLUMN_PATTERN_FATHERS_PAPER_Q = ['MRN']

COLUMN_PATTERN_MOTHERS_PAPER_Q = ['MRN']

COLUMN_PATTERN_PATERNAL_CONSENT_FULL_ORIGINS = ['Participants OPN', 'Participants OUID']

COLUMN_PATTERN_SCREENING = ['Screening ID', 'SCR']

COLUMN_PATTERN_UNIQID = ['Unique ID', 'Medical Record Number']

COLUMN_PATTERN_AES = ['Pregnancy Number','Unique ID']

COLUMN_PATTERN_MNS = ['Pregnancy Number','Unique ID']

COLUMN_PATTERN_JHC_PARTNER_DIGI_QUEST = ['Medical Reference Number']

COLUMN_PATTERN_AGES_AND_STAGES = ['OPN_OUID', 'OPN', 'OUID']

COLUMN_PATTERN_CONCORD_REDCAP = ['Participant ID']

SOURCE_AES = 'Australian_Eating_Survey'

SOURCE_AGES = 'Ages_and_Stages_Questionnaire'

SOURCE_IDS = 'ORIGINS_Unique_ID_Generator'

SOURCE_SCREENING = 'ORIGINS_Screening_Database'

SOURCE_MOTHERS_PQ = 'ORIGINS_Mothers_Paper_Q'

SOURCE_FATHERS_PQ = 'ORIGINS_Fathers_Paper_Q'

SOURCE_QUESTIONNAIRES = 'ORIGINS_Questionnaire'

SOURCE_CHILD_CONSENT_FOP = 'ORIGINS_Digital_Consent_Child_Consent_Full_ORIGINS_Participation'

SOURCE_CHILD_ROUTINE_DC = 'ORIGINS_Digital_Consent_Child_Routine_Data_Consent'

SOURCE_MATERNAL_CONSENT_FOP = 'ORIGINS_Digital_Consent_Maternal_Consent_Full_ORIGINS_Participation'

SOURCE_MATERNAL_ROUTINE_DC = 'ORIGINS_Digital_Consent_Maternal_Routine_Data_Consent'

SOURCE_PATERNAL_ROUTINE_DC = 'ORIGINS_Digital_Consent_Paternal_Routine_Data_Consent'

SOURCE_PATERNAL_CONSENT_FOP = 'ORIGINS_Digital_Consent_Paternal_Consent_Full_ORIGINS_Participation'

SOURCE_PARTNERS_DQ = 'ORIGINS_Partners_Digital_Questionnaire'

SOURCE_MOTHER_DQ = 'ORIGINS_Mother_Digital_Questionnaire'

SOURCE_E_CONSENT_WMC = 'ORIGINS_E_Consent_Withdrawal_Maternal_Child'

SOURCE_E_CONSENT_WP = 'ORIGINS_E_Consent_Withdrawal_Paternal'

SOURCE_MNS = 'Midwife_Notification_System'

SOURCE_ORIGINS_DB = 'ORIGINS_Database'

SOURCE_CONCORD_REDCAP = 'CONCORD_Redcap'

APP_COL_PATTERN = 'Participant_ID'

APP = "App"

CP_OUTPUT_DIR = '/nfs/mnt1/staging/import/{}/origins_data'

INSTR_NAME = 'instrument_name'

MODEL_ENTITY = 'model_entity'

PERMISSION = 'permission'