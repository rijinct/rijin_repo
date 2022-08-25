import argparse
import os
import sys
import datetime
import org.tki.origins.constants as c
from org.tki.common.dao import Dao
from org.tki.common.dao import DaoForm
from org.tki.common.df_util import DfUtil
import glob
import shutil


class AesDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_AES_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_AES_PROPERTY
    COLUMN_PATTERN = c.COLUMN_PATTERN_AES
    SOURCE = c.SOURCE_AES


class AgesAndStagesDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_AGES_AND_STAGES_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_AGES_AND_STAGES_PROPERTY
    COLUMN_PATTERN = c.COLUMN_PATTERN_AGES_AND_STAGES
    SOURCE = c.SOURCE_AGES

    def sort_instruments_df(self, instruments):
        self.list_of_instruments = sorted(
            zip(instruments.instrument_name, instruments.instrument_label))

    def modify_ext_property(self, instrument_name, event_name=''):
        self.data_ext_property['forms[0]'] = instrument_name
        if self.date_range_lst[0] != 'None':
            self.data_ext_property['dateRangeBegin'] = self.date_range_lst[0]
            self.data_ext_property['dateRangeEnd'] = self.date_range_lst[1]
        print('data_ext_property after modification -> {}'.format(self.data_ext_property)) ## logger.debug

    def log_message(self, event_instr):
        print("Problem with getting data from " +
              event_instr[0])

    def write_df_to_csv(self, instrument_name, event_name):
        self.results.to_csv(os.path.join(
            self.output_dir, instrument_name + "_" + self.timestr + ".csv"),
            index=False)

    def trigger_execute_steps(self, event_instr):
        self.execute_steps(event_instr[0])


class IdsDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_DEFAULT_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_IDS_PROPERTY
    COLUMN_PATTERN = c.COLUMN_PATTERN_UNIQID
    SOURCE = c.SOURCE_IDS

    def create_instruments_df(self):
        pass

    def check_for_header_df(self):
        pass

    def modify_ext_property(self, instrument_name, event_name=''):
        self.data_ext_property['forms[0]'] = instrument_name
        if self.date_range_lst[0] != 'None':
            self.data_ext_property['dateRangeBegin'] = self.date_range_lst[0]
            self.data_ext_property['dateRangeEnd'] = self.date_range_lst[1]
        print('data_ext_property after modification -> {}'.format(self.data_ext_property)) ## logger.debug

    def write_df_to_csv(self, instrument_name, event_name):
        self.results.to_csv(os.path.join(
            self.output_dir, instrument_name + "_" + self.timestr + ".csv"),
            index=False)

    def iterate_instrument_and_execute(self):
        self.execute_steps(c.IDS_INSTRUMENT_NAME)


class MnsDao(DaoForm):
    DATA_SRC_PROPERTY = c.DATA_SRC_MNS_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_MNS_PROPERTY
    COLUMN_PATTERN = c.COLUMN_PATTERN_MNS
    SOURCE = c.SOURCE_MNS


class ScreeningDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_SCREENING_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_SCREENING_PROPERTY
    COLUMN_PATTERN = c.COLUMN_PATTERN_SCREENING
    SOURCE = c.SOURCE_SCREENING



class QuestionairesDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_QUESTIONAIRES_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_QUESTIONAIRES_PROPERTY
    DATA_METADATA_SRC_PROPERTY = c.DATA_METADATA_SRC_QUESTIONAIRES_PROPERTY
    META_DATA_DF_WO_DESC = ''
    META_DATA_DF = ''
    COLUMN_PATTERN = c.COLUMN_PATTERN_QUESTIONAIRES
    SOURCE = c.SOURCE_QUESTIONNAIRES

    # def create_metadata_df(self):
    #     r = requests.post(self.src_url, data=self.DATA_METADATA_SRC_PROPERTY)
    #     self.META_DATA_DF_WO_DESC = pd.read_csv(StringIO(r.text))
    #     self.META_DATA_DF = self.META_DATA_DF_WO_DESC[
    #         self.META_DATA_DF_WO_DESC.field_type != "descriptive"]
    #
    # def check_for_header_df(self):
    #     name_header = self.results.columns.tolist()
    #     print(self.META_DATA_DF_WO_DESC['field_name'])
    #     index = 1
    #     for name in name_header:
    #         if not self.META_DATA_DF.loc[self.META_DATA_DF_WO_DESC[
    #                                          'field_name'] == name, 'field_label'].empty:
    #             name_header[name_header.index(name)] = str(index) + "-" + \
    #                                                    self.META_DATA_DF_WO_DESC.loc[
    #                                                        self.META_DATA_DF_WO_DESC[
    #                                                            'field_name'] == name, 'field_label'].item()
    #             index += 1
    #     self.results.columns = name_header

    def write_df_to_csv(self, instrument_name, event_name):
        self.results.to_csv(os.path.join(
            self.output_dir,
            instrument_name + "_" + event_name + "_" + self.timestr + ".csv"),
            mode='a',
            index=False)

    def trigger_execute_steps(self, instrument_name):
        #self.create_metadata_df()
        self.execute_steps(instrument_name[0], instrument_name[1])


class DbDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_DB_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_DB_PROPERTY
    COLUMN_PATTERN = c.COLUMN_PATTERN_DB
    SOURCE = c.SOURCE_ORIGINS_DB

    def execute_steps(self, instrument_name, event_name=''):
        self.modify_ext_property(instrument_name, event_name)
        #return
        self.create_data_df()
        self.execution_steps_variant(instrument_name, event_name)
        variants = c.ORIGINS_DB_VARIANTS
        if instrument_name == "web_questionnaires":
            for val in variants:
                self.execution_steps_variant(instrument_name + val, event_name)

    def execution_steps_variant(self, instrument_name, event_name):
        self.enrich_data_df(instrument_name)
        self.check_for_header_df()
        self.write_stats(instrument_name, event_name)
        self.write_df_to_csv(instrument_name, event_name)

class EConsentWithdrawPaternalDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_ECONSENT_WITHDRAW_PATERNAL_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_ECONSENT_WITHDRAW_PATERNAL_PROPERTY
    COLUMN_PATTERN = c.COLUMN_PATTERN_ECONSENT_WITHDRAW_PATERNAL
    SOURCE = c.SOURCE_E_CONSENT_WP


class EConsentWithdrawMaternalDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_ECONSENT_WITHDRAW_MATERNAL_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_ECONSENT_WITHDRAW_MATERNAL_PROPERTY
    COLUMN_PATTERN = c.COLUMN_PATTERN_ECONSENT_WITHDRAW_MATERNAL
    SOURCE = c.SOURCE_E_CONSENT_WMC


class JhcOriginsMotherDigiQuestDao(DaoForm):
    DATA_SRC_PROPERTY = c.DATA_SRC_JHC_ORIGINS_MOTHER_DIGI_QUEST_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_JHC_ORIGINS_MOTHER_DIGI_QUEST_PROPERTY
    COLUMN_PATTERN = c.COLUMN_PATTERN_JHC_MOTHER_DIGI_QUEST
    SOURCE = c.SOURCE_MOTHER_DQ

    def trigger_execute_steps(self, instrument_name):
        self.execute_steps(instrument_name[0])


class JhcOriginsPartnerDigiQuestDao(DaoForm):
    DATA_SRC_PROPERTY = c.DATA_SRC_JHC_ORIGINS_PARTNER_DIGI_QUEST_PROPERTY
    DATA_EXT_PROPERTY = c.DATA_EXT_JHC_ORIGINS_PARTNER_DIGI_QUEST_PROPERTY
    COLUMN_PATTERN = c.COLUMN_PATTERN_JHC_PARTNER_DIGI_QUEST
    SOURCE = c.SOURCE_PARTNERS_DQ

    def trigger_execute_steps(self, instrument_name):
        self.execute_steps(instrument_name[0])


class PaternalFullOriginsParticipationDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_DIGI_CONSENT_PATERNAL_FULL_ORIGINS_PARTICIPATION_PROP
    DATA_EXT_PROPERTY = c.DATA_EXT_DIGI_CONSENT_PATERNAL_FULL_ORIGINS_PARTICIPATION_PROP
    COLUMN_PATTERN = c.COLUMN_PATTERN_PATERNAL_CONSENT_FULL_ORIGINS
    SOURCE = c.SOURCE_PATERNAL_CONSENT_FOP


class PaternalRoutineDataConsentDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_PATERNAL_ROUTINE_DATA_CONSENT_PROP
    DATA_EXT_PROPERTY = c.DATA_EXT_PATERNAL_ROUTINE_DATA_CONSENT_PROP
    COLUMN_PATTERN = c.COLUMN_PATTERN_PATERNAL_ROUTINE_DATA_CONSENT
    SOURCE = c.SOURCE_PATERNAL_ROUTINE_DC


class MaternalRoutineDataConsentDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_MATERNAL_ROUTINE_DATA_CONSENT_PROP
    DATA_EXT_PROPERTY = c.DATA_EXT_MATERNAL_ROUTINE_DATA_CONSENT_PROP
    COLUMN_PATTERN = c.COLUMN_PATTERN_MATERNAL_ROUTINE_DATA_CONSENT
    SOURCE = c.SOURCE_MATERNAL_ROUTINE_DC


class MaternalConsentFullOriginsDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_MATERNAL_CONSENT_FULL_ORIGINS_PROP
    DATA_EXT_PROPERTY = c.DATA_EXT_MATERNAL_CONSENT_FULL_ORIGINS_PROP
    COLUMN_PATTERN = c.COLUMN_PATTERN_MATERNAL_CONSENT_FULL_ORIGINS
    SOURCE = c.SOURCE_MATERNAL_CONSENT_FOP


class PaternalConsentFullOriginsDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_PATERNAL_CONSENT_FULL_ORIGINS_PROP
    DATA_EXT_PROPERTY = c.DATA_EXT_PATERNAL_CONSENT_FULL_ORIGINS_PROP
    COLUMN_PATTERN = c.COLUMN_PATTERN_PATERNAL_CONSENT_FULL_ORIGINS
    SOURCE = c.SOURCE_PATERNAL_CONSENT_FOP


class ChildRoutineDataConsentDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_CHILD_ROUTINE_DATA_CONSENT
    DATA_EXT_PROPERTY = c.DATA_EXT_CHILD_ROUTINE_DATA_CONSENT
    COLUMN_PATTERN = c.COLUMN_PATTERN_CHILD_ROUTINE_DATE_CONSENT
    SOURCE = c.SOURCE_CHILD_ROUTINE_DC


class ChildConsentFullOriginsParticipationDao(Dao):
    DATA_SRC_PROPERTY = c.DATA_SRC_CHILD_CONSENT_FULL_PARTICIPATION
    DATA_EXT_PROPERTY = c.DATA_EXT_CHILD_CONSENT_FULL_PARTICIPATION
    COLUMN_PATTERN = c.COLUMN_PATTERN_CHILD_CONSENT_FULL_ORIGINS
    SOURCE = c.SOURCE_CHILD_CONSENT_FOP


class FathersPaperQDao(DaoForm):
    DATA_SRC_PROPERTY = c.DATA_SRC_FATHERS_PAPER_Q
    DATA_EXT_PROPERTY = c.DATA_EXT_FATHERS_PAPER_Q
    COLUMN_PATTERN = c.COLUMN_PATTERN_FATHERS_PAPER_Q
    SOURCE = c.SOURCE_FATHERS_PQ

    def trigger_execute_steps(self, instrument_name):
        self.execute_steps(instrument_name[0])


class MothersPaperQDao(DaoForm):
    DATA_SRC_PROPERTY = c.DATA_SRC_MOTHERS_PAPER_Q
    DATA_EXT_PROPERTY = c.DATA_EXT_MOTHERS_PAPER_Q
    COLUMN_PATTERN = c.COLUMN_PATTERN_MOTHERS_PAPER_Q
    SOURCE = c.SOURCE_MOTHERS_PQ

    def trigger_execute_steps(self, instrument_name):
        self.execute_steps(instrument_name[0])

class AppDao(Dao):
    COLUMN_PATTERN = c.APP_COL_PATTERN

    def trigger_execute_steps(self, instrument_name):
        self.execute_steps(instrument_name)

    def execute_steps(self, instrument_name, event_name=''):
        if not self.results.empty:
            self.enrich_data_df(instrument_name)
            self.check_for_header_df()
            self.write_stats(instrument_name, c.APP)
            self.write_df_to_csv(instrument_name, c.APP)

    def iterate_instrument_and_execute(self):
        file_list = glob.glob('{}/*csv'.format(self.output_dir))
        for file in file_list:
            df_util_obj = DfUtil(file)
            df_util_obj.create_csv_df()
            instrument_name = os.path.basename(file).split("-")[0]
            print(instrument_name)
            self.results = df_util_obj.get_df()
            self.execute_steps(instrument_name)
            if self.env == "local":
                archive_dir = '.'
            else:
                archive_dir = '/nfs/mnt1/staging/import/{}/archive'.format(self.env)
            shutil.move(file, archive_dir)


class DaoManager():

    def cmdline_args():
        # Make parser object
        p = argparse.ArgumentParser(description="""
            Extract updates from ORIGINS Database from REDCap
            """,
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        # Optional argument which requires a parameter (eg. -d test)
        # This option is used to set the permission on the Data Hub depending on the ingestion needed
        p.add_argument("-p", "--permission", default="rest-reader",
                       required=False)
        # This option is to determine which Entity we're going to ingest the data for, this will be important to set path to the dataset
        p.add_argument("-c", "--model_entity", required=True)
        # This option sets the outpu directory
        p.add_argument("-od", "--output_directory", required=True)
        p.add_argument("-s", "--source", required=True)
        p.add_argument("-t", "--token", required=False)
        p.add_argument("-d", "--date", required=True)
        p.add_argument("-e", "--env", required=True)



        return (p.parse_args())

    def get_date_range(args):
        args = args.split()
        arg_count = len(args)
        datetimeFormat = c.DATE_FORMAT
        date_range_lst = []
        to_dt = None
        from_dt = None
        if (arg_count < 2) & (args[0] == "All"):
            print('Extracting the complete redcap data')
        elif (arg_count < 2) & (args[0] == "Default"):
            to_dt = datetime.datetime.strptime(
                datetime.datetime.now().strftime(datetimeFormat),
                datetimeFormat)
            from_dt = to_dt - datetime.timedelta(days=1)
        elif (arg_count == 2):
            from_dt = datetime.datetime.strptime(args[0], datetimeFormat)
            to_dt = datetime.datetime.strptime(args[1], datetimeFormat)
        else:
            print('Date format is incorrectly provided. View the help section')
            sys.exit()
        date_range_lst.append(str(from_dt))
        date_range_lst.append(str(to_dt))
        return date_range_lst

    def execute(src, token, date_range_lst):
        class_name = '{}Dao'.format(src)
        d = globals()[class_name](c.SOURCE_URL, target_dir, model_entity,
                                  permission, src, token, date_range_lst, env)
        d.iterate_instrument_and_execute()

    if __name__ == '__main__':

        args = cmdline_args()
        global src, target_dir, model_entity, permission, env
        src = args.source
        token = args.token
        target_dir = args.output_directory
        model_entity = args.model_entity
        permission = args.permission
        date_range_lst = get_date_range(args.date)
        env = args.env

        if src == "MothersPaperQ":
            execute(src, token, date_range_lst)
        elif src == "FathersPaperQ":
            execute(src, token, date_range_lst)
        elif src == "ChildConsentFullOriginsParticipation":
            execute(src, token, date_range_lst)
        elif src == "ChildRoutineDataConsent":
            execute(src, token, date_range_lst)
        elif src == "MaternalConsentFullOrigins":
            execute(src, token, date_range_lst)
        elif src == "PaternalConsentFullOrigins":
            execute(src, token, date_range_lst)
        elif src == "MaternalRoutineDataConsent":
            execute(src, token, date_range_lst)
        elif src == "PaternalRoutineDataConsent":
            execute(src, token, date_range_lst)
        elif src == "DigiConsentPaternalFullOriginsParticipation":
            execute(src, token, date_range_lst)
        elif src == "JhcOriginsMotherDigiQuest":
            execute(src, token, date_range_lst)
        elif src == "JhcOriginsPartnerDigiQuest":
            execute(src, token, date_range_lst)
        elif src == "EConsentWithdrawMaternal":
            execute(src, token, date_range_lst)
        elif src == "EConsentWithdrawPaternal":
            execute(src, token, date_range_lst)
        elif src == "Aes":
            execute(src, token, date_range_lst)
        elif src == "AgesAndStages":
            execute(src, token, date_range_lst)
        elif src == "Ids":
            execute(src, token, date_range_lst)
        elif src == "Mns":
            execute(src, token, date_range_lst)
        elif src == "Questionaires":
            execute(src, token, date_range_lst)
        elif src == "Db":
            execute(src, token, date_range_lst)
        elif src == "Screening":
            execute(src, token, date_range_lst)
        elif src == "App":
            execute(src, token, date_range_lst)
        else:
            print("Source do not exist")

