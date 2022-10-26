import argparse
import os
import sys
import datetime
import org.tki.concord.constants as sc
from org.tki.common.dao import Dao



class ConcordRedcapDao(Dao):
    DATA_SRC_PROPERTY = sc.DATA_SRC_CONCORD_REDCAP
    DATA_EXT_PROPERTY = sc.DATA_EXT_CONCORD_REDCAP
    COLUMN_PATTERN = sc.COLUMN_PATTERN_CONCORD_REDCAP
    SOURCE = sc.SOURCE_CONCORD_REDCAP

    def enrich_data_df(self, instrument_name):
        self.drop_null_records()
        self.results[sc.INSTR_NAME] = [
        'concord_{}'.format(instrument_name) for instr in range(len(self.results))]
        self.results[sc.MODEL_ENTITY] = [
            self.model_entity for instr in range(len(self.results))]
        self.results[sc.PERMISSION] = [
            self.permission for instr in range(len(self.results))]


class ConCordDaoManager():

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
        datetimeFormat = sc.DATE_FORMAT
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
        d = globals()[class_name](sc.CONCORD_SOURCE_URL, target_dir, model_entity,
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

        if src == "ConcordRedcap":
            execute(src, token, date_range_lst)
        else:
            print("Source do not exist")

