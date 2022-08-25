from abc import ABC
import pandas as pd
from io import StringIO
import time
from org.tki.common import common_constants as c
from org.tki.common.df_util import DfUtil
import requests
import os
import traceback
from pandas.errors import EmptyDataError
import csv
import subprocess


class Dao(ABC):
    DATA_SRC_PROPERTY = ''
    DATA_EXT_PROPERTY = ''
    COLUMN_PATTERN = ''
    SOURCE = ''

    def __init__(self, src_url, output_dir, model_entity, permission, src,
                 token, date_range_lst, env):
        self.data_src_property = self.DATA_SRC_PROPERTY
        self.data_ext_property = self.DATA_EXT_PROPERTY
        if src != "App":
            self.DATA_SRC_PROPERTY['token'] = token
            self.DATA_EXT_PROPERTY['token'] = token
        self.src_url = src_url
        self.list_of_instruments = ''
        self.output_dir = output_dir
        self.model_entity = model_entity
        self.permission = permission
        self.timestr = time.strftime("%Y%m%d-%H%M%S")
        self.today_date = time.strftime("%Y%m%d")
        self.src = self.SOURCE
        self.instruments = ''
        self.date_range_lst = date_range_lst
        self.host = subprocess.getoutput('hostname')
        self.env = env
        self.df_obj = DfUtil()
        self.column_pattern = self.COLUMN_PATTERN
        self.actual_rec_count = ''
        self.final_rec_count = ''

    def create_instruments_df(self):
        try:
            r = requests.post(self.src_url, data=self.data_src_property)
            instruments = pd.read_csv(StringIO(r.text))
            self.sort_instruments_df(instruments)
        except:
            print(
                'Invalid token at the {} source/issue during dataframe sort'.format(
                    self.src))
            traceback.print_exc()

    def sort_instruments_df(self, instruments):
        self.list_of_instruments = sorted(
            zip(instruments.form, instruments.unique_event_name))

    def modify_ext_property(self, instrument_name, event_name):
        self.data_ext_property['forms[0]'] = instrument_name
        self.data_ext_property['events[0]'] = event_name
        if self.date_range_lst[0] != 'None':
            self.data_ext_property['dateRangeBegin'] = self.date_range_lst[0]
            self.data_ext_property['dateRangeEnd'] = self.date_range_lst[1]
        #print('data_ext_property after modification -> {}'.format(self.data_ext_property)) ## logger.debug

    def create_data_df(self):
        r = requests.post(self.src_url, data=self.data_ext_property)
        try:
            self.results = pd.read_csv(StringIO(r.text),dtype=str)
            self.actual_rec_count = len(self.results)
        except EmptyDataError:
            self.results = pd.DataFrame()

    def drop_null_records(self):
        self.df_obj.df = self.results
        self.df_obj.drop_null_records(self.column_pattern)
        self.results = self.df_obj.get_df()
        self.final_rec_count = len(self.results)


    def enrich_data_df(self, instrument_name):
        self.drop_null_records()
        self.results[c.INSTR_NAME] = [
            instrument_name for instr in range(len(self.results))]
        self.results[c.MODEL_ENTITY] = [
            self.model_entity for instr in range(len(self.results))]
        self.results[c.PERMISSION] = [
            self.permission for instr in range(len(self.results))]

    def check_for_header_df(self):
        name_header = []
        name_header = self.results.columns.tolist()
        index = 1
        for name in name_header:
            if name_header[name_header.index(name)] not in [c.INSTR_NAME,
                                                            c.MODEL_ENTITY,
                                                            c.PERMISSION]:
                name_header[name_header.index(name)] = str(index) + "-" + name
                index += 1
        self.results.columns = name_header

    # This is overwritten by the concrete class
    def iterate_instrument_and_execute(self):
        self.create_instruments_df()
        for event_instr in self.list_of_instruments:
            if "landing" not in event_instr[0]:
                try:
                    self.trigger_execute_steps(event_instr)
                except Exception as e:
                    self.log_message(event_instr)
                    raise e

    # This is overwritten by the concrete class
    def trigger_execute_steps(self, event_instr):
        self.execute_steps(event_instr[0], event_instr[1])

    def log_message(self, event_instr):
        print("Problem with getting data from " +
              event_instr[0] + " and " + event_instr[1])

    def execute_steps(self, instrument_name, event_name=''):
        self.modify_ext_property(instrument_name, event_name)
        self.create_data_df()
        if not self.results.empty:
            self.enrich_data_df(instrument_name)
            self.check_for_header_df()
            self.write_stats(instrument_name, event_name)
            print('writing to csv')
            self.write_df_to_csv(instrument_name, event_name)

    def write_stats(self,instrument_name, event_name):
        if self.env == "local":
            c.STATS_FILE = c.STATS_FILE_LOCAL
        stat_file = c.STATS_FILE.format(self.env).replace('nifi.csv','nifi-{h}.csv'.format(h=self.host))
        f = open(stat_file,'a')
        row = '{s},{i},{e},{c},{d},{t},{a},{f}'.format(s=self.src,i=instrument_name,e=event_name,c=len(self.results),d=self.today_date,t=self.env,a=self.actual_rec_count,f=self.final_rec_count)
        f.write(f'\n{row}')
        f.close()

    def write_df_to_csv(self, instrument_name, event_name):
        if self.final_rec_count > 0:
            self.results.to_csv(os.path.join(
                self.output_dir,
                instrument_name + "_" + event_name + "_" + self.timestr + ".csv"),
                mode='a',
                index=False)


class DaoForm(Dao):
    DATA_SRC_PROPERTY = ''
    DATA_EXT_PROPERTY = ''

    def sort_instruments_df(self, instruments):
        self.list_of_instruments = sorted(
            zip(instruments.instrument_name, instruments.instrument_label))

    def modify_ext_property(self, instrument_name, event_name=''):
        print('Intrument Name -> {}'.format(instrument_name)) ## Add it as logger.debug
        self.data_ext_property['forms[0]'] = instrument_name
        if self.date_range_lst[0] != 'None':
            self.data_ext_property['dateRangeBegin'] = self.date_range_lst[0]
            self.data_ext_property['dateRangeEnd'] = self.date_range_lst[1]
        #print('data_ext_property after modification -> {}'.format(self.data_ext_property)) ## logger.debug

    def log_message(self, event_instr):
        print("Problem with getting data from " +
              event_instr[0])

    def log_message(self, event_instr):
        print("Problem with getting data from " +
              event_instr[0])

    def write_df_to_csv(self, instrument_name, event_name):
        if self.final_rec_count > 0:
            self.results.to_csv(os.path.join(
            self.output_dir,
            instrument_name + "_" + event_name + "_" + self.timestr + ".csv"),
            mode='a',
            index=False)

    def trigger_execute_steps(self, instrument_name):
        self.execute_steps(instrument_name)

