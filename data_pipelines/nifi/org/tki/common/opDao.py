import requests
from requests.auth import HTTPBasicAuth
import traceback
from org.tki.common import common_constants as c
from abc import ABC
import json
import time
import pandas as pd
from io import StringIO


class OpDao(ABC):
    END_PT = ''

    def __init__(self, env):
        self.req_json = ''
        self.output_dir = ''
        self.end_pt = self.END_PT
        self.timestr = time.strftime("%Y%m%d-%H%M%S")
        self.env = (env)
        self.df = ''
        self.out_dir = ''

    def get_request(self):
        try:
            r = requests.get(
                '{u}/{e}'.format(u=c.OP_URL, e=self.end_pt),
                auth=HTTPBasicAuth(c.OP_USER, c.OP_PASSWORD))
            self.req_json = r.json()
            #self.req_json = r.content
            print(self.req_json)
        except:
            print('Get request failed for OpenSpecimen')
            traceback.print_exc()

    def get_dataframe_for_cpId(self):
        try:
            r = requests.get(
                '{u}/{e}'.format(u=c.OP_URL, e=self.end_pt),
                auth=HTTPBasicAuth(c.OP_USER, c.OP_PASSWORD))
            self.df = pd.read_csv(StringIO(r.text),skiprows=4)
            #print(self.df)
        except:
            print('Get request failed for OpenSpecimen')
            traceback.print_exc()

    def post_request(self):
        pass

    def write_to_file(self):
        if self.env == 'local':
            out_file = '{t}.csv'.format(t=self.timestr)
        else:
            out_file = '{d}/{t}.csv'.format(d=self.out_dir, t=self.timestr)
        print(out_file)
        #exit(0)
        #json.dump(self.df, out_file)
        self.df.to_csv(out_file)

    def execute(self):
        self.get_request()
        self.post_request()
        self.write_to_file()


if __name__ == "__main__":
    op_obj = OpDao()
    op_obj.execute()
