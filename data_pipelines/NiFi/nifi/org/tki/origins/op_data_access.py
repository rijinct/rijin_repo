import time
import org.tki.origins.constants as c
from org.tki.common.opDao import OpDao
import argparse
import datetime
import calendar


class CprOpDao(OpDao):
    END_PT = 'rest/ng/scheduled-jobs/{}/runs'

    def __init__(self, cp_id, env):
        self.req_json = ''
        self.output_dir = ''
        self.end_pt = self.END_PT.format(cp_id)
        self.timestr = time.strftime("%Y%m%d-%H%M%S")
        self.env = env
        self.utc_prev_day = ''
        self.cp_id = cp_id
        self.out_dir = c.CP_OUTPUT_DIR.format(env)

    def get_latest_run_id(self):
        return self.req_json[0]['id']

    def conv_prev_date_utc(self):
        prev_date = datetime.datetime.today() - datetime.timedelta(days=1)
        prev_date_midnight = datetime.datetime.combine(prev_date, datetime.datetime.min.time())
        print(prev_date_midnight)
        self.utc_prev_day = calendar.timegm(prev_date_midnight.utctimetuple()) * 1000
        print(self.utc_prev_day)

    def check_prev_day_run(self):
        os_utc = self.req_json[0]['startedAt']
        if os_utc > self.utc_prev_day:
            return True
        else:
            print('No jobs runs for the previous day, Hence exiting!')
            return False


    def reconstruct_get_end_pt(self, run_id):
        self.end_pt = '{e}/{r}/result-file'.format(e=self.end_pt,r=run_id)
        if self.cp_id in [29,30,31]:
            self.out_dir = '{}/OpenSpecimen_visit'.format(self.out_dir)
        else:
            self.out_dir = '{}/OpenSpecimen_specimen'.format(self.out_dir)
        print(self.out_dir)

    def execute(self):
        self.get_request()
        run_id = self.get_latest_run_id()
        self.conv_prev_date_utc()
        if self.check_prev_day_run():
            self.reconstruct_get_end_pt(run_id)
            self.get_dataframe_for_cpId()
            self.write_to_file()


class OpDaoManager:

    def cmdline_args():
        # Make parser object
        p = argparse.ArgumentParser(description="""
            Extract updates for ORIGINS Database from OpenSpecimen
            """,
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        # Optional argument which requires a parameter (eg. -d test)
        # This option is used to set the permission on the Data Hub depending on the ingestion needed
        p.add_argument("-e", "--env", required=True)



        return (p.parse_args())

    if __name__ == '__main__':
        args = cmdline_args()
        env = args.env
        #d = CprOpDao(29,env)

        for cp_id in c.JOB_LIST:
            d = CprOpDao(cp_id,env)
            d.execute()
            time.sleep(5)
            #exit(0)

