import os
from sqlalchemy import create_engine
from com.rijin.dqhi import constants
import pandas as pd
import os

if ((constants.ENABLE_LOCAL_EXECUTION is False) and (os.getenv('IS_K8S') != 'true')):
    exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(", "(\"").replace(")", "\")"))

class dfDbUtil:

    def __init__(self):
        if constants.ENABLE_LOCAL_EXECUTION is True:
            db_name = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test/SAIREPO.db"))
            self.engine = create_engine("sqlite:///%s" % db_name, execution_options={"sqlite_raw_colnames": True})
        else:
            if os.getenv('IS_K8S') == 'true':
                sqlite_url = 'sqlite:///{}'.format(constants.DQHI_SQLITE_DB)
                self.engine = create_engine(sqlite_url)
            else:
                postgres_url = 'postgresql://{project_application_sdk_database_linux_user}:@{project_postgres_sdk_active_fip_host}:{project_application_sdk_db_port}/{project_sdk_db_name}'.format(project_application_sdk_database_linux_user=project_application_sdk_database_linux_user, project_postgres_sdk_active_fip_host=project_postgres_sdk_active_fip_host, project_application_sdk_db_port=project_application_sdk_db_port, project_sdk_db_name=project_sdk_db_name)
                self.engine = create_engine(postgres_url)         
        self.pd = pd
        
    def get_sql_result_df(self, query): 
        return self.pd.read_sql(query, self.engine).applymap(lambda s:s.lower() if type(s) == str else s)
 
    def insert_db_df(self, df, table_name, schema, action, index):
        if constants.ENABLE_LOCAL_EXECUTION is True or (os.getenv('IS_K8S') == 'true'):
            df.to_sql(table_name, self.engine, if_exists=action, index=index)
        else:
            df.to_sql(table_name, self.engine, schema=schema, if_exists=action, index=index)

