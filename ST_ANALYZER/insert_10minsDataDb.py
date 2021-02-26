'''
Created on 13-Jun-2020

@author: rithomas

'''
import datetime
import PYTHON_OPERATIONS.time_operations as PY_OP
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df as IN_READER
import DF_OUTPUT_WRITE_OPERATIONS.various_write_operations as WRITER
from sqlalchemy import create_engine
import pandas as pd

def timeround10(dt):
    a, b = divmod(round(dt.minute, -1), 60)
    return '%i:%02i' % ((dt.hour + a) % 24, b)

def get_10thmin_data(ltt):
    date_s, time_s = ltt.split(" ")
    d_s, m_s, y_s = date_s.split("-")
    d_s = int(d_s)
    m_s = int(m_s)
    y_s = int(y_s)
    H_s, M_s, S_s = time_s.split(":")
    H_s = int(H_s)
    M_s = int(M_s)
    S_s = int(S_s)
    return timeround10(datetime.datetime(y_s, m_s, d_s, H_s, M_s, 0))

def get_hourMin(ltt):
    date_s, time_s = ltt.split(" ")
    H_s, M_s, S_s = time_s.split(":")
    H_s = int(H_s)
    M_s = int(M_s)
    S_s = int(S_s)
    return '{H_s}:{M_s}'.format(H_s=H_s,M_s=M_s)


#timestamp1=''
print(timeround10(datetime.datetime(2010, 1, 1, 12, 51, 0)))
current_date = PY_OP.get_current_date_with_timeStamp()
formatted_date = PY_OP.get_formatted_date(current_date, '%Y,%m,%d,%H,%M,%S')
y=int(PY_OP.get_formatted_date(current_date, '%Y'))
m=int(PY_OP.get_formatted_date(current_date, '%m'))
d=int(PY_OP.get_formatted_date(current_date, '%d'))
H=int(PY_OP.get_formatted_date(current_date, '%H'))
M=int(PY_OP.get_formatted_date(current_date, '%M'))

print(y)
#print(type(formatted_date_int))
print(timeround10(datetime.datetime(y,m,d,H,M,0)))

ltt = "12-06-2020 09:30:11"
hour_min10 = get_10thmin_data(ltt)
hour_min = get_hourMin(ltt)
if (hour_min10 == hour_min):
    print('success')

MARIADB_URL = 'mysql+mysqlconnector://root:zaq12wsx@127.0.0.1/testrijdb'
engine = create_engine(MARIADB_URL)
sql_query = """
SELECT * FROM st_analysis WHERE Trading_symbol LIKE '%SBI%' AND LTT LIKE '12-06%';
"""    
df_sql = IN_READER.get_df_from_sql(sql_query, engine)
print(df_sql.columns)
list_tuple = []
for ind in df_sql.index:
    LTT=df_sql['LTT'][ind]
    hour_min10 = get_10thmin_data(LTT)
    hour_min = get_hourMin(LTT)
    #print(hour_min)
    if (hour_min10 == hour_min):
        tuple = df_sql['LTT'][ind],df_sql['Trading_symbol'][ind]
        print(tuple)
        list_tuple.append(tuple)
        #df_tuple = pd.DataFrame(tuple,columns=['LTT','Trading_symbol'])
        #WRITER.insert_db_df(df_tuple, engine, 'test_table', 'testrijdb', 'append', False)
print(list_tuple)
df_tuple = pd.DataFrame(list_tuple,columns=['LTT','Trading_symbol'])
#print(df_tuple)
#WRITER.insert_db_df(df_tuple, engine, 'test_table', 'testrijdb', 'append', False)