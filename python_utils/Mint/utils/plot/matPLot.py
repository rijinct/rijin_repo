import sys
sys.path.append(r'C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\rij_git\rijin_repo\python_utils\Mint')

from df_util import DfUtil
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px

input_file=r'C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\BT\DQI\dqi.csv'

df_util_obj = DfUtil(input_file)
df_util_obj.read_csv()
df = df_util_obj.get_df()

#df.plot(y='score', x='end_time', figsize=(15,6))
#plt.show()

px.line(df, y='score', x='end_time').show()