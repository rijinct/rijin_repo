'''
Created on 25-Apr-2020

@author: rithomas
'''
import sys
sys.path.insert(0, r"C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\scripts")
from df_excel_util import dfExcelUtil
import st_constants

def read_input_technicals_excel(file):
    df_excel_util_obj = dfExcelUtil(file,0,0)
    df =  df_excel_util_obj.get_df_excel()
    return df
    
    
if __name__ == '__main__':
    
    file = st_constants.ST_INPUT_TECHNICAL_EXCEL
    print(read_input_technicals_excel(file))