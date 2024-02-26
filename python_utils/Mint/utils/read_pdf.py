
import fitz
from io import StringIO
import pandas as pd
import os
import re

DIGITIZED_FILE = r"C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\BT\data\PDF_1505023\20210617.pdf"
#SCANNED_FILE = "replace_this_with_your_own_path_to_file.pdf"
global df2
#df2 = pd.DataFrame()
text2=''
with fitz.open(DIGITIZED_FILE) as doc:
    for page in doc:
        text = page.get_text("text")
        #text = text.split("\n")
        #print(text.encode("utf-8"))
        #print(text)
        #print(re.findall(r'\d\d/\d\d', text))
        shipto_re=re.compile(r"\d\d/\d\d((?:.*\n){1,3})")
        shipto = [x.strip() for x in shipto_re.findall(text)]
        print(shipto)
        #break
        # df = [ text.encode("utf-8") ]
        # mat = [n.split(';') for n in df]
        # print(mat) 


        #pd.read_csv(StringIO(aString), sep='\t', header=None)   

        # data = list(map(lambda x: x.split(','),text.split("\r\n")))
        # df3 = pd.DataFrame(data[1:], columns=data[0])
        # print(df3)
        # df2.append(df3)
        # print(df2)
        #
        #text2 += text + os.linesep


