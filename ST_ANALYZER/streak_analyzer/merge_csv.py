import os
import glob
import pandas as pd

month="Jan"
month_val="2"
year="2021"

streak_dir=r"C:\Users\admin\Desktop\Rijin\data\15mins\{y}\{m}\Strategy1".format(y=year,m=month)
print(streak_dir)
#cd "$streak_dir"
os.chdir(streak_dir)

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

#combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
#export to csv

combined_csv.to_csv( '{m}.{e}'.format(m=month,e='csv'), index=False, encoding='utf-8-sig')