import pandas as pd 
  
# Define a dictionary containing employee data 
data = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'], 
        'Age':[27, 24, 22, 32], 
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'], 
        'Qualification':['Msc', 'MA', 'MCA', 'Phd']} 
  
# Convert the dictionary into DataFrame  
df = pd.DataFrame(data)
col_list =['Name','Address']
df[col_list] = df[col_list].applymap(lambda s:s.lower() if type(s) == str else s)
print(df)
  
# select two columns 
#print(df) 
#Output: