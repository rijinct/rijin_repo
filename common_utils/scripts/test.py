import numpy as np
from collections import defaultdict
my_list = ['h','e']
my_array = np.array(my_list)
connector = ""
print(connector.join(my_array))

print(type(my_array))


fruits = {1:'f', 3:'a', 2:'i'}

key1 = fruits.keys()
val1 = fruits.values()

fruits1 = {}.fromkeys(key1,val1)
print(fruits1)
print(sorted(fruits))

dict_demo = defaultdict(int)
print(dict_demo[3])

## Reversing a string
stri = 'python hi'
stri2= 'hi'
print(stri[len(stri)::-1])

##
print(stri.find(stri2))