# -*- coding: utf-8 -*-
"""duplicate_image_remove.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/17q-vNL4HQoKg7907A4SogxPYImqJlMD-
"""

# Commented out IPython magic to ensure Python compatibility.
import hashlib
from scipy import misc
from scipy.misc import imread, imresize, imshow
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
# %matplotlib inline
import time
import numpy as np

"""# Removing Duplicate Images Using Hashing"""

def file_hash(filepath):
    with open(filepath, 'rb') as f:
        return md5(f.read()).hexdigest()

import os

os.getcwd()

os.chdir(r"C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\screenshots")
print(os.getcwd())

file_list = os.listdir()
print(len(file_list))

import hashlib, os
duplicates = []
hash_keys = dict()
for index, filename in  enumerate(os.listdir('.')):  #listdir('.') = current directory
    if os.path.isfile(filename):
        with open(filename, 'rb') as f:
            filehash = hashlib.md5(f.read()).hexdigest()
        if filehash not in hash_keys: 
            hash_keys[filehash] = index
        else:
            duplicates.append((index,hash_keys[filehash]))

print(duplicates)
print('rijin')
for file_indexes in duplicates[:30]:
    try:
    
        plt.subplot(121),plt.imshow(imread(file_list[file_indexes[1]]))
        plt.title(file_indexes[1]), plt.xticks([]), plt.yticks([])

        plt.subplot(122),plt.imshow(imread(file_list[file_indexes[0]]))
        plt.title(str(file_indexes[0]) + ' duplicate'), plt.xticks([]), plt.yticks([])
        plt.show()
    
    except OSError as e:
        continue

"""# Delete Files After Printing"""

for index in duplicates:
    os.remove(file_list[index[0]])