# -*- coding: utf-8 -*-
"""
Created on Tue May 15 14:45:44 2018

@author: ADMIN
"""

import os
# from os import walk, listdir
from os.path import isfile, join

root_folder = os.path.abspath(os.path.join(
                os.path.dirname('__file__'),
                'datagov_source_datasets', 'local'))
os.chdir(root_folder)
mypath= os.getcwd()
print(mypath)

files={}
for dirnames in os.walk(mypath):
   for dirname in dirnames[1]:
       dataset_path = mypath + "\\" + dirname
       files[dirname] = [f for f in os.listdir(dataset_path) \
                            if isfile(join(dataset_path, f))]

empty_folders = []
for k1 in files:
   if files[k1]!=[]:
       print(k1)
   if files[k1]==[]:
       empty_folders.append(k1)
       os.removedirs(k1)

print("==================================")
print("EMPTY FOLDERS >>>> " + str(len(empty_folders)))
print(empty_folders)
