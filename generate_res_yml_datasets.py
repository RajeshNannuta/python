# -*- coding: utf-8 -*-
"""
Created on Tue Jun 05 16:01:02 2018

@author: ADMIN
"""
import json
import os
import yaml,random

root_folder = os.getcwd()
for high_level_folders in os.listdir(root_folder):
    if os.path.isdir(high_level_folders):
        for eachfldr in os.listdir(os.path.join(root_folder,high_level_folders)):
            low_level_folders = os.path.join(root_folder,high_level_folders,eachfldr)
#            print low_level_folders
            if "dataset.yaml" in os.listdir(low_level_folders):
                with open(low_level_folders+"\\dataset.yaml") as files:
                    data = yaml.load(files)
                resource_yaml = {
                            "name": data["name"],
                            "title": data["title"],
                            "description": data["description"],
                            "type": {
                                "name": "dataset"
                            }, "icon": "http://icon.png",
                            "price": {
                                "unit": "CCU",
                                "value": "00"
                            }, "tutorials": [
                                {
                                    "title": "Tutorial Title",
                                    "description": "Tutorial Description",
                                    "videoLink": "http://video.avi"
                                }
                            ],
                            "tags": data["tags"]
                        }
                '''file = open(low_level_folders+"\\resource.yaml","w")              
                json.dump(resource_yaml, file, indent=4)'''
            else:
               for next_level in os.listdir(low_level_folders):
                   next_level_fldr = os.path.join(low_level_folders,next_level)
                   if "dataset.yaml" in os.listdir(next_level_fldr):
                        with open(next_level_fldr+"\\dataset.yaml") as files:
                            data = yaml.load(files)
                        resource_yaml = {
                            "name": data["name"],
                            "title": data["title"],
                            "description": data["description"],
                            "type": {
                                "name": "dataset"
                            }, "icon": "http://icon.png",
                            "price": {
                                "unit": "CCU",
                                "value": "00"
                            }, "tutorials": [
                                {
                                    "title": "Tutorial Title",
                                    "description": "Tutorial Description",
                                    "videoLink": "http://video.avi"
                                }
                            ],
                            "tags": data["tags"]
                        }
                        file = open(next_level_fldr+"\\resource.yaml","w")              
                        json.dump(resource_yaml, file, indent=4)                    