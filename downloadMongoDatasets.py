# -*- coding: utf-8 -*-
"""
Created on Tue May 08 17:01:25 2018

@author: RAVITHEJA
"""

import os
import json
from MongodbConnector import mongodbConnector
from config import mongo_config
from bson import json_util


def main():
    db_name = mongo_config.get('db_name')  # + '2'

    mongodb = mongodbConnector()
    colln_names = mongodb.get_collection_names()

    root_folder = os.path.abspath(os.path.join(
                    os.path.dirname('__file__'), '', db_name))

    if not os.path.isdir(db_name):
        os.mkdir(db_name)

    for colln in colln_names:
        os.chdir(root_folder)
        mongo_colln = mongodb.initialize_mongo(colln)
        print colln + " ("+ str(mongo_colln.count()) + ")"

        if mongo_colln.count() == 0:
            continue

        file_name = colln[:60]
        if not os.path.isdir(file_name):
            os.mkdir(file_name)
        os.chdir(file_name)
        cwd = os.getcwd()
        filesInDir = os.listdir(cwd)
        if len(filesInDir) > 0:
            print str(len(filesInDir)) + " files present "
            continue

        try:
            data_list = []
            has_written = False
            j = 0
            for i in mongo_colln.find():
                #print i
                data_list.append(i)
                if len(data_list) == 100000:
                    with open(file_name + '_' + str(j) + '.json', 'w') as colln_file:
                        colln_file.write(json.dumps(data_list, indent=4,
                                                    default=json_util.default))
                    data_list = []
                    has_written = True
                    j += 1

            if len(i) > 0:
                if not has_written:
                    with open(file_name+'.json', 'w') as colln_file:
                        colln_file.write(json.dumps(data_list, indent=4,
                                                    default=json_util.default))
                else:
                    with open(file_name + '_' + str(j) +'.json', 'w') as colln_file:
                        colln_file.write(json.dumps(data_list, indent=4,
                                                    default=json_util.default))
        except:
            raise


if __name__ == "__main__":
    main()
