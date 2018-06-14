# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 15:50:27 2018

@author: RAVITHEJA
"""

import requests
import urllib
import os.path
import pandas as pd
from time import sleep
from MongodbConnector import mongodbConnector
from pget.down import Downloader

root_folder = os.path.abspath(os.path.join(os.path.dirname('__file__'),
                                'datagov_source_datasets', 'local'))

try:
    mongo = mongodbConnector()
except:
    raise


def fetchGovData(domain, dataset_count, datasets_colln):
    url = "http://catalog.data.gov/api/3/action/package_list?q={0}&rows=1000&start="

    start_val = 0

    while True:
        req_url = url.format(domain) + str(start_val)
        print(req_url)

        r = requests.get(req_url)
        json_resp = r.json()
        results = json_resp["result"]["results"]

        if len(results) > 0:
            mongo.bulk_mongo_insert(datasets_colln, results)
        else:
            print("NO MORE DATASETS TO DOWNLOAD.")
            break

        start_val += 1000

    print("Available datasets in this domain are :: " +
          str(json_resp["result"]["count"]))


def extractFromJSON(domain, datasets_colln):
    datasets_cursor = datasets_colln.find()

    print(datasets_cursor)
    #files_download = 10
    for dataset in datasets_cursor:
        """if files_download == 0:
            break
        files_download = files_download - 1"""
        
        dataset_name = dataset["name"]
        print(">>>> " + dataset_name)
        
        available_formats = {}
        res_format = None
        res_urls = {}
        other_formats = {}
        for i, res in enumerate(dataset["resources"]):
            available_formats[i] = {
                    "format": res["format"],
                    "url" : res["url"],
                    "filename" : res["id"]
                    }

        #print(available_formats)
        for a in available_formats.values():
            if 'JSONL' in a["format"]:
                res_format = 'JSONL'
                break
            elif 'jsonl' in a["format"]:
                res_format = 'jsonl'
                break
            elif 'CSV' in a["format"]:
                res_format = 'CSV'
                break
            elif 'csv' in a["format"]:
                res_format = 'csv'
                break
            elif 'JSON' in a["format"]:
                res_format = 'JSON'
                break
            elif 'json' in a["format"]:
                res_format = 'json'
                break
            """elif 'XLS' in a["format"]:
                res_format = 'XLS'
                break
            elif 'xls' in a["format"]:
                res_format = 'xls'
                break
            else:
                other_formats[a["url"]] = a["filename"]
                with open("fileformats.txt", "a+") as fileformats:
                    fileformats.write(a["filename"] + ',' + a["format"] + ',' \
                                      + a["url"] + '\n')"""

        if res_format is None:
            continue
            #res_urls = other_formats
        else:
            #continue
            for a in available_formats.values():
                if res_format in a["format"]:
                    res_urls[a["url"]] = a["filename"]

        os.chdir(root_folder)

        if not os.path.isdir(dataset_name):
            os.mkdir(dataset_name)
        os.chdir(dataset_name)

        print(str(res_format) + " :: " + str(len(res_urls)))

        for res_url in res_urls.keys():
            if res_format is None:
                file_name = res_urls[res_url]
            else:
                file_name = res_urls[res_url] + "." + (res_format).lower()

            print("Downloading... " + file_name)
            #print("... from >> " + res_url)
            try:
                if not os.path.isfile(file_name):
                    sleep(1)
                    downloader = Downloader(res_url, file_name, 8)
                    downloader.start()
                    downloader.wait_for_finish()
                    
                    """resp = urllib.request.urlopen(res_url)
                    resp_content = resp.read()
                    print("Writing...")
                    with open(file_name, 'wb') as res_file:
                        res_file.write(resp_content)"""
            except:
                print("Error @ " + dataset_name)
                continue
            

def main():

    datasets_colln = mongo.initialize_mongo('health')

    #domains = ["health", "finance", "manufacturing", "consumer", "climate", 
    # "local", "energy"]
    domains = ["local"]

    for d in domains:
        #fetchGovData(d, 1000, datasets_colln)
        extractFromJSON(d, datasets_colln)


if __name__ == "__main__":
    main()
