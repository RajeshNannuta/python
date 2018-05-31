# -*- coding: utf-8 -*-

import os
import logging
import requests
import zipfile
import urllib2
import json
#from config import mongo_config
from os import walk
import time
from datetime import datetime
from pget.down import Downloader
import re

quandl_apikey =  'o7xFVwAfWTqCsY5mgMGh'#"zWss8KsbxmzVojqwVr9E"
path = os.getcwd()
DEFAULT_DATA_PATH = path+"/data"
rootfolder = path+"/quandl_data"
def getCodesInCSVsForAllDatasets(quandl_apikey):
    logging.basicConfig(format='%(asctime)s %(levelname)s \
                        %(module)s.%(funcName)s :: %(message)s',
                        level=logging.INFO)

    q_db_base_url = "https://www.quandl.com/api/v3/databases"
    q_databases_url = q_db_base_url + "?api_key={0}&page={1}"
    q_codes_url = q_db_base_url + "/{0}/codes.json?api_key={1}"

    page = 0
    database_codes = []
    premium_codes = []
    total_codes = 0
    json_data = {}
    folder_hierchy = {}
    #quandl_datsetinfo = {}
    try:
        while len(database_codes) == 0 \
                or json_data['meta']['total_count'] > total_codes:
            page += 1
            q_db_URL = q_databases_url.format(quandl_apikey, str(page))
            
            json_data = (requests.get(q_db_URL)).json()
            
            for d in json_data['databases']:
                if not d['premium']:
                    folder_hierchy[d['database_code']] = d['name']
                    database_codes.append(d['database_code'])
                if d['premium']:
                    premium_codes.append(d['database_code'])

            total_codes = len(database_codes) + len(premium_codes)
        print len(database_codes)
        
        '''with open('quandlinfo.json', 'a+') as fp:
            json.dump(json_data['databases'], fp, indent=4)
        
        for code in database_codes:
            print "CODE>> "+code
            zip_filename = code + '-datasets-codes.zip'
            if not os.path.isfile(zip_filename):
                try:
                    print "downloading..."+zip_filename
                    time.sleep(3)
                    resp = urllib2.urlopen(q_codes_url.format(code, quandl_apikey))
                    zipcontent = resp.read()
        
                    with open(zip_filename, 'wb') as zfw:
                        zfw.write(zipcontent)
                except:
                    continue

            with zipfile.ZipFile(zip_filename, "r") as zfr:
                zfr.extractall(DEFAULT_DATA_PATH)'''
        return folder_hierchy
    except:
        raise

def readExtractedfiles():
    #print getCodesInCSVsForAllDatasets(quandl_apikey)
    f_list = []
    folderconvey = getCodesInCSVsForAllDatasets(quandl_apikey)
    for key in folderconvey.keys():
        f_list.append(key)
    #print f_list
    q_data_base_URL = "https://www.quandl.com/api/v3/datasets/{0}"

    filenamesList = []
    for (dirpath, dirnames, filenames) in walk(DEFAULT_DATA_PATH):
        filenamesList.extend(filenames)
    try:
        for fn in filenamesList:
            print fn
            try:
                dataset_qcodes = []
                logging.info(fn + " extracted.")
                codesFile = os.path.abspath(os.path.join(DEFAULT_DATA_PATH, fn))
                with open(codesFile, 'r') as csv_file:
                    csvlines = csv_file.readlines()
                    
                    for num, line in enumerate(csvlines[:5]):
                        codeline = line.split(',')
                        if len(codeline) > 1:
                            dataset_code = codeline[0]
                            
                            dataset_descrpn = codeline[1]
                            download_url = q_data_base_URL.format(dataset_code)
                            
                            data_URL = download_url+"?api_key="+quandl_apikey
                            time.sleep(1)
                            resp = os.popen("curl " +data_URL)
                            resp_data = resp.read()
                            json_data = json.loads(resp_data)
                            
                            #folderconvey = getCodesInCSVsForAllDatasets(quandl_apikey)
                            
                            foldername = json_data["dataset"]["name"]
                            dat_code = json_data["dataset"]["database_code"]
                            #foldername = (foldername.replace('-', '').replace(' ', '_')).lower()
                            foldername = re.sub("[^A-Za-z0-9 ]+", "", foldername)
                            foldername = re.sub(" +"," ",foldername).replace(" ", "_").lower()
                            print ">>>>>>>"+foldername
                            for name in f_list:
                                if name == dat_code:
                                    out_fldr_name =  folderconvey[name]
                                    out_fldr_name = re.sub("[^A-Za-z0-9 ]+", "", out_fldr_name)
                                    out_fldr_name = re.sub(" +"," ",out_fldr_name).replace(" ", "_").lower()
                            try:      
                                os.chdir(rootfolder)
                                if not os.path.isdir(out_fldr_name):
                                    os.mkdir(out_fldr_name)
                                os.chdir(out_fldr_name)
                                if not os.path.isdir(foldername):
                                    os.mkdir(foldername)
                                os.chdir(foldername)
                            except WindowsError:
                                continue
                            fileformat = ".csv"
                            if not os.path.isfile(dataset_code.split('/')[1]+'-datasets-codes'+fileformat):
                                urll = download_url+"/data.csv"
                                
                                downloader = Downloader(urll, dataset_code.split('/')[1]+fileformat , 8)
                                downloader.start()
                                downloader.wait_for_finish()
            except:
                raise
                continue
    except:
        pass
        
             

def main():
    print "copied code executed sucessfully"
    #print getCodesInCSVsForAllDatasets(quandl_apikey)
    print readExtractedfiles()
    
    
if __name__ == '__main__':
    main()
