import os
from os import listdir
import sys
import time
import re
import json
import pandas as pd
import urllib2
import requests
import zipfile
from email import encoders
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
import smtplib
from smtplib import SMTPAuthenticationError


def get_non_premium_collections():
    quandl_apikey = 'o7xFVwAfWTqCsY5mgMGh'
    q_databases_url = "https://www.quandl.com/api/v3/databases?api_key={0}&page={1}"
    non_premium_collections_list = []
    for page in range(1, 4):
        q_db_URL = q_databases_url.format(quandl_apikey, str(page))
        json_data = (requests.get(q_db_URL)).json()
        json_list = json_data['databases']
        for list_element in json_list:
            if not list_element['premium']: non_premium_collections_list.append(list_element)
    return non_premium_collections_list

def get_dataset_info(collection_name, dataset, non_premium_collections_list):
    dataset_title = dataset_desc = ""
    try:
        collection_code = ""
        for list_element in non_premium_collections_list:
            list_element_name = re.sub("[^A-Za-z0-9 ]+", "", list_element['name'])
            list_element_name = re.sub(" +", " ", list_element_name).replace(" ", "_").lower()
            if list_element_name == collection_name: collection_code = list_element['database_code']
            else: continue
    
        if collection_code != "":
            quandl_apikey = 'o7xFVwAfWTqCsY5mgMGh'
            q_codes_url = "https://www.quandl.com/api/v3/databases/" + collection_code + "/codes.json?api_key=" + quandl_apikey
            zipcontent = urllib2.urlopen(q_codes_url).read()
            zip_filename = collection_code + ".zip"
            with open(zip_filename, 'wb') as zfw: zfw.write(zipcontent)
            with zipfile.ZipFile(zip_filename, "r") as zfr: zfr.extractall()
            os.remove(zip_filename)
            csv_file_generated = collection_code + "-datasets-codes.csv"
            collection_dataset = pd.read_csv(csv_file_generated, header=None)
            os.remove(csv_file_generated)
            temp_var = min(6, len(collection_dataset))
            for i in range(0, temp_var):  # range(0,len(collection_dataset)):
                dataset_json = json.loads(os.popen("curl " + "https://www.quandl.com/api/v3/datasets/" + collection_dataset.loc[i, 0] + "?api_key=" + quandl_apikey).read())
                dataset_name = re.sub("[^A-Za-z0-9 ]+", "", dataset_json['dataset']['name'])
                dataset_name = re.sub(" +", " ", dataset_name).replace(" ", "_").lower()
                if dataset_name == dataset:
                    dataset_title = dataset_json['dataset']['name']
                    dataset_desc = collection_dataset.loc[i, 1]
                else: continue
        else: pass
        return dataset_title, dataset_desc
    except:
        error_type, error_obj, error_info = sys.exc_info()
        print error_type, 'Line:', error_info.tb_lineno
        return dataset_title, dataset_desc

def get_collection_info(collection_name, non_premium_collections_list):
    collection_description = collection_name_actual = collection_url = ""
    try:
        for list_element in non_premium_collections_list:
            list_element_name = re.sub("[^A-Za-z0-9 ]+", "", list_element['name'])
            list_element_name = re.sub(" +", " ", list_element_name).replace(" ", "_").lower()
            if list_element_name == collection_name:
                collection_description = list_element['description']
                collection_name_actual = list_element['name']
                collection_url = list_element['url_name']
            else: continue
        return collection_description, collection_name_actual, collection_url
    except:
        error_type, error_obj, error_info = sys.exc_info()
        print error_type, 'Line:', error_info.tb_lineno
        return collection_description, collection_name_actual, collection_url


def send_notification_email():
    fromaddr = "rvanipenta@randomtrees.com"
    toaddr = ["rvanipenta@randomtrees.com"]

    try:
        for mail_to in toaddr:
            msg = MIMEMultipart()
            msg['From'] = fromaddr
            msg['To'] = mail_to
            msg['Subject'] = "Auto Generated Mail: Dataset Inventory List Is Ready - Quandl"
            body = "Please find the attached doc"
            msg.attach(MIMEText(body, 'plain'))
            filename = "datasets_inventory_summary_quandl.xlsx"
            attachment = open(os.getcwd() + "\\datasets_inventory_summary_quandl.xlsx", "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
            msg.attach(part)
            server = smtplib.SMTP('smtp-mail.outlook.com', 587)
            server.starttls()
            server.login(fromaddr, "Rtrees2806")
            text = msg.as_string()
            server.sendmail(fromaddr, mail_to, text)
            del msg
        server.quit()
        print "Email Notification Sent.."
    except SMTPAuthenticationError:
        print "Authentication Failure.."
        pass
    except Exception:
        print "No Email Notification Sent.."
        error_type, error_obj, error_info = sys.exc_info()
        print error_type, 'Line:', error_info.tb_lineno
        pass
        
def main():
    df = pd.DataFrame()
    i = 0
    mypath = os.getcwd() + "\\datasets"
#    non_premium_collections_list = get_non_premium_collections()

    try:
        for datasource in listdir(mypath):
            if "quandl" in datasource:
                try:
                    for collection_name in listdir(os.path.join(mypath, datasource)):
                        print i,"-",collection_name," in", datasource, i
#                        collection_description, collection_name_actual, collection_url = get_collection_info(collection_name, non_premium_collections_list)
                        for dataset in listdir(os.path.join(mypath, datasource, collection_name)):
#                            dataset_title, dataset_desc = get_dataset_info(collection_name, dataset,non_premium_collections_list)
                            for files in listdir(os.path.join(mypath, datasource, collection_name, dataset)):
                                if (files.endswith(".csv") or files.endswith(".json") or files.endswith(".jsonl")):
                                    df.loc[i, 'datasource_name'] = "Quandl"
#                                    df.loc[i, 'collection_name_actual'] = collection_name_actual
                                    df.loc[i, 'collection_name_formatted'] = collection_name
#                                    df.loc[i, 'collection_description'] = collection_description
                                    df.loc[i, 'process_area'] = ""
                                    df.loc[i, 'dataset_name_formatted'] = re.sub("_+", "_",re.sub("[^A-Za-z0-9'_]+", "", dataset))
                                    df.loc[i, 'dataset_name_in_yaml'] = "cortex/" + re.sub("[^A-Za-z0-9'_]+", "",dataset).replace("-", "_")
#                                    df.loc[i, 'dataset_title_in_yaml'] = dataset_title
#                                    df.loc[i, 'dataset_desc'] = dataset_desc
                                    df.loc[i, 'industry'] = "Finance"
#                                    df.loc[i, 'url'] = "https://www.quandl.com/data/" + collection_url
                                    df.loc[i, 'file_name'] = files
                                    df.loc[i, 'file_format'] = files.split(".")[-1]
                                    df.loc[i, 'size_in_mb'] = os.path.getsize(os.path.join(mypath, datasource, collection_name, dataset, files)) / float(1024 * 1024)
                                    i += 1
                    num_files = 0; num_folders = 0
                    for _, dirnames, filenames in os.walk(os.path.join(mypath, datasource)):
                        num_folders += len(dirnames); num_files += len(filenames)
                    print "Processed: ", num_files, "files in ", datasource

                except WindowsError:
                    error_type, error_obj, error_info = sys.exc_info()
                    print error_type, 'Line:', error_info.tb_lineno
                    continue
        datasets_inventory_summary = pd.ExcelWriter(os.getcwd() + "\\datasets_inventory_summary_quandl_1.xlsx",engine='xlsxwriter')
        df.to_excel(datasets_inventory_summary, 'List')
        try:
            datasets_inventory_summary.save()
        except IOError:
            error_type, error_obj, error_info = sys.exc_info()
            print error_type, 'Line:', error_info.tb_lineno
            print "File named 'datasets_inventory_summary_quandl.xlsx' is already open in your system, please Close it and try again..."
            pass
        except:
            error_type, error_obj, error_info = sys.exc_info()
            print error_type, 'Line:', error_info.tb_lineno
            pass
    except WindowsError:
        error_type, error_obj, error_info = sys.exc_info()
        print error_type, 'Line:', error_info.tb_lineno
        pass

if __name__ == "__main__":
    prog_run_time = time.clock()
    main()
#    send_notification_email()
    print "\nProcessed time: ", (time.clock() - prog_run_time) / 60, " Min."