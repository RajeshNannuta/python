import os
from os import listdir
import sys
import time
import re
import pandas as pd
import urllib2
from BeautifulSoup import BeautifulSoup
from email import encoders
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
import smtplib
from smtplib import SMTPAuthenticationError


def get_crowdFlower():
    browser = urllib2.build_opener()
    browser.addheaders = [('User-agent', 'Mozilla/5.0')]
    url = "https://www.figure-eight.com/data-for-everyone/"
    temp_df = pd.DataFrame()
    try:
        response = browser.open(url)
        html = response.read()
        bsObj = BeautifulSoup(html)
        list_element = bsObj.findAll("div", {"class": "item d4e-item"})
        i = 0
        for alpha in list_element:
            try:
                temp_df.loc[i, 'title_scrapped'] = (
                    re.sub('&#[0-9]+[;]', "", alpha.find("h3").text).replace(":", "")).lower().title()
            except:
                temp_df.loc[i, 'title_scrapped'] = ""
            try:
                temp_df.loc[i, 'description_scrapped'] = (
                    re.sub('&#[0-9]+[;]', "", alpha.find("p").text).replace(":", "")).lower().title()
            except:
                temp_df.loc[i, 'description_scrapped'] = ""
            i += 1
        return temp_df
    except Exception:
        error_type, error_obj, error_info = sys.exc_info()
        print error_type, 'Line:', error_info.tb_lineno
        temp_df["title_scrapped"] = ""
        temp_df["description_scrapped"] = ""
        return temp_df


def send_notification_email():
    fromaddr = "rvanipenta@randomtrees.com"
    toaddr = ["rvanipenta@randomtrees.com","jchaparala@randomtrees.com","rburugu@randomtrees.com","rnannuta@randomtrees.com"]
    try:
        for mail_to in toaddr:
            msg = MIMEMultipart()
            msg['From'] = fromaddr
            msg['To'] = mail_to
            msg['Subject'] = "Auto Generated Mail: Dataset Inventory List Is Ready - CrowdFlower"
            body = "Please find the attached doc"
            msg.attach(MIMEText(body, 'plain'))
            filename = "datasets_inventory_summary_crowdflower.xlsx"
            attachment = open(os.getcwd() + "\\datasets_inventory_summary_crowdflower.xlsx", "rb")
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
        error_type, error_obj, error_info = sys.exc_info()
        print error_type, 'Line:', error_info.tb_lineno
        print "Authentication Failure.."
    except SMTPAuthenticationError:
        error_type, error_obj, error_info = sys.exc_info()
        print error_type, 'Line:', error_info.tb_lineno
        pass


def main():
    df = pd.DataFrame()
    i = 0
    mypath = os.getcwd() + "\\datasets"

    alpha_df = get_crowdFlower()
    alpha_df = alpha_df.sort_values("title_scrapped")
    alpha_df.index = range(0, len(alpha_df))

    try:
        for datasource in listdir(mypath):
            if "crowd" in datasource:
                print "Currently Processing: ", datasource
                try:
                    for dataset in listdir(os.path.join(mypath, datasource)):
                        for files in listdir(os.path.join(mypath, datasource, dataset)):
                            if (files.endswith(".csv") or files.endswith(".json") or files.endswith(".jsonl")):
                                df.loc[i, 'datasource_name'] = datasource.replace("_datasets","")
                                df.loc[i, 'process_area'] = ""
                                df.loc[i, 'dataset_name_in_git'] = re.sub("_+", "_",re.sub("[^A-Za-z0-9'_]+", "", dataset))
                                df.loc[i, 'dataset_name_in_yaml'] = "cortex/" + re.sub("[^A-Za-z0-9'_]+", "",dataset).replace("-", "_")
                                df.loc[i, 'industry'] = ""
                                df.loc[i, 'title_scrapped'] = ""
                                df.loc[i, 'description_scrapped'] = ""
                                df.loc[i, 'orginization_name'] = ""
                                df.loc[i, 'publisher_name'] = ""
                                df.loc[i, 'url'] = "https://catalog.data.gov/dataset/" + re.sub("[^A-Za-z0-9'_]+", "",dataset)
                                df.loc[i, 'file_name'] = re.sub("[^A-Za-z0-9'_]+", "", files)
                                df.loc[i, 'file_format'] = files.split(".")[-1]
                                df.loc[i, 'size_in_mb'] = os.path.getsize(os.path.join(mypath, datasource, dataset, files)) / float(1024 * 1024)
                                i += 1

                    num_files = 0;num_folders = 0
                    for _, dirnames, filenames in os.walk(mypath + "\\" + datasource):
                        num_folders += len(dirnames)
                        num_files += len(filenames)
                    print "Processed: ", num_folders, "datasets and ", num_files, "files in ", datasource

                except WindowsError:
                    error_type, error_obj, error_info = sys.exc_info()
                    print error_type, 'Line:', error_info.tb_lineno
                    continue
            else:
                print "Skipped: ", datasource, ",Only Processing crowdFlower datasets.."

        df = df.sort_values("dataset_name_in_git")
        df["title_scrapped"] = alpha_df["title_scrapped"]
        df["description_scrapped"] = alpha_df["description_scrapped"]

    except WindowsError:
        error_type, error_obj, error_info = sys.exc_info()
        print error_type, 'Line:', error_info.tb_lineno
        pass

    datasets_inventory_summary = pd.ExcelWriter(os.getcwd() + "\\datasets_inventory_summary_crowdflower.xlsx",engine='xlsxwriter')
    df.to_excel(datasets_inventory_summary, 'List')
    try:
        datasets_inventory_summary.save()
    except IOError:
        error_type, error_obj, error_info = sys.exc_info()
        print error_type, 'Line:', error_info.tb_lineno
        print "File named 'datasets_inventory_summary_crowdflower.xlsx' is already open in your system, please Close it and try again..."
    except:
        error_type, error_obj, error_info = sys.exc_info()
        print error_type, 'Line:', error_info.tb_lineno


if __name__ == "__main__":
    prog_start_time = time.clock()
    main()
    send_notification_email()
    print "\nProcessed time: ", (time.clock() - prog_start_time) / 60, " Min."