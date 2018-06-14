import os
from os import listdir
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

def get_industry_datagov(datasource):
    if "fin" in datasource: return "Finance"
    elif "health" in datasource: return "Healthcare"
    elif "manu" in datasource: return "Manufacturing"
    elif "consumer" in datasource: return "Commerce"
    elif "climate" in datasource: return "Climate"
    elif "energy" in datasource: return "Energy"
    elif "local" in datasource: return "Local Govt"
    else: return ""

def web_request_datagov(dataset_name):
    browser=urllib2.build_opener()
    browser.addheaders=[('User-agent', 'Mozilla/5.0')]
    url = "https://catalog.data.gov/dataset/" + dataset_name
    response=browser.open(url)
    html = response.read()
    bsObj = BeautifulSoup(html)
    return bsObj

def get_title_datagov(dataset_name):
    try:
        bsObj = web_request_datagov(dataset_name)
        scrapped_title = bsObj.find("section",{"class":"module-content"}).find("h1").text
        scrapped_title = re.sub('[^A-Za-z0-9]+', ' ', scrapped_title)
        return scrapped_title.encode("utf-8")
    except Exception:
        return re.sub('[^A-Za-z0-9]+', ' ', dataset_name).encode("utf-8")

def get_description_datagov(dataset_name):
    try:
        bsObj = web_request_datagov(dataset_name)
        scrapped_desc = bsObj.find("div", {"class": "notes embedded-content"}).find("p").text
        scrapped_desc = re.sub('[^A-Za-z0-9]+',' ', scrapped_desc)
        return scrapped_desc.encode("utf-8")
    except Exception:
        return re.sub('[^A-Za-z0-9]+',' ', dataset_name).encode("utf-8")

def get_publisher_datagov(dataset_name):
    try:
        bsObj = web_request_datagov(dataset_name)
        orginization_name = bsObj.find("div",{"role":"main"}).find("li",{"class":"home"}).findNextSiblings()[1].text
        publisher_name = bsObj.find("div", {"role": "main"}).find("li", {"class": "home"}).findNextSiblings()[2].text
        return orginization_name,publisher_name.replace("&nbsp;","")
    except:
        return "",""

def send_notification_email():
    fromaddr = "rvanipenta@randomtrees.com"
    toaddr = ["rvanipenta@randomtrees.com","jchaparala@randomtrees.com","rburugu@randomtrees.com","rnannuta@randomtrees.com"]
    try:
        for mail_to in toaddr:
            msg = MIMEMultipart()
            msg['From'] = fromaddr
            msg['To'] = mail_to
            msg['Subject'] = "Auto Generated Mail: Dataset Inventory List Is Ready - DataGov"
            body = "Please find the attached doc"
            msg.attach(MIMEText(body, 'plain'))
            filename = "datasets_inventory_summary_datagov.xlsx"
            attachment = open(os.getcwd()+"\\datasets_inventory_summary_datagov.xlsx", "rb")
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

def main():
    df= pd.DataFrame()
    i = 0
    mypath = os.getcwd() + "\\datasets"
    try:
        for datasource in listdir(mypath):
            if "datagov" in datasource:
                print "Currently Processing: ",datasource
                try:
                    for dataset in listdir(os.path.join(mypath,datasource)):
                        for files in listdir(os.path.join(mypath,datasource,dataset)):
                            if (files.endswith(".csv") or files.endswith(".json") or files.endswith(".jsonl")):
                                df.loc[i,'datasource_name'] = datasource
                                df.loc[i,'process_area'] = ""
                                df.loc[i,'dataset_name_in_git'] = dataset
                                df.loc[i,'dataset_name_in_yaml'] = "cortex/"+dataset.replace("-", "_")
                                df.loc[i, 'industry'] = get_industry_datagov(datasource)
                                df.loc[i,'title_scrapped'] = get_title_datagov(dataset)
                                df.loc[i,'description_scrapped'] =  get_description_datagov(dataset)
                                orginization_name, publisher_name = get_publisher_datagov(dataset)
                                df.loc[i,'orginization_name'] = orginization_name
                                df.loc[i,'publisher_name'] = publisher_name
                                df.loc[i,'url'] = "https://catalog.data.gov/dataset/" + dataset
                                df.loc[i,'file_name'] = files
                                df.loc[i,'file_format'] = files.split(".")[-1]
                                df.loc[i,'size_in_mb'] = os.path.getsize(os.path.join(mypath,datasource,dataset,files))/float(1024*1024)
                                i+= 1
                    num_files = 0; num_folders = 0
                    for _, dirnames, filenames in os.walk(mypath + "\\" + datasource):
                        num_folders += len(dirnames)
                        num_files += len(filenames)
                    print "Processed: ", num_folders, "datasets and ", num_files, "files in ", datasource

                except WindowsError:
                    continue
            else:
                print "Skipped: ", datasource, ",Only Processing datagov datasets.."
    except WindowsError:
        pass

    df.to_csv(os.getcwd()+"\\datasets_inventory_summary_datagov.csv")
    datasets_inventory_summary = pd.ExcelWriter(os.getcwd()+"\\datasets_inventory_summary_datagov.xlsx", engine='xlsxwriter')
    df.to_excel(datasets_inventory_summary, 'List')
    datasets_inventory_summary.save()

if __name__ == "__main__":
    prog_run_time = time.clock()
    main()
    send_notification_email()
    print "\nProcessed time: ", (time.clock() - prog_run_time) / 60, " Min."

"""
References
# https://stackoverflow.com/questions/29769181/count-the-number-of-folders-in-a-directory-and-subdirectories
# Reference: https://medium.freecodecamp.org/send-emails-using-code-4fcea9df63f
# Reference: http://naelshiab.com/tutorial-send-email-python/
"""
