# -*- coding: utf-8 -*-
"""
Created on Wed Jun 06 12:15:07 2018

@author: ADMIN
"""

import os,shutil
import pandas as pd
from os.path import isfile, join
import re
import warnings
#from pick import pick


    
def clear_dir():
    mypath = os.getcwd()+"\\dataworld"    
    for _dir,sub,_file in os.walk(mypath):
        
        if 'data' in sub and 'original' in sub:
            path = _dir+"\data"
            shutil.rmtree(_dir+"\original")
        for evfile in _file:
            if ".csv" not in evfile:
                os.remove(_dir+"\\"+evfile)
    
    for dirnames in os.walk(mypath):
        
        for dirname in dirnames[1]:
           dataset_path = mypath + "\\" + dirname
           for f in os.listdir(dataset_path):
               path1 = dataset_path+"\\"+f
               for f1 in os.listdir(dataset_path+"\\"+f):
                   #print f1
                   
                   if isfile(dataset_path+"\\"+f+"\\"+f1):
                       
                       if f1.replace(".csv","")=="data":
                           #print f1
                           moveto = dataset_path+"\\"+f1.replace(".csv","")+"1"
                           os.mkdir(moveto)
                       else:
                           moveto = dataset_path+"\\"+f1.replace(".csv","")
                           #print "..."+f1
                           os.mkdir(moveto)
                       src =  path1+"\\"+f1
                       dst = moveto+"\\"+f1
                       shutil.move(src,dst)
               shutil.rmtree(path1)   
        break
  
def create_parm_name(title):
    """"
    Creates the name for column/filed as per the required format in yaml.

    Args:
          raw column/field name
    Returns:
        Formatted column/field name by removing all the Special characters and concatenating by removing spaces and converting to string to camel case if there are no exceptions.
        If there are any exceptions this function returns actual input title to the yaml file.
    Example:
        Input: this is sample column name
        Output: thisIsSampleColumnName
    """
    try:
        title = re.sub('[^A-Za-z0-9]+', ' ', title).title()
        first_word = title.split(" ")[0].lower()
        title_list = title.split(" ")
        title_list.pop(0)
        return first_word +''.join(e for e in title_list)
    except Exception:
        #error_type, error_obj, error_info = sys.exc_info()
        #write_to_log("create_parm_name", title, error_type, error_info.tb_lineno)
        return title
def create_parm_title(title):
    """"
    Creates a Title as per the required format in yaml for each column/field.

    Args:
        raw column/filed name
    Returns:
        Formatted column/field name by removing all the Special characters and converting to string to camel case.
    Example:
        Input: this is sample title
        Output: This Is Sample Title
    """
    try:
        title = re.sub('[^A-Za-z0-9]+', ' ', title).title()
        return title
    except Exception:
        #error_type, error_obj, error_info = sys.exc_info()
        #write_to_log("create_parm_title", title, error_type, error_info.tb_lineno)
        return title
def convert_camel_datatype(obj):
    """"
    The Datatypes of the csv interpreted by the pandas package is converted into the
    six valid types allowed by CAMEL: integer, number, boolean, string, object, array.
    (Ref: https://docs.cortex-dev.insights.ai/docs/developer-guide/reference-guides/camel/)

    Args:
        obj: dataset object as returned from the 'dataset_column.dtype' function
    Returns:
        valid datatypes as per to CAMEL documentation if there are no exceptions.
        if there is any exception the function returns the actual abject that is passed.
    Ex:
        input:  float64, int64,..
        output: number, integer,..
    """
    try:
        obj = str(obj)
        if re.match(re.compile("int"), obj): return "integer"
        elif (re.match(re.compile("float"), obj)): return "number"
        elif re.match(re.compile("bool"), obj): return "boolean"
        elif re.match(re.compile("str"), obj): return "string"
        elif re.match(re.compile("obj"), obj): return "object"
        elif re.match(re.compile("list"), obj): return "array"
    except Exception:
        #error_type, error_obj, error_info = sys.exc_info()
        #write_to_log("convert_camel_datatype", obj, error_type, error_info.tb_lineno)
        return obj
def create_title(title):
    try:
        return re.sub('[^A-Za-z0-9]+', ' ', title).title()
    except Exception:
        return title

    
def goto(domain_dir,run_selected_datasets):
    #dataentryfile = pd.read_excel("dataworld_inventrylist.xlsx", sheetname="dataworld_uci")
    #print dataentryfile.columns
   
    inventory_df = pd.read_excel("Datasets for Launch - 0605_Updated.xlsx", sheetname="New List")
    inventory_df = inventory_df[inventory_df['datasource_name'].str.contains("dataworld",case = False)]

    for i in run_selected_datasets:
        process_area_tag = inventory_df[inventory_df['dataset_name_in_yaml'] == "cortex/" + i.replace("uci-", "").replace("-", "_")]['Process Area'].values
        dataset_title = inventory_df[inventory_df['dataset_name_in_yaml'] == "cortex/" + i.replace("uci-", "").replace("-", "_")]['title_scrapped'].values
#        print dataset_title
        dataset_desc = inventory_df[inventory_df['dataset_name_in_yaml'] == "cortex/" + i.replace("uci-", "").replace("-", "_")]['description_scrapped'].values
        dataset_industry = inventory_df[inventory_df['dataset_name_in_yaml'] == "cortex/" + i.replace("uci-", "").replace("-", "_")]['Industry'].values

        if len(dataset_title) > 0: 
            title = dataset_title[0]
        else: title = create_title(i)
        if len(dataset_title) > 0: description = dataset_desc[0]
        else: description = create_title(i)
        if len(dataset_title) > 0: industry = dataset_industry[0]
        else: industry = "All"
        
#        print os.path.join(domain_dir,i)

        for subdir, dirs, files in os.walk(os.path.join(domain_dir,i)):
            
            for filename in files:
                if filename.endswith(".csv"):
                    csv_yaml_file = pd.read_csv(subdir + "\\" + filename,low_memory=False,error_bad_lines=False,warn_bad_lines=False,encoding = 'ISO-8859-1',nrows=10)
                    colname_list = csv_yaml_file.columns.values
                    datatype_list = []
                    for k in colname_list:
                        datatype_list.append(csv_yaml_file[k].dtype)
                    if (len(datatype_list) != len(colname_list)):
                        print "Inspect the dataset: '",filename,"'the number"
                    else:
                        yaml_file = open(os.path.join(subdir, "dataset.yaml"), "w")
                        #yaml_file.write("camel: 1.0.0\n")
                        #fetchfldr =  subdir.split('\\')[3].replace("uci-","")
                        yaml_file.write("camel: 1.0.0\n")
                        name = re.sub("[^A-Za-z0-9_-]+","",i)
                        yaml_file.write("name: cortex/" + name.replace("uci-","").replace("-","_") + "\n")
                        yaml_file.write("title: " + title + "\n")
#                        print title+"here"
                        try: description = re.sub("[^A-Za-z0-9.-_ ]","",str(description))
                        except: description = title
                        yaml_file.write("description: " + description + "\n")
                        yaml_file.write("parameters: " + "\n")
                        for p in range(0, len(datatype_list)):
                            yaml_file.write("  -" + "\n")
                            yaml_file.write("    name: " + create_parm_name(colname_list[p]) + "\n")
                            yaml_file.write("    required: false" + "\n")
                            yaml_file.write("    title: " + create_parm_title(colname_list[p]) + "\n")
                            yaml_file.write("    type: " + convert_camel_datatype(datatype_list[p]) + "\n")
                        yaml_file.write("\nconnectionName: cortex/content\n")
                        yaml_file.write("connectionQuery:\n")
                        yaml_file.write("  -\n")
                        yaml_file.write("    name: key\n")
                        yaml_file.write("    value: files/csvfile.csv\n")
                        yaml_file.write("  -\n")
                        yaml_file.write("    name: contentType\n")
                        yaml_file.write("    value: text/csv\n")
                        serv_prov_label, serv_prov_value = (".dataworld_uci","DATA WORLD UCI")
                        if industry == "All":
                            industry_label = ""
                        else:
                            industry_label = "."+industry.lower()
                        yaml_file.write("tags:\n")
                        yaml_file.write("  -\n")
                        yaml_file.write("    label: dataset.service_provider" + serv_prov_label + "\n")
                        yaml_file.write("    value: " + serv_prov_value + "\n")
                        yaml_file.write("  -\n")
                        yaml_file.write("    label: dataset.industry" + industry_label + "\n")
                        yaml_file.write("    value: " + industry + "\n")
                        yaml_file.write("  -\n")
                        yaml_file.write("    label: dataset.title." +re.sub("[^A-Za-z0-9_]+", "",i) + "\n")
                        yaml_file.write("    value: " + re.sub("[^A-Za-z0-9_]+", "",i).replace("_", " ").title() + "\n")
                        if len(process_area_tag) > 0:
                            process_area_value = process_area_tag[0]
                            process_area_value_list = process_area_value.split(",")
                            for alpha in process_area_value_list:
                                if "General" in alpha:
                                    yaml_file.write("  -\n")
                                    yaml_file.write("    label: dataset.solution_patterns." + alpha.lower().strip() + "\n")
                                    yaml_file.write("    value: " + alpha.strip() + "\n")
                                elif "ENGAGE" in alpha:
                                    alpha_label = alpha.replace(":", ".").strip().replace(" ",
                                                                                          "_").lower()
                                    alpha_value = alpha.split(":")[1].strip()
                                    yaml_file.write("  -\n")
                                    yaml_file.write("    label: dataset.solution_patterns." + alpha_label + "\n")
                                    yaml_file.write("    value: " + alpha_value + "\n")
                                elif "AMPLIFY" in alpha:
                                    alpha_label = alpha.replace(":", ".").strip().replace(" ",
                                                                                          "_").lower()
                                    alpha_value = alpha.split(":")[1].strip()
                                    yaml_file.write("  -\n")
                                    yaml_file.write("    label: dataset.solution_patterns." + alpha_label + "\n")
                                    yaml_file.write("    value: " + alpha_value + "\n")
                                elif ("Intelligent" in alpha) or ("Risk" in alpha):
                                    alpha_label = alpha.strip().replace(" ", "_").lower()
                                    alpha_value = alpha.strip()
                                    yaml_file.write("  -\n")
                                    yaml_file.write("    label: dataset.solution_patterns.amplify." + alpha_label + "\n")
                                    yaml_file.write("    value: " + alpha_value + "\n")
                                elif ("Decision" in alpha) or ("Personalized" in alpha):
                                    alpha_label = alpha.strip().replace(" ", "_").lower()
                                    alpha_value = alpha.strip()
                                    yaml_file.write("  -\n")
                                    yaml_file.write("    label: dataset.solution_patterns.engage." + alpha_label + "\n")
                                    yaml_file.write("    value: " + alpha_value + "\n")
                            yaml_file.close()
                        else:
                            yaml_file.close()                    
    return


def main():
   #clear_dir() 
   domain_option = "crowd_flower_datasets"
   domain_dir = os.path.join(os.getcwd(),"dataworld")

#   print "\nExecuting: ",domain_option,"\n"
#   title_1 = "Do you wish to run all the datasets or selected ones..??"
#   dataset_option_list = ['Run All','Run Selected']
#   dataset_option, dataset_index = pick(dataset_option_list,title_1)
   dataset_option = "Run All"
   if (dataset_option == 'Run All'):
       run_selected_datasets = os.listdir(domain_dir)
       goto(domain_dir,run_selected_datasets)
   else:
       run_selected_datasets = raw_input("Enter the Datasets you wish to run (Comma \",\" Seperated): ")
       print "\n"
       run_selected_datasets = run_selected_datasets.split(",")
       run_selected_datasets = [i.strip() for i in run_selected_datasets]
       remove_elements = []
       for i in run_selected_datasets:
           if i not in os.listdir(domain_dir):
               print "Dataset: ",i," not in: ",domain_option
               remove_elements.append(i)
       for j in remove_elements:
            run_selected_datasets.remove(j)
       print "\n"
       goto(domain_dir,run_selected_datasets)
   print "\nExecution Completed..."

warnings.filterwarnings("ignore")

if __name__=="__main__":
    main()