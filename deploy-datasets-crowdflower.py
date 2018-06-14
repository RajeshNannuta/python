

import os
import sys
import re
import urllib
import time
import pandas as pd
from bs4 import BeautifulSoup
import warnings
from pick import pick
import shutil
import stat

def write_to_log(err_in_fun,err_for,err_type,err_lnb):
    try:
        f_0.write("\nError if Function: "+err_in_fun+" For "+err_for+".\n\t Error Type: "+str(err_type)+" in Line: "+str(err_lnb))
    except:
        pass

def create_title(title):
    """"
    Creates a Title as per the required format in yaml for the dataset.

    Args:
          raw dataset name
    Returns:
        Formatted dataset name by removing all the Special characters and converting to string to camel case, if there are no exceptions.
        If there are any exceptions this function returns actual input title to the yaml file.
    Example:
        Input: this@is-dataset-name
        Output: This Is Dataset Name
    """
    try:
        return re.sub('[^A-Za-z0-9]+', ' ', title).title()
    except Exception:
        error_type, error_obj, error_info = sys.exc_info()
        write_to_log("Create_title",title,error_type,error_info.tb_lineno)
        return title

def get_description(dataset_name):
    """
    This function returns the the description of the dataset from the "https://catalog.data.gov/" and incoporates it in the yaml.
    For the input datase that is provided, the function navigates to 'https://catalog.data.gov/dataset_name'
    At present this works only for datasets from data.gov.

    Args:
        the dataset name
    Returns:
        the dataset description from the data.gov if no exceptions. else the 'dataset_name + Dataset' is returned.

    Example:
        Input: risk_management_department_enrollment_summary_april_2012_a35f7
        Output:  Monthly summary of employee enrollment, by department. Data is for April 2012.
    """
    browser = urllib.request.build_opener()
    browser.addheaders = [('User-agent', 'Mozilla/5.0')]
    url = "https://catalog.data.gov/dataset/" + dataset_name
    try:
        response = browser.open(url,verify=False) #timeout=(2.0),
        html = response.read()
        bsObj = BeautifulSoup(html)
        desc = bsObj.find('p').text
        desc = desc.replace(":", ";")
        return desc.encode("utf-8")
    except Exception:
        error_type, error_obj, error_info = sys.exc_info()
        write_to_log("get_description", dataset_name, error_type, error_info.tb_lineno)
        return create_title(dataset_name) + " Dataset"

def get_title(dataset_name):
    """"
    This function returns the the title of the dataset from the "https://catalog.data.gov/" and incoporates it in the yaml.
    For the input dataset that is provided, the function navigates to 'https://catalog.data.gov/dataset_name'
    At present this works only for datasets from data.gov.

    Args:
        the dataset name
    Returns:
        the dataset description from the data.gov if no exceptions. else the 'dataset_name + Dataset' is returned.

    Example:
        Input: risk_management_department_enrollment_summary_april_2012_a35f7
        Output:  Risk Management - Employee Enrollment and Pool - 2012
    """
    browser=urllib.request.build_opener()
    browser.addheaders=[('User-agent', 'Mozilla/5.0')]
    url = "https://catalog.data.gov/dataset/" + dataset_name
    try:
        response=browser.open(url,verify=False) #timeout=(2.0),
        html = response.read()
        bsObj = BeautifulSoup(html)
        desc = bsObj.find("section",{"class":"module-content"}).find('h1').text
        desc = desc.replace(":"," ")
        return desc.encode("utf-8")
    except Exception:
        error_type, error_obj, error_info = sys.exc_info()
        write_to_log("get_title", dataset_name, error_type, error_info.tb_lineno)
        return create_title(dataset_name) + " Dataset"

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
        error_type, error_obj, error_info = sys.exc_info()
        write_to_log("create_parm_name", title, error_type, error_info.tb_lineno)
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
        error_type, error_obj, error_info = sys.exc_info()
        write_to_log("create_parm_title", title, error_type, error_info.tb_lineno)
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
        error_type, error_obj, error_info = sys.exc_info()
        write_to_log("convert_camel_datatype", obj, error_type, error_info.tb_lineno)
        return obj

def tag_service_provider(provider):
    last_element = provider.split("\\")[-1].lower()
    if "datagov" in last_element: return ".datagov","data.gov"
    elif "crowdflower" in last_element: return ".crowdflower","Crowdflower"
    elif "quan" in last_element: return ".quandl","Quandl"
    elif "cortex" in last_element: return ".cortex","Cortex"
    elif "monitor" in last_element: return ".opendatamonitor","Open Data Monitor"
    elif "industry_data" in last_element: return ".industrydata","Industry Data"
    else: return "","General"

def tag_industry(industry):
    last_element = industry.split("\\")[-1].lower()
    if "fin" in last_element: return ".finance", "Finance"
    elif "health" in last_element: return ".healthcare", "Healthcare"
    elif "consumer" in last_element: return ".consumer", "Commerce"
    else: return "","All"

def make_file_dirs(domain_dir,run_selected_datasets):
    temp_list = []
    for i in run_selected_datasets:
        temp_list.append(os.path.join(domain_dir, i))
    run_selected_datasets = temp_list
    try:
        for subdir, dirs, files in os.walk(os.path.join(domain_dir)):
            if subdir in run_selected_datasets:
                for filename in files:
                    if (filename.endswith(".csv") or filename.endswith(".json") or filename.endswith(".jsonl")) and (
                            (os.path.getsize(os.path.join(subdir, filename)) / float(1024 * 1024)) < 1):
                        os.remove(os.path.join(subdir, filename))
                    elif (filename.endswith(".yaml") or filename.endswith(".sh")):
                        os.remove(os.path.join(subdir, filename))
                    elif (filename.endswith(".csv") or filename.endswith(".json") or filename.endswith(".jsonl")) and (
                            (os.path.getsize(os.path.join(subdir, filename)) / float(1024 * 1024)) > 1):
                        file_folder_name = filename.split(".")[0]
                        os.mkdir(os.path.join(subdir, file_folder_name))
                        shutil.move(os.path.join(subdir, filename), os.path.join(subdir, file_folder_name, filename))
                    else:
                        os.remove(os.path.join(subdir, filename))
                try: os.rmdir(subdir)
                except WindowsError: pass
            else:
                pass
    except:
        error_type, error_obj, error_info = sys.exc_info()
        write_to_log("Error in Make File Dirs",domain_dir, error_type, error_info.tb_lineno)

def goto(domain_dir,run_selected_datasets):
    """"
    This function takes in as input the domain_folder_path which the user needs to run
    and the list of the datasets that are to be executed in that domain folder.
    As of now this supports only the .csv formatted files. Files that are present in the dataset folder and prepares an yaml and sh file(to deploy the dataset into cortex marketplace)
    """
    inventory_df = pd.read_excel("Datasets for Launch - 0602_7AM_Reviewed.xlsx", sheetname="New List")
    inventory_df = inventory_df[inventory_df['datasource_name'] == "CrowdFlower"]

    make_file_dirs(domain_dir, run_selected_datasets)

    try:
        counter = 1
        for i in run_selected_datasets:

            process_area_tag = inventory_df[inventory_df['dataset_name_in_yaml'] == "cortex/" + re.sub("[^A-Za-z0-9_]+", "",i)]['Process Area'].values
            dataset_title = inventory_df[inventory_df['dataset_name_in_yaml'] == "cortex/" + re.sub("[^A-Za-z0-9_]+", "",i)]['title_scrapped'].values
            dataset_desc = inventory_df[inventory_df['dataset_name_in_yaml'] == "cortex/" + re.sub("[^A-Za-z0-9_]+", "",i)]['description_scrapped'].values
            dataset_industry = inventory_df[inventory_df['dataset_name_in_yaml'] == "cortex/" + re.sub("[^A-Za-z0-9_]+", "",i)]['Industry'].values

            if len(dataset_title) > 0: title = dataset_title[0]
            else: title = create_title(i)
            if len(dataset_title) > 0: description = dataset_desc[0]
            else: description = create_title(i)
            if len(dataset_title) > 0:
                industry = dataset_industry[0]
            else: industry = "All"

            for subdir, dirs, files in os.walk(os.path.join(domain_dir, i)):
                if len(files) > 0:
                    try:
                        for j in files:
                            f_1.write(os.path.join(subdir, j) + "\n")
                            size_of_file = os.path.getsize(os.path.join(subdir, j)) / float(1024 * 1024)
                            if j.endswith(".csv"):
                                try:
                                    csv_yaml_file = pd.read_csv(subdir + "\\" + j, low_memory=False,
                                                                error_bad_lines=False, warn_bad_lines=False,
                                                                encoding='ISO-8859-1', nrows=10)
                                    colname_list = csv_yaml_file.columns.values
                                    datatype_list = []
                                    for k in colname_list:
                                        datatype_list.append(csv_yaml_file[k].dtype)
                                    if (len(datatype_list) != len(colname_list)):
                                        print "Inspect the dataset: '", j, "' the number of columns did not match number of datatypes returned..!!"
                                    else:
                                        yaml_file = open(os.path.join(subdir, "dataset.yaml"), "w")
                                        yaml_file.write("camel: 1.0.0\n")
                                        yaml_file.write("name: cortex/" + re.sub("[^A-Za-z0-9]+","",i) + "\n")
                                        yaml_file.write("title: " + title + "\n")
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

                                        serv_prov_label, serv_prov_value = tag_service_provider(domain_dir)
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
                                                    alpha_label = alpha.replace(": ", ".").strip().replace(" ","_").lower()
                                                    alpha_value = alpha.split(":")[1].strip()
                                                    yaml_file.write("  -\n")
                                                    yaml_file.write("    label: dataset.solution_patterns." + alpha_label + "\n")
                                                    yaml_file.write("    value: " + alpha_value + "\n")
                                                elif "AMPLIFY" in alpha:
                                                    alpha_label = alpha.replace(": ", ".").strip().replace(" ",
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
                                except Exception:
                                    os.remove(os.path.join(subdir, j))
                                    os.rmdir(subdir)
                                    error_type, error_obj, error_info = sys.exc_info()
                                    write_to_log("File Corrupted or has an error", subdir + j, error_type,error_info.tb_lineno)
                                    break

                            elif j.endswith(".json") and size_of_file >= 1:
                                try:
                                    json_yaml_file_chunk = pd.read_json(subdir + "\\" + j, lines=True, chunksize=1)
                                    for json_yaml_file in json_yaml_file_chunk:
                                        colname_list = json_yaml_file.columns.values
                                        datatype_list = []
                                        for k in colname_list:
                                            datatype_list.append(json_yaml_file[k].dtype)
                                        if (len(datatype_list) != len(colname_list)):
                                            print "Inspect the dataset: '", j, "' the number of columns did not match number of datatypes returned..!!"
                                        else:
                                            yaml_file = open(os.path.join(subdir, "dataset.yaml"), "w")
                                            yaml_file.write("camel: 1.0.0\n")
                                            yaml_file.write("name: cortex/" + re.sub("[^A-Za-z0-9_]+", "",i) + "\n")
                                            yaml_file.write("title: " + title + "\n")
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
                                            yaml_file.write("    value: files/jsonfile.csv\n")
                                            yaml_file.write("  -\n")
                                            yaml_file.write("    name: contentType\n")
                                            yaml_file.write("    value: text/json\n")

                                            serv_prov_label, serv_prov_value = tag_service_provider(domain_dir)
                                            if industry == "All": industry_label = ""
                                            else: industry_label = industry.lower()
                                            yaml_file.write("tags:\n")
                                            yaml_file.write("  -\n")
                                            yaml_file.write("    label: dataset.service_provider" + serv_prov_label + "\n")
                                            yaml_file.write("    value: " + serv_prov_value + "\n")
                                            yaml_file.write("  -\n")
                                            yaml_file.write("    label: dataset.industry." + industry_label + "\n")
                                            yaml_file.write("    value: " + industry + "\n")
                                            yaml_file.write("  -\n")
                                            yaml_file.write("    label: dataset.title." + re.sub("[^A-Za-z0-9_]+", "",i) + "\n")
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
                                except:
                                    os.remove(os.path.join(subdir, j))
                                    os.rmdir(subdir)
                                    error_type, error_obj, error_info = sys.exc_info()
                                    write_to_log("File Corrupted", str(subdir) + str(j), error_type,error_info.tb_lineno)
                                    break

                            shell_file = open(os.path.join(subdir, "deploy-dataset.sh"), "w")
                            shell_file.write("#!/usr/bin/env bash\n")
                            shell_file.write("set -e\n")
                            shell_file.write("SCRIPT_DIR=\"$( cd \"$( dirname \"${BASH_SOURCE[0]}\" )\" && pwd )\" \n\n")
                            shell_file.write("# Deploy our skill to Cortex\n")
                            shell_file.write("cortex datasets save --yaml $SCRIPT_DIR/dataset.yaml")
                            shell_file.close()
                            os.chmod(os.path.join(subdir, "deploy-dataset.sh"),stat.S_IRWXO)
                    except Exception:
                        error_type, error_obj, error_info = sys.exc_info()
                        write_to_log("inner_goto", subdir, error_type, error_info.tb_lineno)
                        continue

            print "\tProcessed:", counter, "datasets\r",
            counter += 1
        print "\tProcessed:", counter - 1, "datasets"

        for subdir, dirs, files in os.walk(os.path.join(domain_dir)):
            try: os.rmdir(subdir)
            except WindowsError: pass

    except Exception:
        error_type, error_obj, error_info = sys.exc_info()
        write_to_log("outer_goto", domain_dir, error_type, error_info.tb_lineno)

def main():

   domain_option = "crowd_flower_datasets"
   domain_dir = os.path.join(os.getcwd() +"\\datasets\\"+domain_option)

   print "\nExecuting: ",domain_option,"\n"
   title_1 = "Do you wish to run all the datasets or selected ones..??"
   dataset_option_list = ['Run All','Run Selected']
   dataset_option, dataset_index = pick(dataset_option_list,title_1)
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

if __name__ == "__main__":
    prog_run_time = time.clock()
    f_0 = open("errorLog_deploy_crowdflower.txt", "w")
    f_1 = open("deploy_crowdflower_check.txt", "w")
    main()
    print "\nProcessed time: ", (time.clock() - prog_run_time) / 60, " Min."

"""
References & Notes:
Pick module Documentation: https://pypi.org/project/pick/#description
Go to: "https://www.lfd.uci.edu/~gohlke/pythonlibs/#curses" Download: "curses-2.2-cp27-cp27m-win_amd64.whl"
navigate to the whl directory and run: "python -m pip install curses-2.2-cp27-cp27m-win_amd64.whl"

https://stackoverflow.com/questions/2690324/list-directories-and-get-the-name-of-the-directory
https://docs.cortex-dev.insights.ai/docs/developer-guide/reference-guides/camel/#reference-object    
https://www.dataquest.io/blog/web-scraping-tutorial-python/
http://www.pythonforbeginners.com/python-on-the-web/beautifulsoup-4-python/
https://www.dataquest.io/blog/web-scraping-beautifulsoup/
https://unix.stackexchange.com/questions/84686/how-to-create-custom-commands-in-unix-linux


https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_json.html
https://www.saltycrane.com/blog/2008/11/python-unicodeencodeerror-ascii-codec-cant-encode-character/

https://stackoverflow.com/questions/44680141/pandas-skipping-linesstop-warnings-from-showing
https://stackoverflow.com/questions/14463277/how-to-disable-python-warnings
https://stackoverflow.com/questions/7152762/how-to-redirect-print-output-to-a-file-using-python
https://stackoverflow.com/questions/3788870/how-to-check-if-a-word-is-an-english-word-with-python
https://stackoverflow.com/questions/6996603/how-to-delete-a-file-or-folder
"""