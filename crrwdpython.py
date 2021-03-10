
import requests
from bs4 import BeautifulSoup
import urllib
import os
from StringIO import StringIO
import zipfile
from pget.down import Downloader
from zipfile import ZipFile
rootfolder = os.getcwd()
original_url = ''
resp = requests.get('https://www.crowdflower.com/data-for-everyone/')
soup = BeautifulSoup(resp.content, "html.parser")
listOfLinks = soup.find_all("div",{"class": "item"})
for url in listOfLinks:
        alphaURL = url.find_all('a', {"class": "download"}, href=True)
        names = url.find_all('h3')
        
        for r in alphaURL:
            for n in names:
                original_url = r['href']
                name = n.text
                
                if name  and original_url:
                    
                    fileformat = ".csv"
                    filename = re.sub("[^A-Za-z0-9 ]","",name).lower().replace(" ","_")+fileformat
                    
                    if original_url.split(".")[-1] == 'csv':
    
                        os.chdir(rootfolder)
                        if not os.path.isdir(filename):
                            os.mkdir(filename)
                        os.chdir(filename)
                        if not os.path.isfile(filename):
                            downloader = Downloader(original_url, filename, 8)
                            downloader.start()
                            print "came here"
                            print "downloading file "+filename
                            downloader.wait_for_finish()
                            
                            
                    '''if original_url.split(".")[-1] == 'zip':
                        
                        #url = urllib.urlopen(original_url)
                        #zip_file = ZipFile(StringIO(url.read()))
                        #files = zipfile.namelist()
                        #fopen = open(filename+'.csv', 'w')                               
                        #zipcontent = url.read()
                        downloader = Downloader(original_url, filename+".zip", 8)
                        downloader.start()
                        downloader.wait_for_finish()
                        print "comitted here"
                        with zipfile.ZipFile(filename+".zip", "r") as zfr:
                           zfr.extractall(filename)
                        os.chdir(rootfolder)
                        os.remove(filename+".zip")'''
                           #os.rmdir("_MACOSX")                              
                                                      
                                                      
