"""
@author: EAweblog

downloads delination file from census.gov and builds PSA delineations csv
"""

from urllib.request import urlretrieve
from time import sleep
import os
import pandas as pd

def download_file(path, url):
    print("downloading {}".format(path), end='')
    sleep(1) # so as not to cause a server time-out
    urlretrieve(url, path)

def get(path, backup_url):
    if not os.path.exists(path):
        download_file(path, backup_url)

path = "list1_2020.xls"
url = 'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/' + path
writefn = "PSA-delineations.csv"

get(path, url)
# Delinations source file originally hosted at:
# https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls
# https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html
# https://www.census.gov/programs-surveys/metro-micro.html)

PSAdf = pd.read_excel(path, skiprows=2, nrows=1916, dtype=str)
PSAdf["FIPS"] = PSAdf["FIPS State Code"] + PSAdf["FIPS County Code"]
PSAdf.rename(columns={"CBSA Code": "CBSACode", "CSA Code": "CSACode",
                      "CBSA Title": "CBSATitle", "CSA Title": "CSATitle"},
             inplace=True)

PSAdf.to_csv(writefn, columns = ["FIPS", "CBSACode", "CSACode", "CBSATitle", "CSATitle"], index=False)
