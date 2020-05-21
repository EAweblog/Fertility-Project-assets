"""
@author: EAweblog

downloads County Characteristics datasets from census.gov and builds data tsv
files by year for CRR and ACE variables
"""

import glob
import pandas as pd
from urllib.request import urlretrieve
import numpy as np
from itertools import product
from collections import defaultdict
import os
def ensure_dir(dirname):
    if not os.path.exists(dirname): os.makedirs(dirname)
from time import sleep, time

def clickwatch(f):
    def F(*args, **kwargs):
        print((f.__doc__ or ''), end='')
        t0 = time()
        f(*args, **kwargs)
        t1 = time()
        print(' :\t{:.2f} seconds'.format(t1-t0))
    return F

@clickwatch
def download_file(path, url):
    print("downloading {}".format(path), end='')
    sleep(1) # so as not to cause a server time-out
    urlretrieve(url, path)

def get(path, backup_url):
    if not os.path.exists(path):
        download_file(path, backup_url)

dir2000 = 'cc-est2009'
fn2000  = 'cc-est2009-alldata-{}.csv'
dir2010 = 'cc-est2018'
fn2010  = 'cc-est2018-alldata.csv'
def download_datasets():
    if decade == 2000:
        url = 'https://www2.census.gov/programs-surveys/popest/datasets/2000-2009/counties/asrh/' + fn2000
        excluded_from_FIPS = [3,7,14,43,52]
        ensure_dir(dir2000)
        FIPS_included = [str(fips).zfill(2) for fips in sorted(set(range(1,56+1)) - set(excluded_from_FIPS))]
        for FIPS in FIPS_included:
            path = dir2000 + os.sep + fn2000.format(FIPS)
            get(path, url.format(FIPS))
    
    if decade == 2010:    
        url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2018/counties/asrh/' + fn2010
        ensure_dir(dir2010)
        path = dir2010 + os.sep + fn2010
        get(path, url)

##############################################################################################################################################################
@clickwatch
def load_PSAdf():
    """loading PSA dataframe"""
    # Primary Statistical Area dataframe
    path = 'list1_2020.xls'
    url = 'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/' + path
    get(path, url)
    global PSAdf
    PSAdf = pd.read_excel(path, skiprows=2, nrows=1916, dtype=str)
    # Delinations source file originally hosted at:
    # https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls
    # https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html
    # https://www.census.gov/programs-surveys/metro-micro.html
    PSAdf.set_index(PSAdf['FIPS State Code'] + PSAdf['FIPS County Code'], inplace=True)
##############################################################################################################################################################

##############################################################################################################################################################
@clickwatch
def load_ccest():
    """loading county characteristics dataframe"""
    # County characteristics source file originally hosted at:
    # https://www2.census.gov/programs-surveys/popest/datasets/2010-2018/counties/asrh/cc-est2018-alldata.csv
    # https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2018/cc-est2018-alldata.pdf 
    # https://www.census.gov/data/tables/time-series/demo/popest/2010s-counties-detail.html
    
    # ccdf = county characteristics dataframe
    ignored_cols = ['SUMLEV', 'STNAME', 'CTYNAME']
    global ccdf
    if decade == 2010:
        path = dir2010 + os.sep + fn2010
        ccdf = pd.read_csv(path, encoding = "ISO-8859-1",
                           usecols=lambda x: x not in ignored_cols,
                           dtype={"STATE": str, "COUNTY": str})
    if decade == 2000:
        dd = defaultdict(lambda: int, {"STATE": str, "COUNTY": str})
        ccdfs = []
        files = glob.glob(dir2000 + '{}*.csv'.format(os.sep))
        for path in files:
            df = pd.read_csv(path, encoding = "ISO-8859-1",
                             usecols=lambda x: x not in ignored_cols,
                             dtype=dd)
            ccdfs.append(df)
        ccdf = pd.concat(ccdfs)
        
    ccdf["GEO"] = ccdf["STATE"] + ccdf["COUNTY"]
    ccdf.drop(["STATE", "COUNTY"], axis=1, inplace=True)
    ccdf.set_index(["GEO", "YEAR", "AGEGRP"], inplace=True)
    if decade == 2000:
        for c in ccdf.columns: # why you gotta put x's in my flat files, mang?
            ccdf[c] = pd.to_numeric(ccdf[c], errors='coerce').fillna(0)
    
##############################################################################################################################################################

##############################################################################################################################################################
@clickwatch
def porcess_geos():
    """porcessing aggregate geographies"""
    global Geocdfs
    Geocdfs = dict() # Geo characteristics dataframe(s)
    for idx, df in ccdf.groupby(level=0): # is this the only way to loop through the top index?
        df = df.loc[idx]
        ste = idx[:2]
        FIPS = idx
        Geos = ['0', ste] # [state, nation]
        if FIPS in PSAdf.index:
            CBSACode = PSAdf['CBSA Code'][FIPS]
            Geos.append('M' + CBSACode) # Core-based Statistical Area
            CSACode = PSAdf['CSA Code'][FIPS]    
            Geos.append('P' + (CSACode if CSACode != str(np.nan) else CBSACode)) # Primary Statistical Area
        for g in Geos:
            if g in Geocdfs:
                Geocdfs[g] += df
            else:
                Geocdfs[g] = df.copy() # the .copy() fixed some weird errors for me
##############################################################################################################################################################

##############################################################################################################################################################
@clickwatch
def append_geos():
    """appending aggregate geographies"""
    for g, df in Geocdfs.items():
        df.index = pd.MultiIndex.from_tuples((g,a,b) for a,b in df.index)
    
    global ccdf
    ccdf = ccdf.append(list(Geocdfs.values())) # DataFrame reshapes take a long time so it's import to only do it once
##############################################################################################################################################################

##############################################################################################################################################################
@clickwatch
def add_myrace_columns():
    """adding race columns"""
    hispanic_status = ['H', 'NH'] # hispanic or non-hispanic
    census_races = ['W', 'B', 'I', 'A', 'N']
    sexes = ['_MALE', '_FEMALE']
    # Census descriptions of races are as follows:
    # W is white
    # B is black
    # I is American Indian or Alaska Native
    # A is Asian
    # N is Native Hawaiian or Pacific Islander
    """
    According to the census classification, Mestizos (white hispanics) are classified
    as white, whereas I think they should be classified as their own racial group
    designated as red. Non-mixed Amerindians should belong to the same red group,
    but it is worth noting that the majority of people in North and South America
    who have any Amerindian ancestry are mixed European-Amerindian (Mestizo).
    """
    
    
    for his, crace, sex in product(hispanic_status, census_races, sexes):
        ph = his + crace + '%s' + sex # placeholder
        ccdf[ph%'C'] = ccdf[ph%'AC'] - ccdf[ph%'A']
        # (in combination) = (alone or in combination) - (alone)
        
    for his, sex in product(hispanic_status, sexes):
        ph = his + '%s' + sex # placeholder
        ccdf[ph%'TC'] = sum(ccdf[ph%(crace+'C')] for crace in census_races)
        # (total in combination) = (sum of [(crace in combination) for crace in census_races])
        ccdf[ph%'addterm'] = (ccdf[ph%'TOM'] / ccdf[ph%'TC']).replace([-np.inf, np.nan, np.inf], 0)
    
    for his, crace, sex in product(hispanic_status, census_races, sexes):
        ph = his + crace + '%s' + sex # placeholder
        ccdf[ph%'T'] = ccdf[ph%'A'] + (ccdf[ph%'C'] * ccdf[his + 'addterm' + sex])
    """
    The people who are labeled `two or more races` (TOM) are partitioned into the
    five census races in proportion to the frequency at which someone reports being
    a given race "in combination" with additional race(s) (relative to the frequency
    at which someone reports being any race "in combination" with additional races(s)).
    """
    
    g = lambda sex,el: sum(ccdf[desc+sex] for desc in el)
    for sex in sexes:
        # See above commentary on whites, white hispanics (Mestizos), and Amerindians.
        ccdf["E"+sex] = g(sex,["TOT"])
        ccdf["W"+sex] = g(sex,["NHWT"])
        ccdf["B"+sex] = g(sex,["NHBT", "HBT"])
        ccdf["R"+sex] = g(sex,["NHIT", "HIT", "HWT"])
        ccdf["Y"+sex] = g(sex,["NHAT", "HAT", "NHNT", "HNT"])
    
my_races = ["E", "W", "B", "R", "Y"]
# everyone, white, black, red, yellow

##############################################################################################################################################################

##############################################################################################################################################################

@clickwatch
def porcess_data():
    """calculating CRR and ACE"""
       
    relevant = ccdf[[race+"_FEMALE" for race in my_races]].reset_index().set_index(["GEO", "YEAR"])
    relevant_groups = relevant.groupby("AGEGRP")
    mygroup = lambda i: relevant_groups.get_group(i).drop("AGEGRP", axis=1)
    daughters, bottom, top = (mygroup(i) for i in (1,4,10))
    mothers = sum(mygroup(i) for i in range(5,10)) + (bottom+top)/2
    
    crr = (daughters*6 / mothers).round(2).replace([-np.inf, np.nan, np.inf], 0).rename(columns=lambda x: x[0]+'_CRR')
    ace = ((daughters-top)/5).round(0).astype(int).rename(columns=lambda x: x[0]+'_ACE')
    
    global alldata
    alldata = pd.concat([crr, ace], axis=1)
    # crr = crude reproduction rate, an approximation of the net reproduction rate
    # ace = annual cohort exchange

##############################################################################################################################################################

##############################################################################################################################################################

@clickwatch
def write_data():
    """writing tsvs"""
    yearcodes = lambda yc: yc-3+decade
    # for consistency's sake I'm using July 2010 estimate for 2010 population instead
    # of April 2010 estimate, because every other year uses the July estimate
    ensure_dir('DATA')
    for idx, data in alldata.groupby(level=1):
        if idx < 3: continue
        data = data.reset_index().set_index("GEO").drop("YEAR", axis=1)
        data.to_csv('DATA{}{}.tsv'.format(os.sep, yearcodes(idx)), sep='\t')

def main2(param):
    global decade
    decade = param
    assert decade in {2000, 2010}
    
    download_datasets()
    load_PSAdf()
    load_ccest()
    porcess_geos()
    append_geos()
    add_myrace_columns()
    porcess_data()
    write_data()

def main():
    main2(2000)
    main2(2010)

if __name__ == "__main__":
    main()
