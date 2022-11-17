"""
@author: EAweblog

downloads County Characteristics datasets from census.gov and builds data tsv
files by year for CRR and ACE variables
"""

import sys 
from helpers import *
import glob
import pandas as pd
import numpy as np
from itertools import product
from collections import defaultdict
import os

dir2000 = 'cc-est2010'
fn2000  = 'cc-est2010-alldata.csv'
dir2010 = 'cc-est2019'
fn2010  = 'cc-est2019-alldata.csv'
def download_datasets():
    if decade == 2000:
        url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010/2010-eval-estimates/' + fn2000
        os.makedirs(dir2000, exist_ok=True)
        path = dir2000 + os.sep + fn2000
        get_file(path, url)
    if decade == 2010:    
        url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/asrh/' + fn2010
        os.makedirs(dir2010, exist_ok=True)
        path = dir2010 + os.sep + fn2010
        get_file(path, url)

##############################################################################################################################################################
@clickwatch
def load_PSAdf():
    """loading PSA dataframe"""
    # Primary Statistical Area dataframe
    path = 'list1_2020.xls'
    url = 'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/' + path
    get_file(path, url)
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
    # https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/asrh/cc-est2019-alldata.csv
    # https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2019/cc-est2019-alldata.pdf 
    # https://www.census.gov/data/tables/time-series/demo/popest/2010s-counties-detail.html
    
    # ccdf = county characteristics dataframe
    ignored_cols = ['SUMLEV', 'STNAME', 'CTYNAME']
    global ccdf
    if decade == 2010: path = dir2010 + os.sep + fn2010
    if decade == 2000: path = dir2000 + os.sep + fn2000
    with open(path) as f: header = f.readline().split(',')
    dtype = {"STATE": str, "COUNTY": str}
    ccdf = pd.read_csv(path, encoding = "ISO-8859-1",
                        usecols=lambda x: x not in ignored_cols,
                        dtype=dtype)
    ccdf.fillna(0,inplace=True)
    if decade == 2000:
        ccdf['STATE'] = ccdf['STATE'].apply(lambda x: x.zfill(2))
        ccdf['COUNTY'] = ccdf['COUNTY'].apply(lambda x: x.zfill(3))
    ccdf["GEO"] = ccdf["STATE"] + ccdf["COUNTY"]
    ccdf.drop(["STATE", "COUNTY"], axis=1, inplace=True)
    ccdf.set_index(["GEO", "YEAR", "AGEGRP"], inplace=True)
    
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
            if str(CSACode) != str(np.nan): Geos.append('P' + str(int(CSACode)))
            # Primary Statistical Areas include Combined Statistical Areas (CSAs)
            # and the Core-Based Statistical Areas (CBSAs) that aren't in a CSA
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
    for geo, df in Geocdfs.items():
        df.index = pd.MultiIndex.from_tuples((geo,)+idx for idx in df.index)
    
    global Geocdf
    Geocdf = ccdf.append(list(Geocdfs.values())) # DataFrame reshapes take a long time so it's import to only do it once
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
        Geocdf[ph%'C'] = Geocdf[ph%'AC'] - Geocdf[ph%'A']
        # (in combination) = (alone or in combination) - (alone)
        
    for his, sex in product(hispanic_status, sexes):
        ph = his + '%s' + sex # placeholder
        Geocdf[ph%'TC'] = sum(Geocdf[ph%(crace+'C')] for crace in census_races)
        # (total in combination) = (sum of [(crace in combination) for crace in census_races])
        Geocdf[ph%'addterm'] = replace_inf(Geocdf[ph%'TOM'] / Geocdf[ph%'TC'])
    
    for his, crace, sex in product(hispanic_status, census_races, sexes):
        ph = his + crace + '%s' + sex # placeholder
        Geocdf[ph%'T'] = Geocdf[ph%'A'] + (Geocdf[ph%'C'] * Geocdf[his + 'addterm' + sex])
    """
    The people who are labeled `two or more races` (TOM) are partitioned into the
    five census races in proportion to the frequency at which someone reports being
    a given race "in combination" with additional race(s) (relative to the frequency
    at which someone reports being any race "in combination" with additional races(s)).
    """
    
    global my_races
    my_races = {
        'E': ["TOT"],
        'W': ["NHWT"],
        'B': ["NHBT", "HBT"],
        'R': ["NHIT", "HIT", "HWT"],
        'Y': ["NHAT", "HAT", "NHNT", "HNT"]
    }
    # everyone, white, black, red, yellow
    # See above commentary on whites, white hispanics (Mestizos), and Amerindians.
    
    for (race,desig), sex in product(my_races.items(), sexes):
        Geocdf[race+sex] = sum(Geocdf[desc+sex] for desc in desig)
    
##############################################################################################################################################################

##############################################################################################################################################################

@clickwatch
def porcess_data():
    """calculating CRR and ACE"""
       
    relevant = Geocdf[[race+"_FEMALE" for race in my_races]].reset_index().set_index(["GEO", "YEAR"])
    relevant_groups = relevant.groupby("AGEGRP")
    mygroup = lambda i: relevant_groups.get_group(i).drop("AGEGRP", axis=1)
    daughters, bottom, top = (mygroup(i) for i in (1,4,10))
    mothers = sum(mygroup(i) for i in range(5,10)) + (bottom+top)/2
    
    crr = (daughters*6 / mothers).round(2).rename(columns=lambda x: x[0]+'_CRR')
    crr = replace_inf(crr)
    ace = (replace_inf(daughters-top)/5).round(0).astype(int).rename(columns=lambda x: x[0]+'_ACE')
    
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
    os.makedirs('DATA', exist_ok=True)
    for idx, data in alldata.groupby(level=1):
        if not (3 <= idx < 13): continue
        data = data.reset_index().set_index("GEO").drop("YEAR", axis=1)
        data.to_csv(f'DATA{os.sep}{yearcodes(idx)}.tsv', sep='\t')

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