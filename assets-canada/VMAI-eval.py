"""
@author: EAweblog

Downloads Visible Minority and Aboriginal identity datasets from statcan.gc.ca
and computes CRR and ACE

VM = visible minority
AI = aboriginal identity
"""

import sys 
sys.path.append('..')
from helpers import *

import pandas as pd
import numpy as np

import os
from zipfile import ZipFile
from lxml import etree
from collections import defaultdict
from functools import partial

VM_url = {2016: 'https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/dt-td/OpenDataDownload.cfm?PID=112451',
          2011: 'https://www12.statcan.gc.ca/nhs-enm/2011/dp-pd/dt-td/OpenDataDownload.cfm?PID=105395',
          2006: 'https://www12.statcan.gc.ca/census-recensement/2006/dp-pd/tbt/OpenDataDownload.cfm?PID=92338',
          2001: 'https://www12.statcan.gc.ca/English/census01/products/standard/themes/OpenDataDownload.cfm?PID=65798'}
AI_url = {2016: 'https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/dt-td/OpenDataDownload.cfm?PID=110588',
          2011: 'https://www12.statcan.gc.ca/nhs-enm/2011/dp-pd/dt-td/OpenDataDownload.cfm?PID=105387',
          2006: 'https://www12.statcan.gc.ca/census-recensement/2006/dp-pd/tbt/OpenDataDownload.cfm?PID=89122',
          2001: 'https://www12.statcan.gc.ca/English/census01/products/standard/themes/OpenDataDownload.cfm?PID=62716'}

root = lambda table: f"{table}.ZIP"
VM_table = {2016: '98-400-X2016192',
            2011: '99-010-X2011029',
            2006: '97-562-XCB2006011',
            2001: '95F0363XCB2001004'}
AI_table = {2016: '98-400-X2016155',
            2011: '99-011-X2011028',
            2006: '97-558-XCB2006007',
            2001: '97F0011XCB2001002'}

# The heuristic to remember the USA (standard) cohorts is C[0] is total population,
# C[18] is 85+ population, and C[i] for 0 < i < 18 is P(5(i-1),5i)
import re
age_to_cohort = lambda age: 1+int(age)//5
def get_cohorts(s):
    x = re.findall('\d+', s) + [False, False]
    a = age_to_cohort(x[0]) if x[0] else 0
    b = (age_to_cohort(x[1]) if x[1] else 18) + 1
    p = re.compile(r".*under.*?(\d+)", re.I)
    y = re.match(p, s)
    if y: a,b = 1, age_to_cohort(y.group(1))
    return list(range(a, b))
def CL_AGE_to_USA_age(CL_AGE):
    available_cohorts = {k:get_cohorts(v) for k,v in CL_AGE.items()}
    chosen_cohorts = {k:[] for k in CL_AGE}
    for usa_cohort in range(19):
        chosen = min([k for k,v in available_cohorts.items() if usa_cohort in v],
                     key=lambda k: len(available_cohorts[k]))
        chosen_cohorts[chosen].append(usa_cohort)
    return chosen_cohorts

localname = lambda element: etree.QName(element).localname
# It's bizarre to me that this is a necessary step to remove the specification
# URL prefix from the tag name. Why the hell would I want to have the prefix
# string-prepended to the localname when I use element.tag?
childrendict = lambda element: {localname(child):child for child in element}
get_tags = lambda element,tag: (el for el in element if localname(el) == tag)

def parsezip(zipfn, fn, parsefunction, **kwargs):
    with ZipFile(zipfn) as myzip:
        return parsefunction( myzip.open(fn), **kwargs )

def get_records_from_xml(file, record_name):
    for event, element in etree.iterparse(file):
        if localname(element) != record_name: continue
        yield element
        if element.getparent() is not None: element.getparent().clear()
        # for reasons unbeknownst to me, sometimes the parent is None and
        # sometimes the parent is not None!

def parseStructure(file):
    Codes = defaultdict(dict)    
    for CodeList in get_records_from_xml(file, 'CodeList'):
        for Code in get_tags(CodeList, 'Code'):
            desc = next(get_tags(Code, 'Description')).text
            Codes[CodeList.get('id')][Code.get('value')] = desc    
    return dict(Codes)

@memoize
def Codes(table):
    return parsezip(f'{table}.ZIP', f'Structure_{table}.xml', parseStructure)

def parseGeneric(file, seriesfunction):
    columns = defaultdict(dict)
    for Series in get_records_from_xml(file, 'Series'):
        for idx, col, val in seriesfunction(Series):
            columns[col][idx] = val    
    df = pd.DataFrame.from_dict(columns)
    df.index.rename( ("GEO", "AGE", "SEX"), inplace=True )
    return df

def parseSeries(age_to_USA_age, condition, relevant_key, # first line of params will be partial'd
                Series):
    children = childrendict(Series)
    key = {child.get("concept").upper():child.get("value") for child in children["SeriesKey"]}
    obs = childrendict(children["Obs"])
    
    standardAges = age_to_USA_age[key["AGE"]]
    if condition(key) and standardAges:
        col, geo, sex = map(key.get, (relevant_key, "GEO", "SEX") )
        val = float(obs["ObsValue"].get("value")) / len(standardAges) # population value
        for sAge in standardAges:
            idx = (geo, sAge, sex)
            yield idx, col, val

@clickwatch
def parseAItable(year):
    """parsing AI table"""
    AI = AI_table[year]
    CL_AGE = Codes(AI)["CL_AGE"]
    age_to_USA_age = CL_AGE_to_USA_age(CL_AGE)
    if year in [2016, 2011]:
        condition = lambda key: key["RGINDR"] == '1' # and key["ABIDENT"] == '2'
        relevant_key = "ABIDENT"
    elif year in [2006]:
        condition = lambda key: True
        relevant_key = "ABIDENT"
    elif year in [2001]:
        condition = lambda key: True
        relevant_key = "B01_ABORIG_IDENTITY"
    parseAISeries = partial(parseSeries, age_to_USA_age, condition, relevant_key)
    aidf = parsezip(f'{AI}.ZIP', f'Generic_{AI}.xml', parseGeneric, seriesfunction=parseAISeries)
    return aidf

@clickwatch
def parseVMtable(year):
    """parsing VM table"""
    VM = VM_table[year]
    CL_AGE = Codes(VM)["CL_AGE"]
    age_to_USA_age = CL_AGE_to_USA_age(CL_AGE)
    if   year in [2016]:
        condition = lambda key: key["DIM2"] == '1'
        relevant_key = "DVISMIN"
    elif year in [2011]:
        condition = lambda key: key["GENSTPOB"] == '1'
        relevant_key = "DVISMIN"
    elif year in [2006]:
        condition = lambda key: key["YRIM"] == '1'
        relevant_key = "DVISMIN"
    elif year in [2001]:
        condition = lambda key: True
        relevant_key = "DVISMIN"
    parseVMSeries = partial(parseSeries, age_to_USA_age, condition, relevant_key)
    vmdf = parsezip(f'{VM}.ZIP', f'Generic_{VM}.xml', parseGeneric, seriesfunction=parseVMSeries)
    return vmdf

@clickwatch
def porcess_geos():
    """porcessing aggregate geographies"""
    global Geocdfs
    Geocdfs = dict() # Geo dataframe(s)
    for idx, df in vmaidf.groupby(level=0):
        df = df.loc[idx]
        rurals = []
        # The populations of `rural province` are computed by inclusion-exclusion
        if len(idx) == 2:
            # Include (this province) in (this rural province)
            if idx != '01': rurals.append( (idx, 1) )
        elif len(idx) == 5:
            # exclude (whole CMA/CA which is primarily from this province) in (this rural province)
            rurals.append( (idx[:2], -1) )
        elif len(idx) == 7 and idx[:2] != idx[-2:]:
            # include (iow negate the exclusion of) (part of CMA/CA from other province) in (this rural province)
            rurals.append( (idx[:2], 1) )
            # exclude (part of CMA/CA from other province) in (other rural province)
            rurals.append( (idx[-2:], -1) )
        for prov, coeff in rurals:
            Rprov = 'R'+prov
            cdf = coeff*df
            Geocdfs[Rprov] = Geocdfs[Rprov]+cdf if Rprov in Geocdfs else cdf

@clickwatch
def append_geos():
    """appending aggregate geographies"""
    for geo, df in Geocdfs.items():
        df.index = pd.MultiIndex.from_tuples((geo,)+idx for idx in df.index)
    
    global Geocdf
    Geocdf = vmaidf.append(list(Geocdfs.values()))

@clickwatch
def add_myrace_columns():
    """adding race columns"""
    total_specified = sum(Geocdf[str(x)] for x in list(range(3,12+1))+[15])
    coeff  = Geocdf['E'] / total_specified
    g = lambda eth: sum(Geocdf[x] for x in eth)
    # Geocdf['E'] = Geocdf['E']
    Geocdf['W'] = coeff * (Geocdf['15'] - Geocdf['A'])
    Geocdf['B'] = coeff * g(['5'])
    Geocdf['R'] = coeff * g(['A', '7'])
    Geocdf['Y'] = coeff * g(['4', '6', '9', '11', '12'])
    Geocdf['N'] = coeff * g(['3', '8', '10'])
    
    global my_races
    my_races = ['E', 'W', 'B', 'R', 'Y', 'N']
    for idx, val in coeff.iteritems():
        if val not in [-np.inf, np.nan, np.inf]: continue
        replace_value = Geocdf['1'][idx] / 5
        for race in my_races:
            Geocdf[race][idx] = replace_value

@clickwatch
def porcess_data():
    """calculating CRR and ACE"""
    relevant = Geocdf[my_races].groupby("SEX").get_group("3").reset_index().set_index("GEO")
    relevant = relevant.drop(["SEX"], axis=1).drop([idx for idx in relevant.index if len(idx) == 7])
    
    relevant_groups = relevant.groupby("AGE")
    mygroup = lambda i: relevant_groups.get_group(i).drop("AGE", axis=1)
    daughters, bottom, top = (mygroup(i) for i in (1,4,10))
    mothers = sum(mygroup(i) for i in range(5,10)) + (bottom+top)/2
    
    crr = (daughters*6 / mothers).round(2).rename(columns=lambda x: x[0]+'_CRR')
    crr = replace_inf(crr)
    ace = ((daughters-top)/5).round(0).astype(int).rename(columns=lambda x: x[0]+'_ACE')
    
    global alldata
    alldata = pd.concat([crr, ace], axis=1)
    # crr = crude reproduction rate, an approximation of the net reproduction rate
    # ace = annual cohort exchange

@clickwatch
def write_data(year):
    """writing tsvs"""
    ensure_dir('DATA')
    alldata.to_csv(f'DATA{os.sep}{year}.tsv', sep='\t')

def main2(year):
    get_file(root(AI_table[year]), AI_url[year])
    get_file(root(VM_table[year]), VM_url[year])
    print(f"processing data for {year}")
    aidf = parseAItable(year)
    vmdf = parseVMtable(year)
    global vmaidf
    vmaidf = pd.concat([vmdf, aidf[['1','2']].rename(columns={'1':'E','2':'A'})], axis=1)
    porcess_geos()
    append_geos()
    add_myrace_columns()
    porcess_data()
    write_data(year)

for year in [2001, 2006, 2011, 2016]:
    main2(year)