"""
@author: EAweblog

DTR4 = Detailed Total Responses Level 4

parses DTR4 dataset and computes CRR and ACE for regions of New Zealand

DTR4 dataset was downloaded from:
http://nzdotstat.stats.govt.nz/ => 2018 Census => Ethnicity, culture, and identity
=> Ethnic group (detailed total response - level 4), by age group and sex, 2006, 2013, and 2018 Censuses
=> Select all years, select all ethnic groups, select all sexes,
select all Regional Council Areas including Total - Regional Council Areas but not Total - New Zealand by Regional Council Area,
select all age groups except under 15 years, 15-29 years, 30-64 years, or 65 years and over
=> Export to text file (CSV) with English label and tab column seperator
"""

from dtr4_categories import *
import sys
sys.path.append('..')
from helpers import *
import pandas as pd
from collections import defaultdict

# Responses must be normalized before they are partitioned because folk are allowed
# to give zero or one or more ethnicity responses
# Response categories are normalized to the group response totals and then again
# normalized to the population total

@clickwatch
def load_dtr4df():
    '''loading dtr4 dataframe'''
    global dtr4df
    dtr4df = pd.read_table('DTR4_2018.csv', index_col=[0,1,2,3,4])
    v_name = "Value  Flags"
    dtr4df[v_name] = pd.to_numeric(dtr4df[v_name], errors='coerce').fillna(0)
    dtr4df = dtr4df.loc[~dtr4df.index.duplicated()][v_name].unstack()
    dtr4df.index.rename( ("GEO", "YEAR", "SEX", "AGE"), inplace=True)

@clickwatch
def sanity_check():
    '''making sure dataframe matches stored ethnic categories'''
    x, y = set(all_ethnic_groups), set(dtr4df.columns)
    if x != y:
        print()
        left, right = x-y, y-x
        if left: print(' '.join(left), 'not in dataframe columns')
        if right: print(' '.join(right), 'not in stored categories')
        if left or right: raise ValueError("dataframe columns don't match stored categories")

@clickwatch
def normalize_columns():
    '''normalizing columns'''
    def normalize_level(level_name, sublevel_names):
        subgroups = [s for s in sublevel_names if s not in unspecified_groups]
        total_column = sum(dtr4df[s] for s in subgroups)
        ratio_column = replace_inf(dtr4df[level_name] / total_column)
        for s in sublevel_names: dtr4df[s] *= ratio_column
    normalize_level(total_people_name, total_people_sublevels)
    for k,v in total_people_dict.items():
        if k not in unspecified_groups: normalize_level(k, v)

@clickwatch
def add_myrace_columns():
    '''adding myrace columns'''
    dtr4df['E'] = dtr4df[total_people_name]
    ethnicities_in_race = defaultdict(set)
    for eth in race_counted_groups:
        races = Ethnicity_to_race[eth]
        weight = 1 / len(races)
        for r in races: ethnicities_in_race[r].add( (eth, weight) )
    global my_races
    my_races = ['E','W','B','R','Y','N','Z']
    for r in my_races[1:]:
        dtr4df[r] = sum(dtr4df[eth]*weight for eth,weight in ethnicities_in_race[r])

@clickwatch
def porcess_data():
    """calculating CRR and ACE"""
    import re
    def get_cohort(s):
        x = re.search('\d+', s)
        if not x: return 0
        else: return 1+int(x.group(0))//5
        
    relevant = dtr4df[my_races].groupby("SEX").get_group("Female").reset_index().set_index(["GEO", "YEAR"])
    relevant.drop(["SEX"], axis=1, inplace=True)
    relevant["AGE"] = relevant["AGE"].apply(get_cohort)
    
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
def write_data():
    """writing tsvs"""
    ensure_dir('DATA')
    for year, data in alldata.groupby(level=1):
        data = data.reset_index().set_index("GEO").drop("YEAR", axis=1)
        data.to_csv(f'DATA{os.sep}{year}.tsv', sep='\t')

def main():
    load_dtr4df()
    sanity_check()
    normalize_columns()
    add_myrace_columns()
    porcess_data()
    write_data()

if __name__ == '__main__':
    main()
