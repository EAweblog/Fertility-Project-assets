"""
@author: EAweblog

Various helper functions for common Fertility Project tasks
"""

import os
from urllib.request import urlretrieve
from time import time, sleep
import numpy as np

def replace_inf(df):
    return df.replace([-np.inf, np.nan, np.inf], 0)

def ensure_dir(dirname):
    if not os.path.exists(dirname): os.makedirs(dirname)

def clickwatch(f):
    def F(*args, **kwargs):
        print((f.__doc__ or ''), end='')
        t0 = time()
        returned = f(*args, **kwargs)
        t1 = time()
        print(' :\t{:.2f} seconds'.format(t1-t0))
        return returned
    return F

@clickwatch
def download_file(path, url):
    print(f'downloading {path}', end='')
    sleep(1) # so as not to cause a server time-out
    urlretrieve(url, path)

def get_file(path, backup_url):
    if not os.path.exists(path):
        download_file(path, backup_url)

def memoize(f):
    memo = dict()
    return lambda *X: memo[X] if X in memo else memo.setdefault(X, f(*X))

"""
import types
from zipfile import ZipExtFile
def myreadline(*X,**Y):
    # necessary to runtime-patch the ZipExtFile.readline method to repair a
    # byte-reading error when sdmx.read_sdmx opens a (huge) zipped xml file
    return ZipExtFile.readline(*X,**Y).decode('utf-8-sig')
"""

Ethnicity_to_race = {
    "European nfd":             'W',
    "New Zealand European":     'W',
    "British nfd":              'W',
    "Celtic nfd":               'W',
    "Channel Islander":         'W',
    "Cornish":                  'W',
    "English":                  'W',
    "Irish":                    'W',
    "Manx":                     'W',
    "Scottish":                 'W',
    "Welsh":                    'W',
    "British nec":              'W',
    "Dutch":                    'W',
    "Greek":                    'W',
    "Polish":                   'W',
    "South Slav nfd":           'W',
    "Croatian":                 'W',
    "Dalmatian":                'W',
    "Macedonian":               'W',
    "Serbian":                  'W',
    "Slovenian":                'W',
    "Bosnian":                  'W',
    "South Slav nec":           'W',
    "Italian":                  'W',
    "German":                   'W',
    "Australian":               'W',
    "Albanian":                 'W',
    "Austrian":                 'W',
    "Belgian":                  'W',
    "Bulgarian":                'W',
    "Belorussian":              'W',
    "Cypriot nfd":              'W',
    "Czech":                    'W',
    "Danish":                   'W',
    "Estonian":                 'W',
    "Finnish":                  'W',
    "Flemish":                  'W',
    "French":                   'W',
    "Hungarian":                'W',
    "Icelandic":                'W',
    "Latvian":                  'W',
    "Lithuanian":               'W',
    "Maltese":                  'W',
    "Norwegian":                'W',
    "Portuguese":               'W',
    "Romanian":                 'W',
    "Russian":                  'W',
    "Slavic":                   'W',
    "Slovak":                   'W',
    "Spanish":                  'W',
    "Swedish":                  'W',
    "Swiss":                    'W',
    "Ukrainian":                'W',
    "American":                 'W',
    "Canadian":                 'W',
    "New Caledonian":           'W', #??? In New Zealand Census New Caledonian is marked as European which refers to the French colonists of New Caledonia but the majority population of New Caledonian is Melanesian
    "South African European":   'W',
    "Afrikaner":                'W',
    "Zimbabwean European":      'W',
    "European nec":             'W',
    "New Zealander":            'W',

    "African nfd":          'B',
    "Jamaican":             'B',
    "Kenyan":               'B',
    "Nigerian":             'B',
    "African American":     'B',
    "Caribbean":            'B',
    "Somali":               'B',
    "Eritrean":             'B',
    "Ethiopian":            'B',
    "Ghanaian":             'B',
    "Burundian":            'B',
    "Congolese":            'B',
    "Sudanese":             'B',
    "Zambian":              'B',
    "Other Zimbabwean":     'B',
    "African nec":          'B',
    "Seychellois":          'B', #??? BY hybrid?
    "Other South African":  'B',
    
    "Latin American nfd":   'R', #??? Which of these if any should be hybridized?
    "Argentinian":          'R',
    "Bolivian":             'R',
    "Brazilian":            'R',
    "Chilean":              'R',
    "Colombian":            'R',
    "Ecuadorian":           'R',
    "Mexican":              'R',
    "Peruvian":             'R',
    "Puerto Rican":         'R',
    "Uruguayan":            'R',
    "Venezuelan":           'R',
    "Latin American nec":   'R',
    "Indigenous American":  'R',
    
    "Maori":                    'Y',
    "Pacific Peoples nfd":      'Y',
    "Samoan":                   'Y',
    "Cook Islands Maori":       'Y',
    "Tongan":                   'Y',
    "Niuean":                   'Y',
    "Tokelauan":                'Y',
    "Hawaiian":                 'Y',    
    "Kiribati":                 'Y',
    "Nauruan":                  'Y',
    "Pitcairn Islander":        'Y',
    "Tahitian":                 'Y',
    "Tuvaluan":                 'Y',
    "Pacific Peoples nec":      'Y',
    "Asian nfd":                'Y',
    "Southeast Asian nfd":      'Y',
    "Filipino":                 'Y',
    "Cambodian":                'Y',
    "Vietnamese":               'Y',
    "Burmese":                  'Y',
    "Indonesian":               'Y',
    "Lao":                      'Y',
    "Malay":                    'Y',
    "Thai":                     'Y',
    "Karen":                    'Y',
    "Chin":                     'Y',
    "Southeast Asian nec":      'Y',
    "Chinese nfd":              'Y',
    "Hong Kong Chinese":        'Y',
    "Cambodian Chinese":        'Y',
    "Malaysian Chinese":        'Y',
    "Singaporean Chinese":      'Y',
    "Vietnamese Chinese":       'Y',
    "Taiwanese":                'Y',
    "Chinese nec":              'Y',
    "Tibetan":                  'Y',
    "Japanese":                 'Y',
    "Korean":                   'Y',
    "Mongolian":                'Y',
    "Asian nec":                'Y',
    "Bhutanese":                'Y',
    "Eurasian":                 'WY',
    #??? I'm considering changing all ambiguous Asian categories to YN hybrid
    
    "Armenian":             'N',
    "Gypsy":                'N',
    "Indian nfd":           'N',
    "Bengali":              'N',
    "Fijian Indian":        'N',
    "Indian Tamil":         'N',
    "Punjabi":              'N',
    "Sikh":                 'N',
    "Anglo Indian":         'WN',
    "Malaysian Indian":     'N',
    "South African Indian": 'N',
    "Indian nec":           'N',
    "Sri Lankan nfd":       'N',
    "Sinhalese":            'N',
    "Sri Lankan Tamil":     'N',
    "Sri Lankan nec":       'N',
    "Afghani":              'N',
    "Bangladeshi":          'N',
    "Nepalese":             'N',
    "Pakistani":            'N',
    "Maldivian":            'N',
    "Middle Eastern nfd":   'N',
    "Algerian":             'N',
    "Arab":                 'N',
    "Assyrian":             'N',
    "Egyptian":             'N',
    "Iranian/Persian":      'N',
    "Iraqi":                'N',
    "Israeli/Jewish":       'N',
    "Jordanian":            'N',
    "Kurd":                 'N',
    "Lebanese":             'N',
    "Moroccan":             'N',
    "Palestinian":          'N',
    "Syrian":               'N',
    "Turkish":              'N',
    "Middle Eastern nec":   'N',
    "Mauritian":            'N', #??? BN hybrid?

    "Papua New Guinean":        'Z',
    "Indigenous Australian":    'Z',
    "Fijian":                   'Z',
    "Rotuman":                  'Z',
    "Solomon Islander":         'Z',
    "Ni Vanuatu":               'Z',
}