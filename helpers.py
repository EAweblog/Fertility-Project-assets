"""
@author: Miner

Various helper functions for common Fertility Project tasks
"""

import os
from urllib.request import urlretrieve
from time import time, sleep

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