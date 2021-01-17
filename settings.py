'''
2021-01-15
Luis Sastre Verzun
'''
import os

from sqlalchemy import create_engine

# DIRECTORIES
DIR_ROOT = os.path.dirname(os.path.abspath(__file__))
DIR_DATABASES = DIR_ROOT + '/databases/'
DIR_ARCHIVE = DIR_ROOT + '/archive/'
DIR_RESOURCES = DIR_ROOT + '/resources/'

# SQLITE DATABASES
DB_SELLERS_JSON = DIR_DATABASES + '/SELLERS_JSON.db'
DB_ADS_TXT = DIR_DATABASES + '/ADS_TXT.db'

# DATABASE CONNECTIONS
CON_SELLERS_JSON = create_engine('sqlite:///' + DB_SELLERS_JSON)
CON_ADS_TXT = create_engine('sqlite:///' + DB_ADS_TXT)

# REQUESTS
HEADERS = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Accept-Language': 'en-US,en;q=0.9',
}

# RESOURCES
FILE_SELLERS_JSON = DIR_RESOURCES + '/sellers.json.txt'

# BASIC DATA
ADSTXT_ENTRY_RANGE_SIZES = [3, 4]

try:
    from local_settings import *
except ImportError:
    pass
