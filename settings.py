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

# SELLERS.JSON SOURCES
LIST_OF_SELLERS_JSON = {'Xandr': 'https://xandr.com/sellers.json',
                        'Google': 'http://realtimebidding.google.com/sellers.json',
                        'District M': 'https://districtm.io/sellers.json',
                        'Media.net': 'https://media.net/sellers.json',
                        'Verizon Media': 'https://www.verizonmedia.com/sellers.json',
                        'Index Exchnage': 'https://indexexchange.com/sellers.json',
                        'Amazon A9': 'https://aps.amazon.com/sellers.json'}

# BASIC DATA
ADSTXT_ENTRY_RANGE_SIZES = [3, 4]

try:
    from local_settings import *
except ImportError:
    pass
