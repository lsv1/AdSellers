#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import random
from urllib.parse import urlparse

import pandas as pd
import requests
import sqlalchemy

import settings

logging.basicConfig(level=logging.INFO)


def get_sellers_json(url):
    logging.debug('Getting sellers.json for ' + str(url))
    date = pd.to_datetime('today')

    response = requests.get(url, headers=settings.HEADERS, allow_redirects=True)
    if response.status_code != 200:
        logging.debug("Bad response for " + url)
        return

    try:
        data = json.loads(response.text)
    except:
        logging.debug("Unable to parse JSON " + url)
        return

    # Fix for Telaria's sellers.json because they don't know how specs work.
    try:
        data['contact_email'] = data.pop('contactEmail')
    except:
        pass
    try:
        data['contact_address'] = data.pop('contactAddress')
    except:
        pass

    logging.debug('Downloaded sellers.json for ' + str(url))

    df = pd.json_normalize(data["sellers"])

    logging.debug('Normalized sellers.json for ' + str(url))

    try:
        df['contact_domain'] = data['contact_email'].split("@")[1]
    except:
        df['contact_domain'] = urlparse(url).netloc
        logging.debug('No sellers.json contact_email for ' + str(url))

    df['scrape_date'] = date

    try:
        df['version'] = data['version']
    except:
        df['version'] = 'N/A'
        logging.debug('No sellers.json version for ' + str(url))
        pass

    try:
        df['contact_email'] = data['contact_email']
    except:
        df['contact_email'] = 'N/A'
        logging.debug('No sellers.json contact_email for ' + str(url))
        pass

    try:
        df['contact_address'] = data['contact_address']
    except:
        logging.debug('No sellers.json contact_address for ' + str(url))
        pass

    try:
        for identifier in data['identifiers']:
            df[identifier['name']] = identifier['value']
    except:
        logging.debug('No sellers.json identifiers for ' + str(url))
        pass

    logging.debug('Processed sellers.json for ' + str(url) + " as dataframe.")
    return df


def sellers_json_url_list(shuffle=None):
    with open(settings.FILE_SELLERS_JSON) as f:
        seller_json_urls = [line.rstrip() for line in f]

        raw_domains = []
        for url in seller_json_urls:
            url = url.split("//")[1]
            url = url.split("/sellers.json")[0]
            raw_domains.append(url)
        raw_domains = list(dict.fromkeys(raw_domains))

        clean_urls = []
        for url in raw_domains:
            url = "http://" + url + "/sellers.json"
            clean_urls.append(url)

        if shuffle:
            random.shuffle(clean_urls)

        return clean_urls


def get_all_sellers_json():
    # Create necessary folder if does not exist.
    try:
        os.stat(settings.DIR_DATABASES)
    except:
        os.mkdir(settings.DIR_DATABASES)

    df = pd.DataFrame()
    for url in sellers_json_url_list(shuffle=True):
        logging.info(url + " starting processing.")
        df_sellers = get_sellers_json(url)
        df = pd.concat([df, df_sellers], ignore_index=True)
        logging.info(url + " done processing.")
    df.to_sql(name="sellers",
              con=settings.CON_SELLERS_JSON,
              if_exists='replace',
              dtype={'scrape_date': sqlalchemy.types.DATE})

    logging.info('Wrote all sellers.json to database.')


if __name__ == "__main__":
    get_all_sellers_json()
    # print(get_sellers_json('https://tremorhub.com/sellers.json'))
