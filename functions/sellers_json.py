#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os

import pandas as pd
import requests
import sqlalchemy

import settings

logging.basicConfig(level=logging.DEBUG)


def get_sellers_json(url):
    logging.debug('Getting sellers.json for ' + str(url))
    date = pd.to_datetime('today')

    response = requests.get(url, headers=settings.HEADERS)

    data = json.loads(response.text)
    logging.debug('Downloaded sellers.json for ' + str(url))

    df = pd.json_normalize(data["sellers"])
    logging.debug('Normalized sellers.json for ' + str(url))

    try:
        df['seller_domain'] = data['contact_email'].split("@")[1]
    except:
        df['seller_domain'] = data['contact_email']

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
        for identifier in data['identifiers']:
            df[identifier['name']] = identifier['value']
    except:
        logging.debug('No sellers.json identifiers for ' + str(url))
        pass

    logging.debug('Processed sellers.json for ' + str(url) + " as dataframe.")
    return df


def get_all_sellers_json():
    # Create necessary folder if does not exist.
    try:
        os.stat(settings.DIR_DATABASES)
    except:
        os.mkdir(settings.DIR_DATABASES)

    with open(settings.FILE_SELLERS_JSON) as f:
        seller_json_urls = [line.rstrip() for line in f]

        df = pd.DataFrame()
        for url in seller_json_urls:
            df_sellers = get_sellers_json(url)
            df = pd.concat([df, df_sellers], ignore_index=True)
        df.to_sql(name="sellers",
                  con=settings.CON_SELLERS_JSON,
                  if_exists='replace',
                  dtype={'scrape_date': sqlalchemy.types.DATE})

    logging.info('Wrote all sellers.json to database.')


if __name__ == "__main__":
    get_all_sellers_json()
    # get_sellers_json('https://www.advenuemedia.co.uk/sellers.json')
