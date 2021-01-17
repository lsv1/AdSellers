#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

import pandas as pd
import requests
import sqlalchemy

import settings

logging.basicConfig(level=logging.INFO)


def get_sellers_json(name, file):
    logging.info('Getting sellers.json for ' + str(name))
    response = requests.get(file, headers=settings.HEADERS)
    data = json.loads(response.text)

    df = pd.json_normalize(data["sellers"])

    df['ssp'] = name
    df['scrape_date'] = pd.to_datetime('today')

    try:
        df['version'] = data['version']
    except:
        df['version'] = 'N/A'
        logging.warning('No sellers.json version for ' + str(name))
        pass

    try:
        df['contact_email'] = data['contact_email']
    except:
        df['contact_email'] = 'N/A'
        logging.warning('No sellers.json contact_email for ' + str(name))
        pass

    try:
        for identifier in data['identifiers']:
            df[identifier['name']] = identifier['value']
    except:
        logging.warning('No sellers.json identifiers for ' + str(name))
        pass

    logging.info('Retrieved sellers.json for ' + str(name))
    return df


def get_all_sellers_json():
    df = pd.DataFrame()

    for seller in settings.LIST_OF_SELLERS_JSON:
        seller_df = get_sellers_json(seller, settings.LIST_OF_SELLERS_JSON[seller])
        df = pd.concat([df, seller_df], ignore_index=True)

    df.to_sql(name="sellers",
              con=settings.DB_SELLERS_JSON,
              if_exists='replace',
              dtype={'scrape_date': sqlalchemy.types.DATE})

    logging.info('Wrote all sellers.json to database.')


if __name__ == "__main__":
    get_all_sellers_json()
