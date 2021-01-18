#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import random
from urllib.parse import urlparse

import pandas as pd
import requests

import settings

logging.basicConfig(level=logging.INFO)


def get_sellers_json(url):
    logging.debug('Getting sellers.json for ' + str(url))
    date = pd.to_datetime('today')

    response = requests.get(url, headers=settings.HEADERS, allow_redirects=True)
    logging.debug('Downloaded sellers.json for ' + str(url))
    if response.status_code != 200:
        logging.debug("Bad response for " + url)
        return

    try:
        data = json.loads(response.text)
        logging.debug('JSON loaded sellers.json for ' + str(url))
    except:
        logging.debug("Unable to parse JSON " + url)
        return

    # Fix for Telaria's sellers.json (https://tremorhub.com/sellers.json) because they don't follow the spec.
    try:
        data['contact_email'] = data.pop('contactEmail')
    except:
        pass
    try:
        data['contact_address'] = data.pop('contactAddress')
    except:
        pass

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
            url = url.replace("https://", "http://")
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
        os.stat(settings.DIR_ARCHIVE)
    except:
        os.mkdir(settings.DIR_ARCHIVE)

    for url in sellers_json_url_list(shuffle=True):
        try:
            logging.info(url + " starting processing.")

            domain = urlparse(url).netloc
            filename = pd.to_datetime('today').strftime("%Y_%m_%d") + "_SELLERS.JSON_" + domain + ".csv"
            file_path = settings.DIR_ARCHIVE + "/" + filename

            df_sellers = get_sellers_json(url)
            df_sellers.to_csv(path_or_buf=file_path,
                              index=False,
                              encoding='utf-8')

            logging.info(url + " done processing.")
        except Exception as e:
            logging.info(url + " error processing: " + str(e))
            pass

    logging.info('Wrote all sellers.json files to archive.')


if __name__ == "__main__":
    get_all_sellers_json()
    # print(get_sellers_json('http://richaudience.com/sellers.json'))
