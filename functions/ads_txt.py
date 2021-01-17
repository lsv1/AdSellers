#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Imports
import logging
import multiprocessing
import os
import random
from multiprocessing import Pool

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from tqdm import tqdm

import settings

# Swallow SSL errors.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_domain_list(shuffle=None):
    df = pd.read_sql("""
                     SELECT DISTINCT "domain" FROM sellers WHERE "domain" IS NOT NULL
                     """, con=settings.DB_SELLERS_JSON)

    list = df['domain'].to_list()

    if shuffle:
        random.shuffle(list)

    return list


def get_ads_txt(domain):
    try:
        # Check if the file already exists, for resuming scrape on same day.
        filename = pd.to_datetime('today').strftime("%Y_%m_%d") + "_" + domain + ".csv"
        file_path = settings.DIR_ARCHIVE + "/" + filename
        if os.path.isfile(file_path):
            logging.debug(domain + ' file already scraped, skipping.')
            return

        url = 'https://www.' + domain + '/ads.txt'

        response = requests.get(url,
                                headers=settings.HEADERS,
                                allow_redirects=True,
                                timeout=0.5,  # I only want to get ads.txt if it can be returned in less than 500ms.
                                verify=False)  # Don't verify SSL, just get the data.

        if response.status_code != 200:
            logging.debug(domain + ": Bad response, skipping.")
            return

        data = response.text

        if bool(BeautifulSoup(data, "html.parser").find()):
            logging.debug(domain + ": HTML detected, skipping.")
            return

        df = pd.DataFrame()

        for line in data.splitlines():
            entries_df = pd.DataFrame()
            try:
                if len(line.split(",")) in settings.ADSTXT_ENTRY_RANGE_SIZES:
                    line = line.split("#", 1)[0]
                    line = line.replace(' ', '')
                    line = line.strip()
                    row = line.split(',')

                    # All ads.txt entries should have the below elements.
                    # Reference: https://iabtechlab.com/ads-txt/
                    entries_df = entries_df.append({
                        'domain': domain,
                        'scrape_date': pd.to_datetime('today'),
                        'ssp': row[0],
                        'account_id': row[1],
                        'type': row[2]},
                        ignore_index=True)
                    try:
                        # Some ads.txt entries will also have a certificate authority ID.
                        entries_df['certificate_authority_id'] = row[3]
                    except IndexError:
                        pass

                df = pd.concat([df, entries_df], ignore_index=True)
            except:
                pass

        if df.shape[0] > 0:
            # I want to write CSV files of each scrape to disk for eventually being able to host the latest files.
            # Pandas was an easy way to complete this, though it might be slower than creating the string for each line
            # and writing to file.
            df.to_csv(path_or_buf=file_path,
                      index=False,
                      encoding='utf-8')

        else:
            logging.debug(domain + ' failed to scrape.')
        logging.debug(domain + ' scraped.')

    except:
        logging.debug(domain + ' failed to scrape.')
        pass


def run_apply_async_multiprocessing(func, argument_list, num_processes):
    pool = Pool(processes=num_processes)

    jobs = [
        pool.apply_async(func=func, args=(*argument,)) if isinstance(argument, tuple) else pool.apply_async(func=func,
                                                                                                            args=(
                                                                                                                argument,))
        for argument in argument_list]
    pool.close()
    result_list_tqdm = []
    for job in tqdm(jobs):
        result_list_tqdm.append(job.get())

    return result_list_tqdm


def async_process():
    # Create necessary folder if does not exist.
    try:
        os.stat(settings.DIR_ARCHIVE)
    except:
        os.mkdir(settings.DIR_ARCHIVE)

    num_processes = multiprocessing.cpu_count()
    argument_list = get_domain_list(shuffle=True)
    logging.info("Processing " + str(len(argument_list)) + " domains.")
    result_list = run_apply_async_multiprocessing(func=get_ads_txt,
                                                  argument_list=argument_list,
                                                  num_processes=num_processes)
    assert result_list == argument_list
    logging.info("Done.")


if __name__ == "__main__":
    get_ads_txt('mtv.com')
    # async_process()
