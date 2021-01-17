#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os

import pandas as pd
from tqdm import tqdm

import settings


def refresh_adstxt_database():
    logging.info("Starting db refresh.")
    # Delete existing sqlite db for new pull.
    try:
        os.remove(settings.DB_ADS_TXT)
    except:
        pass

    date = pd.to_datetime('today').strftime("%Y_%m_%d")

    # Get list of files.
    files_to_load = [f for f in os.listdir(settings.DIR_ARCHIVE) if
                     os.path.isfile(os.path.join(settings.DIR_ARCHIVE, f))]

    for file in tqdm(files_to_load):
        file_path = settings.DIR_ARCHIVE + "/" + file
        if date in file and os.stat(file_path).st_size > 0:
            df = pd.read_csv(file_path)
            df.to_sql('ads_txt', con=settings.CON_ADS_TXT, if_exists='append')
    logging.info("Finished db refresh.")


if __name__ == "__main__":
    refresh_adstxt_database()
