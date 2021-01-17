#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from functions import ads_txt, sellers_json

logging.basicConfig(level=logging.INFO)


def main(parse_sellers_json=None, parse_ads_txt=None, load_database=None):
    if parse_sellers_json:
        sellers_json.get_all_sellers_json()
    if parse_ads_txt:
        ads_txt.async_process()
    if load_database:
        load_database.refresh_adstxt_database()


if __name__ == "__main__":
    main(parse_sellers_json=True,
         parse_ads_txt=True,
         load_database=False)
