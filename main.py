#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from functions import ads_txt, sellers_json

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    sellers_json.get_all_sellers_json()
    ads_txt.async_process()
