#!/usr/bin/env python
# -*- coding: utf-8 -*-
from os import listdir
from os.path import isfile, join

import pandas as pd

import settings


def refresh_adstxt_database():
    date = pd.to_datetime('today').strftime("%Y_%m_%d")
    files_to_load = [f for f in listdir(settings.DIR_ARCHIVE) if isfile(join(settings.DIR_ARCHIVE, f))]

    for file in files_to_load:
        if date in file:
            print(file)


refresh_adstxt_database()
