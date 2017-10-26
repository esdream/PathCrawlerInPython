# _*_ coding: utf-8 _*_

"""Global settings.

Default settings of environment variable.
"""

import re
import os

GLOBAL_SETTINGS_URL = re.sub(r'\\', '/', os.path.split(__file__)[0])

CITIES_URL = GLOBAL_SETTINGS_URL + '/../data/cities/'

CITY_COMS_URL = GLOBAL_SETTINGS_URL + '/../data/city_coms/'

PATH_DATA_URL = GLOBAL_SETTINGS_URL + '/../data/path_data/'

CRAWL_ERROR_URL = GLOBAL_SETTINGS_URL + '/../data/crawl_error/'

PARSE_ERROR_URL = GLOBAL_SETTINGS_URL + '/../data/parse_error/'

if(__name__ == '__main__'):
    print(CITY_COMS_URL)
    print(CITIES_URL)
