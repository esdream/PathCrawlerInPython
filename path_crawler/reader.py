"""
The Collection of Data Reader

See document in docs/topics/reader.md
"""

import os
import csv
import sqlite3

# 读取数据库中的城市组合
class CityCombinationsReader(object):
    """city combinations reader.

    Read the combinations of city from city coms csv file.
    """

    def __init__(self, city_coms_file, city_coms_queue):
        self.__city_coms_file = city_coms_file
        self.__city_coms_queue = city_coms_queue

    def read_data(self):
        """Initiator of CityCombinationsReader.

        Run the program of reader.
        """

        print('Reader of combinations start...')
        self.__combinations_reader()
        print('Reader of combinations finished. The city combinations have read in memory.')

    def __combinations_reader(self):
        """Data Reader.

        Read city combinations data from 'comsfile.csv', and put coms data into CITY_COMS_QUEUE.
        """

        with open(self.__city_coms_file, mode='r', encoding='utf-8') as f_city_coms_file:
            f_city_coms_data = csv.reader(f_city_coms_file)
            next(f_city_coms_data)
            for row in f_city_coms_data:
                self.__city_coms_queue.put(row)
