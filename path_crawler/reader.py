"""
The Collection of Data Reader

See document in docs/topics/reader.md
"""

import os
import csv

# 读取数据库中的城市组合
class ODReader(object):
    """Origin and destination reader.

    Read the origin and destination data from csv file.
    """

    def __init__(self, od_file, od_queue):
        self.__od_file = od_file
        self.__od_queue = od_queue

    def read_data(self):
        """Initiator of ODReader.

        Run the program of reader.
        """

        print('Reader of OD start...')
        self.__od_reader()
        print('Reader of OD finished. The OD data have read in memory.')

    def __od_reader(self):
        """Data Reader.

        Read OD data from 'ODfile.csv', and put OD data into OD_QUEUE.
        """

        with open(self.__od_file, mode='r', encoding='utf-8') as f_od_file:
            f_od_data = csv.reader(f_od_file)
            next(f_od_data)
            for row in f_od_data:
                self.__od_queue.put(row)
