'''Search Poi

Search poi info from AMap.
'''

import os
import csv
import queue
import requests
import threading
import sqlite3

from path_crawler.conf import global_settings

SEARCH_QUEUE = queue.Queue()
POI_QUEUE = queue.Queue(300)

PARSER_EXIT_FLAG = False

class SearchReader(object):
    def __init__(self, search_file, search_queue):
        self._search_file = search_file
        self._search_queue = search_queue

    def read_data(self):
        print('Reader started..')
        self._reader()
        print('Saerching data have read in search queue.')

    def _reader(self):
        with open(self._search_file, mode='r', encoding='utf-8') as f_search:
            csv_search = csv.reader(f_search)
            next(csv_search)
            for row in csv_search:
                self._search_queue.put(row)

class Searcher(threading.Thread):
    def __init__(self, **thread_args):
        threading.Thread.__init__(self)
        self._search_file = thread_args['search_file']
        self._poi_file = thread_args['poi_file']
        self._search_params = thread_args['search_params']

    def search_poi(self):
        while not PARSER_EXIT_FLAG:
            pass

def main():

    # if(int(search_type) == 1):
    #     search_filename = input('请输入使用关键字搜索的文件（默认为.csv文件，不需要输入文件后缀名）：')
    # elif(int(search_type) == 2):
    #     search_filename = input('请输入使用周边搜索的文件（默认为.csv文件，不需要输入文件后缀名）：')
    # elif(int(search_type) == 3):
    #     search_filename = input('请输入使用多边形搜索的文件（默认为.csv文件，不需要输入文件后缀名）：')
    search_type = input('请输入搜索方式：1 关键字搜索；2 周边搜索；3 多边形搜索')

    search_filename = input('请输入待搜索的POI文件（默认为.csv文件，不需要输入文件后缀名）：')
    poi_filename = input('请输入输出文件名（默认为.db文件，不需要输入文件后缀名）：')

    search_file = os.path.join(global_settings.POI_URL, search_filename + '.csv')
    poi_file = os.path.join(global_settings.POI_URL, poi_filename + '.db')

    key = input('请输入高德地图开发者密钥：')
    search_params = {
        'search_type': search_type,
        'key': key
    }

    search_reader = SearchReader(search_file, SEARCH_QUEUE)
    search_reader.read_data()

    poi_db = sqlite3.connect(poi_file)
    with poi_db:
        poi_db.execute('create table poi(id int primary key, )')


if(__name__ == '__main__'):
    main()
