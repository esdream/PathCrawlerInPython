'''Search Poi

Search poi info from AMap.
'''

import os
import csv
import time
import random
import sqlite3
import threading

import queue
import requests

from path_crawler.conf import global_settings

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


class KeywordSearcher(threading.Thread):

    def __init__(self, **crawl_args):
        threading.Thread.__init__(self)
        self._thread_id = crawl_args['thread_id']
        self._search_queue = crawl_args['search_queue']
        self._poi_queue = crawl_args['poi_queue']
        self._search_params = crawl_args['search_params']

    def run(self):
        print('No.{} KeywordSearcher start...'.format(self._thread_id))
        self._search_poi()
        print('No.{} KeywordSearcher finished!'.format(self._thread_id))

    # 关键字查找的源数据格式：id,keywords(多个关键字用竖线|分割),types(多个类型用竖线|分割),查询城市
    def _search_poi(self):
        while True:
            if(self._search_queue.empty()):
                break
            else:

                search_req = self._search_queue.get()

                url = 'http://restapi.amap.com/v3/place/text?&keywords={0[1]}&types={0[2]}&city={0[3]}&output=json&offset=20&key={key}&extensions=all'.format(
                    search_req, **self._search_params)

                timeout = 2
                while timeout > 0:
                    timeout -= 1
                    try:
                        poi_res = requests.get(url, timeout=5).json()
                        poi_count = int(poi_res[u'count'])
                        pages = poi_count // 20 + 1

                        for page in range(pages):
                            while True:
                                try:
                                    if(self._poi_queue.full()):
                                        time.sleep(8)
                                    page_num = str(page + 1)
                                    sub_url = url + '&page=' + page_num
                                    
                                    poi = {}
                                    poi['id'] = search_req[0]
                                    poi['keywords'] = search_req[1]
                                    poi['types'] = search_req[2]
                                    poi['city'] = search_req[3]
                                    poi['page_num'] = page_num
                                    poi['info'] = requests.get(sub_url, timeout=5).json()
                                    self._poi_queue.put(poi)
                                    print('id:{0}, page num{1} crawl succeed!'.format(poi['id'], page_num))
                                    
                                    # 检查_poi_queue中元素个数
                                    print(self._poi_queue.qsize())


                                    time.sleep(random.random())

                                    break
                                except Exception as error:
                                    print(error)
                                    print('id:{0} page num {1} have some error, crawl again...'.format(
                                        poi['id'],  page_num))
                        
                        break

                    except Exception as crawl_error:
                        if(timeout == 0):
                            print(crawl_error)
                            self._search_queue.put(search_req)


class PoiParser(threading.Thread):

    def __init__(self, **parser_args):
        threading.Thread.__init__(self)
        self._thread_id = parser_args['thread_id']
        self._poi_file = parser_args['poi_file']
        self._poi_queue = parser_args['poi_queue']
        self._error_file = parser_args['error_file']
        self._error_lock = parser_args['error_lock']
        self._data_batch = parser_args['data_batch']

    def run(self):
        print('No.{} PoiParser start...'.format(self._thread_id))
        self._parse_poi()
        print('No.{} PoiParser finished!'.format(self._thread_id))

    def _parse_poi(self):

        global PARSER_EXIT_FLAG
        while not PARSER_EXIT_FLAG:

            try:
                #检查_poi_queue中元素个数
                print(self._poi_queue.qsize())

                poi = self._poi_queue.get(True, 20)
                
                result = {}
                result['id'] = poi['id']
                result['keywords'] = poi['keywords']
                result['types'] = poi['types']
                result['city'] = poi['city']
                result['page_num'] = poi['page_num']
                
                num_of_pois = len(poi['info'][u'pois'])
                for i in range(num_of_pois):
                    result['poi_id'] = poi['info'][u'pois'][i][u'id']
                    # result['poi_tag'] = poi['info'][u'pois'][i][u'tag']
                    result['poi_name'] = poi['info'][u'pois'][i][u'name']
                    result['poi_type'] = poi['info'][u'pois'][i][u'type']
                    result['poi_typecode'] = poi['info'][u'pois'][i][u'typecode']
                    result['poi_address'] = poi['info'][u'pois'][i][u'address']
                    result['poi_loc'] = poi['info'][u'pois'][i][u'location']

                    result_vector = (
                        result['id'], result['keywords'], result['types'], result['city'], result['page_num'], result['poi_id'], result['poi_name'], result['poi_type'], result['poi_typecode'], result['poi_address'], result['poi_loc'])
                    self._data_batch.append(result_vector)

                if(len(self._data_batch) > 200):
                    poi_db = sqlite3.connect(self._poi_file)
                    with poi_db:
                        poi_db.executemany('insert into poi values (?,?,?,?,?,?,?,?,?,?,?)', self._data_batch)
                        poi_db.commit()
                        self._data_batch[:] = []

                self._poi_queue.task_done()
                break

            except Exception as parse_error:
                with self._error_lock:
                    self._error_file.write(
                        '{id},{keywords},{types},{city}\n'.format(**poi))
                    print('Parse path {} failed!'.format(
                        poi['id']))
                    print(parse_error)


SEARCH_QUEUE = queue.Queue()
POI_QUEUE = queue.Queue(300)
ERROR_LOCK = threading.Lock()
PARSER_EXIT_FLAG = False

def main():

    search_type = input('请输入搜索方式：1 关键字搜索；2 周边搜索；3 多边形搜索：')

    search_filename = input('请输入待搜索的POI文件（默认为.csv文件，不需要输入文件后缀名）：')
    poi_filename = input('请输入输出文件名（默认为.db文件，不需要输入文件后缀名）：')

    search_file = os.path.join(global_settings.POI_URL, search_filename + '.csv')
    poi_file = os.path.join(global_settings.POI_URL, poi_filename + '.db')
    parse_error_file = os.path.join(
        global_settings.POI_URL, search_filename + '_error.csv')

    key = input('请输入高德地图开发者密钥：')
    search_params = {
        'key': key
    }

    search_reader = SearchReader(search_file, SEARCH_QUEUE)
    search_reader.read_data()

    poi_db = sqlite3.connect(poi_file)
    with poi_db:
        poi_db.execute(
            'create table poi(id int, keywords varchar(255), types varchar(255), city varchar(20), page_num int, poi_id varchar(20), poi_name varchar(20), poi_type varchar(50), poi_typecode int, poi_address varchar(255), poi_loc varchar(20))')

    with open(parse_error_file, mode='w', encoding='utf-8') as f_pares_error:
        if(int(search_type) == 1):
            search_threads = []
            search_list = ['search_thread' + str(num) for num in range(2)]
            for search_thread_id in search_list:
                search_thread = KeywordSearcher(
                    thread_id=search_thread_id,
                    search_queue=SEARCH_QUEUE,
                    poi_queue=POI_QUEUE,
                    search_params=search_params
                )
                search_thread.start()
                search_threads.append(search_thread)

        data_batch = []
        parse_thread_id = 'parse_thread'
        poi_parser = PoiParser(
            thread_id=parse_thread_id,
            poi_file=poi_file,
            poi_queue=POI_QUEUE,
            error_file=f_pares_error,
            error_lock=ERROR_LOCK,
            data_batch=data_batch
        )
        poi_parser.start()

        while not SEARCH_QUEUE.empty():
            pass

        for search_thread in search_threads:
            search_thread.join()

        while not POI_QUEUE.empty():
            pass
        
        global PARSER_EXIT_FLAG
        PARSER_EXIT_FLAG = True

        poi_parser.join()

        poi_db.executemany('insert into poi values (?,?,?,?,?,?,?,?,?,?,?)', data_batch)
        poi_db.commit()
        data_batch[:] = []

        print('Exiting Main Thread')
        with ERROR_LOCK:
            f_pares_error.close()

if(__name__ == '__main__'):
    main()
