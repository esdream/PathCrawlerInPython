# _*_ coding: utf-8 _*_

"""The path crawler.

Crawl the path using Baidu Map direction API.
"""

import os
import threading
from multiprocessing import Queue
import sqlite3
import requests

CITY_COMS_QUEUE = Queue()
PATH_QUEUE = Queue()


# 读取数据库中的城市组合
class CityCombinationsReader(object):
    """city combinations reader.

    Read the combinations of city from combination database.
    """

    def __init__(self, database_name):
        self._database_name = database_name

    def run(self):
        """Initiator of CityCombinationsReader.

        Run the program of reader.
        """

        print('Reader of combinations start...')
        self.combination_database_reader()
        print('Reader of combinations finished. The city combinations have read in memory.')

    def combination_database_reader(self):
        """Database Reader.

        Read the data from city combinations database.
        """

        try:
            db_name = '{}.db'.format(self._database_name)
            db_file = os.path.join(os.path.dirname(__file__), db_name)
            connection_of_coms_db = sqlite3.connect(db_file)
        except IOError as no_db_error:
            print('The city combinations database is not existed!')
            print(no_db_error)

        with connection_of_coms_db:
            cur = connection_of_coms_db.cursor()
            city_combinations_data = cur.execute('select * from city_combination').fetchall()
            for data in city_combinations_data:
                CITY_COMS_QUEUE.put(data)

# 拼接URL并发送请求
class PathCrawlerThread(threading.Thread):
    """Path crawler.

    Crawl the path using Baidu Map direction API.
    """

    def __init__(self, thread_id):
        threading.Thread.__init__(self)
        self._thread_id = thread_id

    def run(self):
        print('No.{} crawler start...'.format(self._thread_id))
        self.path_crawler()
        print('No.{} crawler finished!'.format(self._thread_id))

    def path_crawler(self):
        """Crawler function.

        Send requests to Baidu Map and get the data from responses.
        """

        while True:
            if(CITY_COMS_QUEUE.empty()):
                break
            else:
                city_coms_data = CITY_COMS_QUEUE.get()
                print('path {0[0]}: From {0[1]} to {0[2]}'.format(city_coms_data))
                url = 'http://api.map.baidu.com/direction/v1?mode=driving&origin={0[1]}&destination={0[2]}&origin_region={0[1]}&destination_region={0[2]}&output=json&ak=YtsG0tZOwjVgDkcLZDuEiSL2PbKzP9HG'.format(city_coms_data)

                timeout = 3
                while(timeout > 0):
                    timeout -= 1
                    try:
                        path_id = city_coms_data[0]
                        path_json = requests.get(url).json()
                        path_data = self.path_data_parser(path_id, path_json)
                        PATH_QUEUE.put(path_data)
                        break
                    except Exception as crawl_error:
                        print('Crawl path {0[0]} failed!'.format(city_coms_data))
                        print(crawl_error)

    # 解析路径/时间等
    def path_data_parser(self, path_id, response):
        """Parser function.

        Parse the data from Baidu Map.
        """

        while True:
            if(response[u'status'] not in [0]):
                print('Thread {}: path {} crawl failed.'.format(self._thread_id, path_id))
                break

            result = {}
            result['driving_duration'] = response[u'result'][u'routes'][0][u'duration']
            result['distance_km'] = response[u'result'][u'routes'][0][u'distance'] / 1000

            num_of_steps = len(response[u'result'][u'routes'][0][u'steps'])
            path_string = ''
            for i in range(num_of_steps):
                if(i == num_of_steps - 1):
                    path_string += response[u'result'][u'routes'][0][u'steps'][i][u'path']
                    break
                path_list = response[u'result'][u'routes'][0][u'steps'][i][u'path'].split(';')
                path_string += ";".join(path_list[0:-1]) + ';'

            return path_string
