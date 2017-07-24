# _*_ coding: utf-8 _*_

"""The path crawler.

Crawl the path using Baidu Map direction API.
"""

import os
import time
import threading
import sqlite3
import queue
import requests


# 读取数据库中的城市组合
class CityCombinationsReader(object):
    """city combinations reader.

    Read the combinations of city from combination database.
    """

    def __init__(self, database_name, city_com_queue):
        self._database_name = database_name
        self._city_com_queue = city_com_queue

    def read_data(self):
        """Initiator of CityCombinationsReader.

        Run the program of reader.
        """

        print('Reader of combinations start...')
        self.__combination_database_reader()
        print('Reader of combinations finished. The city combinations have read in memory.')

    def __combination_database_reader(self):
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
                self._city_com_queue.put(data)


# 抓取线程
class PathCrawlerThread(threading.Thread):
    """Path crawler.

    Crawl the path using Baidu Map direction API.
    """

    def __init__(self, **crawler_args):
        threading.Thread.__init__(self)
        self._thread_id = crawler_args['thread_id']
        self._city_com_queue = crawler_args['city_com_queue']
        self._path_queue = crawler_args['path_queue']
        self._error_file = crawler_args['error_file']
        self._error_lock = crawler_args['error_lock']

    def run(self):
        print('No.{} crawler start...'.format(self._thread_id))
        self.__path_crawler()
        print('No.{} crawler finished!'.format(self._thread_id))

    def __path_crawler(self):
        """Crawler function.

        Send requests to Baidu Map and get the path json data.
        """

        while True:
            if(self._city_com_queue.empty()):
                break
            else:
                city_coms_data = self._city_com_queue.get()
                print('path {0[0]}: From {0[1]} to {0[2]}'.format(city_coms_data))
                url = 'http://api.map.baidu.com/direction/v1?mode=driving&origin={0[1]}&destination={0[2]}&origin_region={0[1]}&destination_region={0[2]}&output=json&ak=YtsG0tZOwjVgDkcLZDuEiSL2PbKzP9HG'.format(city_coms_data)

                timeout = 3
                while(timeout > 0):
                    timeout -= 1
                    try:
                        path_info = {}
                        path_info['city_com_num'] = city_coms_data[0]
                        path_info['origin'] = city_coms_data[1]
                        path_info['destination'] = city_coms_data[2]
                        path_info['path_json'] = requests.get(url, timeout=10).json()
                        self._path_queue.put(path_info)
                        break
                    except Exception as crawl_error:
                        with self._error_lock:
                            self._error_file.write('{0}\t{1}\t{2}\n'.format(city_coms_data[0], city_coms_data[1], city_coms_data[2]))
                            print('Crawl path {0[0]} failed!'.format(city_coms_data))
                            print(crawl_error)


# 解析线程
class ParserThread(threading.Thread):
    """Path data parser thread.

    Parse the path data crawled from Baidu Map.
    """

    def __init__(self, **parser_args):
        threading.Thread.__init__(self)
        self._thread_id = parser_args['thread_id']
        self._path_queue = parser_args['path_queue']
        self._db_name = parser_args['db_name']
        self._error_file = parser_args['error_file']
        self._error_lock = parser_args['error_lock']

    def run(self):
        print('No.{} crawler start...'.format(self._thread_id))
        self.__data_parser()
        print('No.{} crawler finished!'.format(self._thread_id))

    # 解析路径、时间、距离
    def __data_parser(self):
        """Parser of json data.

        Parse the data from Baidu Map.
        """

        path_data_db = sqlite3.connect(self._db_name)
        with path_data_db:

            cursor = path_data_db.cursor()

            global PARSER_EXIT_FLAG
            while not PARSER_EXIT_FLAG:

                try:
                    path_info = self._path_queue.get(True, 20)

                    timeout = 2
                    while(timeout > 0):
                        timeout -= 1

                        result = {}
                        result['id'] = path_info['city_com_num']
                        result['origin'] = path_info['origin']
                        result['destination'] = path_info['destination']
                        result['driving_duration'] = path_info['path_json'][u'result'][u'routes'][0][u'duration']
                        result['distance_km'] = path_info['path_json'][u'result'][u'routes'][0][u'distance'] / 1000

                        # 解析路径
                        num_of_steps = len(path_info['path_json'][u'result'][u'routes'][0][u'steps'])
                        path_string = ''
                        for i in range(num_of_steps):
                            if(i == num_of_steps - 1):
                                path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path']
                                break
                            path_list = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path'].split(';')
                            path_string += ";".join(path_list[0:-1]) + ';'
                        result['path'] = path_string

                        print('From:{} to {}, duration: {}, distance: {}'.format(result['origin'], result['destination'], result['driving_duration'], result['distance_km']))

                        cursor.execute('insert into path_data values (?,?,?,?,?,?)', (result['id'], result['origin'], result['destination'], result['driving_duration'], result['distance_km'], result['path']))

                        path_data_db.commit()

                        self._path_queue.task_done()
                        break

                except Exception as parser_error:
                    with self._error_lock:
                        self._error_file.write('{0}\t{1}\t{2}\n'.format(path_info['city_com_num'], path_info['origin'], path_info['destination']))
                        print('Parse path {} failed!'.format(path_info['city_com_num']))
                        print(parser_error)


CITY_COMS_QUEUE = queue.Queue()
PATH_QUEUE = queue.Queue()
PARSER_EXIT_FLAG = False
ERROR_LOCK = threading.Lock()


def main():
    """
    Main function
    """

    data_base_start_num = input('数据库起始num:')

    start = time.time()

    data_base_end_num = str(int(data_base_start_num) + 200000)

    path_db_name = 'path_data_{0}_to_{1}.db'.format(data_base_start_num, data_base_end_num)

    if(os.path.isfile(path_db_name)):
        print('This database of city combinations have been crawled.')
        return

    city_coms_db_name = 'city_coms_{0}_to_{1}'.format(data_base_start_num, data_base_end_num)
    data_reader = CityCombinationsReader(city_coms_db_name, CITY_COMS_QUEUE)
    data_reader.read_data()

    path_data_db = sqlite3.connect(path_db_name)
    with path_data_db:
        cursor = path_data_db.cursor()
        try:
            cursor.execute('create table path_data(id int primary key, origin_city varchar(20), destination_city varchar(20), duration double, distance double, path varchar(255))')
        except Exception as create_db_error:
            print('create database error: {}'.format(create_db_error))

    with open('crawl_error.txt', 'a') as crawl_error_file, open('parse_error.txt', 'a') as parse_error_file:
        crawler_threads = []
        crawler_list = ['crawl_thread' + str(num) for num in range(100)]

        for crawler_thread_id in crawler_list:
            crawler_thread = PathCrawlerThread(thread_id=crawler_thread_id, city_com_queue=CITY_COMS_QUEUE, path_queue=PATH_QUEUE, error_file=crawl_error_file, error_lock=ERROR_LOCK)
            crawler_thread.start()
            crawler_threads.append(crawler_thread)

        parser_thread_id = 'parser_thread'
        parser_thread = ParserThread(thread_id=parser_thread_id, path_queue=PATH_QUEUE, db_name=path_db_name, error_file=parse_error_file, error_lock=ERROR_LOCK)
        parser_thread.start()

        # 等待城市组合队列清空
        while not CITY_COMS_QUEUE.empty():
            pass

        # 等待所有抓取线程完成
        for crawler_thread in crawler_threads:
            crawler_thread.join()

        # 等待路径json队列清空
        while not PATH_QUEUE.empty():
            pass

        # 当所有队列清空后，将解析线程退出标记置为True，通知其退出
        global PARSER_EXIT_FLAG
        PARSER_EXIT_FLAG = True

        # 等待解析线程完成
        parser_thread.join()

        print('Exiting Main Thread')
        with ERROR_LOCK:
            crawl_error_file.close()
            parse_error_file.close()

    end = time.time()
    print("Time used: {}".format(end - start))

if(__name__ == '__main__'):
    main()
