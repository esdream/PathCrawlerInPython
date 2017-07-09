# _*_ coding: utf-8 _*_

"""The path crawler.

Crawl the path using Baidu Map direction API.
"""

import os
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

    def __init__(self, thread_id, city_com_queue, path_queue):
        threading.Thread.__init__(self)
        self._thread_id = thread_id
        self._city_com_queue = city_com_queue
        self._path_queue = path_queue

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
                        path_info['path_json'] = requests.get(url).json()
                        self._path_queue.put(path_info)
                        break
                    except Exception as crawl_error:
                        print('Crawl path {0[0]} failed!'.format(city_coms_data))
                        print(crawl_error)

# 解析线程
class ParserThread(threading.Thread):
    """Path data parser thread.

    Parse the path data crawled from Baidu Map.
    """

    def __init__(self, thread_id, path_queue, cursor):
        threading.Thread.__init__(self)
        self._thread_id = thread_id
        self._path_queue = path_queue
        self._cursor = cursor

    def run(self):
        print('No.{} crawler start...'.format(self._thread_id))
        self.__data_parser()
        print('No.{} crawler finished!'.format(self._thread_id))

    # 解析路径、时间、距离
    def __data_parser(self):
        """Parser of json data.

        Parse the data from Baidu Map.
        """

        global PARSER_EXIT_FLAG
        while not PARSER_EXIT_FLAG:
            try:
                path_info = self._path_queue.get(True, 20)
                result = {}
                result['id'] = path_info['city_com_num']
                result['origin'] = path_info['origin']
                result['destination'] = path_info['destination']
                result['driving_duration'] = path_info['path_json'][u'result'][u'routes'][0][u'duration']
                result['distance_km'] = path_info['path_json'][u'result'][u'routes'][0][u'distance'] / 1000

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

                self.__data_saver(result)

            except Exception as parser_error:
                # print('Parse path {} failed!'.format(path_info['city_com_num']))
                print(parser_error)


    # 存储解析数据
    def __data_saver(self, result):
        """Data saver.

        Save the parsed data in database
        """

        # TODO(conquerfu@gmail.com): 给这里的数据库加锁
        cursor = self._cursor()
        cursor.execute('insert into path_data values (?,?,?,?,?,?)', (result['id'], result['origin'], result['destination'], result['driving_duration'], result['distance_km'], result['path']))



CITY_COMS_QUEUE = queue.Queue()
PATH_QUEUE = queue.Queue()
PARSER_EXIT_FLAG = False


def main():
    """
    Main function
    """
    data_reader = CityCombinationsReader('city_dataset_test', CITY_COMS_QUEUE)
    data_reader.read_data()

    path_data_db = sqlite3.connect('path_data.db')

    with path_data_db:

        cursor = path_data_db.cursor()
        cursor.execute('create table path_data(id int primary key, origin_city varchar(20), destination_city varchar(20), duration double, distance double, path varchar(255))')

        crawler_threads = []
        crawler_list = ['crawl_thread1', 'crawl_thread2', 'crawl_thread3']

        for crawler_thread_id in crawler_list:
            crawler_thread = PathCrawlerThread(crawler_thread_id, CITY_COMS_QUEUE, PATH_QUEUE)
            crawler_thread.start()
            crawler_threads.append(crawler_thread)

        parser_threads = []
        parser_list = ['parse_thread1', 'parse_thread2', 'parse_thread3']

        for parser_thread_id in parser_list:
            parser_thread = ParserThread(parser_thread_id, PATH_QUEUE, cursor)
            parser_thread.start()
            parser_threads.append(parser_thread)

        # 等待城市组合队列清空
        while not CITY_COMS_QUEUE.empty():
            pass

        # 等待所有抓取线程完成
        for crawler_thread in crawler_threads:
            crawler_thread.join()

        # 等待路径json队列清空
        while not PATH_QUEUE.empty():
            pass

        # 等待所有解析线程完成
        for parser_thread in parser_threads:
            parser_thread.join()

        # 当所有队列清空后，将解析线程退出标记置为True，通知其推出
        global PARSER_EXIT_FLAG
        PARSER_EXIT_FLAG = True

        print('Exiting Main Thread')

if(__name__ == '__main__'):
    main()
