# _*_ coding: utf-8 _*_

"""The path crawler.

Crawl the path using Baidu Map direction API.
"""

import os
import time
import threading
import sqlite3
import queue

from path_crawler.reader import CityCombinationsReader
from path_crawler.crawler import PathCrawlerThread
from path_crawler import parser
from path_crawler.parser import ParserThread

CITY_COMS_QUEUE = queue.Queue()
PATH_QUEUE = queue.Queue(300)
# PARSER_EXIT_FLAG = False
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
        crawler_list = ['crawl_thread' + str(num) for num in range(50)]

        for crawler_thread_id in crawler_list:
            crawler_thread = PathCrawlerThread(thread_id=crawler_thread_id, city_com_queue=CITY_COMS_QUEUE, path_queue=PATH_QUEUE, error_file=crawl_error_file, error_lock=ERROR_LOCK)
            crawler_thread.start()
            crawler_threads.append(crawler_thread)

        data_batch = []
        parser_thread_id = 'parser_thread'
        parser_thread = ParserThread(thread_id=parser_thread_id, path_queue=PATH_QUEUE, db_name=path_db_name, error_file=parse_error_file, error_lock=ERROR_LOCK, data_batch=data_batch)
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
        parser.PARSER_EXIT_FLAG = True

        # 等待解析线程完成
        parser_thread.join()

        cursor.executemany('insert into path_data values (?,?,?,?,?,?)', data_batch)
        path_data_db.commit()
        data_batch[:] = []
        
        print('Exiting Main Thread')
        with ERROR_LOCK:
            crawl_error_file.close()
            parse_error_file.close()

    end = time.time()
    print("Time used: {}".format(end - start))

if(__name__ == '__main__'):
    main()
