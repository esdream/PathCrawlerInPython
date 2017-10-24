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
ERROR_LOCK = threading.Lock()

#
# # 设置参数并爬取路径和时间
# def pathCrawler():
#
#     data = readComOfCity(data)
#
#     coord_type = input('坐标类型(默认为百度经纬度坐标):')
#     tactics_incity = input('市内公交换乘策略(默认为0):')
#     tactics_intercity = input('跨城公交换乘策略(默认为0):')
#     trans_type_intercity = input('跨城交通方式策略(默认为0):')
#     ret_coordtype = input('返回值坐标类型(默认为百度经纬度坐标):')
#     ak = input('开发者密钥:')
#
#     parameter = {
#         'coord_type': coord_type,
#         'tactics_incity': tactics_incity,
#         'tactics_intercity': tactics_intercity,
#         'trans_type_intercity': trans_type_intercity,
#         'ret_coordtype': ret_coordtype,
#         'ak': ak or 'YtsG0tZOwjVgDkcLZDuEiSL2PbKzP9HG'
#     }
#
#     crawPathAndTime(data, parameter)

def main():
    """
    Main function
    """

    input_filename = input('待抓取路径的城市组文件: ')
    output_filename = input('输出文件名: ')

    start = time.time()

    city_coms_file = input_filename
    path_data_file = output_filename

    # 判断待抓取的城市组文件是否存在
    if(not os.path.exists(city_coms_file)):
        print('The city coms file is not existed!')
        return

    # 判断是否已经抓取过该城市组的路径
    if(os.path.exists(path_data_file)):
        print('This database of city combinations have been crawled.')
        return

    data_reader = CityCombinationsReader(city_coms_file, CITY_COMS_QUEUE)
    data_reader.read_data()

    path_data_db = sqlite3.connect(path_data_file)
    with path_data_db:
        cursor = path_data_db.cursor()
        try:
            cursor.execute('create table path_data(id int primary key, origin_city varchar(20), destination_city varchar(20), duration double, distance double, path varchar(255))')
        except Exception as create_db_error:
            print('create database error: {}'.format(create_db_error))

    with open('crawl_error.csv', mode='w', encoding='utf-8') as crawl_error_file, open('parse_error.csv', mode='w', encoding='utf-8') as parse_error_file:
        crawler_threads = []
        crawler_list = ['crawl_thread' + str(num) for num in range(50)]

        crawl_error_file.write('id,origin_city,destination_city\n')
        parse_error_file.write('id,origin_city,destination_city\n')

        for crawler_thread_id in crawler_list:
            crawler_thread = PathCrawlerThread(thread_id=crawler_thread_id, city_com_queue=CITY_COMS_QUEUE, path_queue=PATH_QUEUE, error_file=crawl_error_file, error_lock=ERROR_LOCK)
            crawler_thread.start()
            crawler_threads.append(crawler_thread)

        data_batch = []
        parser_thread_id = 'parser_thread'
        parser_thread = ParserThread(thread_id=parser_thread_id, path_queue=PATH_QUEUE, db_name=path_data_file, error_file=parse_error_file, error_lock=ERROR_LOCK, data_batch=data_batch)
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
