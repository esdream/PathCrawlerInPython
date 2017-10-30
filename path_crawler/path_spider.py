# _*_ coding: utf-8 _*_

"""The path crawler.

Crawl the path using Baidu Map direction API.
"""

import os
import time
import threading
import sqlite3
import queue

from path_crawler.conf import global_settings
from path_crawler.reader import ODReader
from path_crawler import crawler, parser

OD_QUEUE = queue.Queue()
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

    mode = input('请选择抓取路径的交通模式（1 公交模式；2 驾车模式）：')
    if(str(mode) == 1):
        input_filename = input('待抓取路径的origin-destination文件名（csv文件名，不需要输入文件拓展名）：')
        output_filename = input('输出文件名（csv文件名，不需要输入文件拓展名）：')
        ak = input('请输入开发者密钥：')
        coord_type = input('起终点坐标类型（默认为bd09ll）：')
        tactics_incity = input(
            '市内公交策略（默认为0；0 推荐；1 少换乘；2 少步行；3 不坐地铁；4 时间短；5 地铁优先）：')
        tactics_intercity = input('跨城公交换乘策略（默认为0；0 时间短；1 出发早；2 价格低）：')
        trans_type_intercity = input('跨城交通方式策略（默认为0，0 火车优先；1 飞机优先；2 大巴优先）')
        ret_coordtype = input('返回值的坐标类型（默认为bd09ll）：')

        crawler_parameter = {
            'ak': ak,
            'coord_type': coord_type or 'bd09ll',
            'tactics_incity': tactics_incity or '0',
            'tactics_intercity': tactics_intercity or '0',
            'trans_type_intercity': trans_type_intercity or '0',
            'ret_coordtype': ret_coordtype or 'bd09ll'
        }



    # TODO: 完成驾车模式的input
    input_filename = input('待抓取路径的城市组文件名（不需要输入文件拓展名）: ')
    output_filename = input('输出文件名（不需要输入文件拓展名）: ')

    # 使用百度API时需要输入的参数
    ak = input('开发者密钥（企业号/个人号）: ')
    crawl_parameter = {
        'ak': ak
    }




    start = time.time()

    # 默认输入文件类型为.csv，默认输出文件类型为.db
    od_file = global_settings.OD_URL + input_filename + '.csv'
    path_data_file = global_settings.PATH_DATA_URL + output_filename + '.db'

    # 判断待抓取的OD文件是否存在
    if(not os.path.exists(od_file)):
        print('The OD file is not existed!')
        return

    # 判断是否已经抓取过该OD文件的路径
    if(os.path.exists(path_data_file)):
        print('The path data have been crawled.')
        return

    data_reader = ODReader(od_file, OD_QUEUE)
    data_reader.read_data()




    # TODO: 不同交通方式第一处不同：需要创建不同的写入文件格式
    path_data_db = sqlite3.connect(path_data_file)
    with path_data_db:
        cursor = path_data_db.cursor()
        try:
            cursor.execute('create table path_data(id int primary key, origin_city varchar(20), destination_city varchar(20), origin_region varchar(20), destination_region varchar(20), duration_s double, distance_km double, path varchar(255))')
        except Exception as create_db_error:
            print('create database error: {}'.format(create_db_error))





    parse_error_path = global_settings.PARSE_ERROR_URL + output_filename
    if(os.path.exists(parse_error_path)):
        parse_error_file = parse_error_path + '/parse_error.csv'
    else:
        os.mkdir(parse_error_path)
        parse_error_file = parse_error_path + '/parse_error.csv'
    
    crawl_error_path = global_settings.CRAWL_ERROR_URL + output_filename
    if(os.path.exists(crawl_error_path)):
        crawl_error_file = crawl_error_path + '/crawl_error.csv'
    else:
        os.mkdir(crawl_error_path)
        crawl_error_file = crawl_error_path + '/crawl_error.csv'





    # TODO: 不同模式第二处不同：需要使用不同的抓取和解析线程
    with open(crawl_error_file, mode='w', encoding='utf-8') as crawl_error_file, open(parse_error_file, mode='w', encoding='utf-8') as parse_error_file:
        crawler_threads = []
        crawler_list = ['crawl_thread' + str(num) for num in range(50)]

        crawl_error_file.write('id,origin_city,destination_city,origin_region,destination_region\n')
        parse_error_file.write('id,origin_city,destination_city,origin_region,destination_region\n')

        for crawler_thread_id in crawler_list:
            crawler_thread = crawler.DrivingCrawlerThread(
                thread_id=crawler_thread_id,
                city_com_queue=OD_QUEUE,
                path_queue=PATH_QUEUE,
                crawl_parameter=crawl_parameter,
                error_file=crawl_error_file,
                error_lock=ERROR_LOCK
            )
            crawler_thread.start()
            crawler_threads.append(crawler_thread)

        data_batch = []
        parser_thread_id = 'parser_thread'
        parser_thread = parser.DrivingParserThread(
            thread_id=parser_thread_id,
            path_queue=PATH_QUEUE,
            db_name=path_data_file,
            error_file=parse_error_file,
            error_lock=ERROR_LOCK,
            data_batch=data_batch
        )
        parser_thread.start()

        # 等待城市组合队列清空
        while not OD_QUEUE.empty():
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




        # TODO: 不同模式第三处不同：最后清空data_batch时写入的格式不同
        cursor.executemany('insert into path_data values (?,?,?,?,?,?,?,?)', data_batch)
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
