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

def run_spider(mode, input_filename, output_filename, crawl_parameter):
    start = time.time()
    # 默认输入文件类型为.csv，默认输出文件类型为.db
    input_file = global_settings.OD_URL + input_filename + '.csv'
    output_file = global_settings.PATH_DATA_URL + output_filename + '.db'
    # 判断待抓取的OD文件是否存在
    if(not os.path.exists(input_file)):
        print('The OD file is not existed!')
        return
    # 判断是否已经抓取过该OD文件的路径
    if(os.path.exists(output_file)):
        print('The path data have been crawled.')
        return

    data_reader = ODReader(input_file, OD_QUEUE)
    data_reader.read_data()

    result_data_db = sqlite3.connect(output_file)
    with result_data_db:
        cursor = result_data_db.cursor()
        try:

            # 根据不同的交通方式创建不同的数据库格式
            if(str(mode) == '1'):
                cursor.execute(
                    'create table path_data(id int primary key, origin_lat varchar(20), origin_lng varchar(20), destination_lat varchar(20), destination_lng varchar(20), origin varchar(20), destination varchar(20), origin_region varchar(20), destination_region varchar(20), duration_s double, distance_km double, path varchar(255))')
            elif(str(mode) == '2'):
                cursor.execute(
                    'create table path_data(id int primary key, origin_lat varchar(20), origin_lng varchar(20), destination_lat varchar(20), destination_lng varchar(20), origin_city varchar(20), destination_city varchar(20), duration_s double, distance_km double, price_yuan double, path varchar(255))')
            elif(str(mode == '3')):
                cursor.execute(
                    'create table path_data(id int primary key, origin_lat varchar(20), origin_lng varchar(20), destination_lat varchar(20), destination_lng varchar(20), duration_s double, distance_km double, taxi_cost double, path varchar(255))')

        except Exception as create_db_error:
            print('create database error: {}'.format(create_db_error))

    parse_error_file = global_settings.PARSE_ERROR_URL + output_filename + '.csv'
    crawl_error_file = global_settings.CRAWL_ERROR_URL + output_filename + '.csv'
    with open(crawl_error_file, mode='w', encoding='utf-8') as f_crawl_error, open(parse_error_file, mode='w', encoding='utf-8') as f_parse_error:

        # 根据不同的交通方式写入不同的error文件头
        if(str(mode) == '1'):
            f_crawl_error.write(
                'id,origin_lat,origin_lng,destination_lat,destination_lng,origin,destination,origin_region,destination_region\n')
            f_parse_error.write(
                'id,origin_lat,origin_lng,destination_lat,destination_lng,origin,destination,origin_region,destination_region\n')
        elif(str(mode) == '2'):
            f_crawl_error.write(
                'id,origin_lat,origin_lng,destination_lat,destination_lng\n')
            f_parse_error.write(
                'id,origin_lat,origin_lng,destination_lat,destination_lng\n')
        elif(str(mode) == '3'):
            f_crawl_error.write(
                'id,origin_lat,origin_lng,destination_lat,destination_lng\n')
            f_parse_error.write(
                'id,origin_lat,origin_lng,destination_lat,destination_lng\n')

        crawler_threads = []
        crawler_list = ['crawl_thread' + str(num) for num in range(50)]
        for crawler_thread_id in crawler_list:

            # 根据不同的交通方式启用不同的抓取线程
            if(str(mode) == '1'):    
                crawler_thread = crawler.BaiduDrivingCrawlerThread(
                    thread_id=crawler_thread_id,
                    od_queue=OD_QUEUE,
                    path_queue=PATH_QUEUE,
                    crawl_parameter=crawl_parameter,
                    error_file=f_crawl_error,
                    error_lock=ERROR_LOCK
                )
                
            elif(str(mode) == '2'):
                crawler_thread = crawler.BaiduTransitCrawlerThread(
                    thread_id=crawler_thread_id,
                    od_queue=OD_QUEUE,
                    path_queue=PATH_QUEUE,
                    crawl_parameter=crawl_parameter,
                    error_file=f_crawl_error,
                    error_lock=ERROR_LOCK
                )
            elif(str(mode) == '3'):
                crawler_thread = crawler.AMapTransitCrawlerThread(
                    thread_id=crawler_thread_id,
                    od_queue=OD_QUEUE,
                    path_queue=PATH_QUEUE,
                    crawl_parameter=crawl_parameter,
                    error_file=f_crawl_error,
                    error_lock=ERROR_LOCK
                )
            crawler_thread.start()
            crawler_threads.append(crawler_thread)

        data_batch = []
        parser_thread_id = 'parser_thread'

        # 根据不同的交通方式启动不同的解析线程
        if(str(mode) == '1'):
            parser_thread = parser.BaiduDrivingParserThread(
                thread_id=parser_thread_id,
                path_queue=PATH_QUEUE,
                db_name=output_file,
                error_file=f_parse_error,
                error_lock=ERROR_LOCK,
                data_batch=data_batch
            )
        elif(str(mode) == '2'):
            parser_thread = parser.BaiduTransitParserThread(
                thread_id=parser_thread_id,
                path_queue=PATH_QUEUE,
                db_name=output_file,
                error_file=f_parse_error,
                error_lock=ERROR_LOCK,
                data_batch=data_batch
            )
        elif(str(mode) == '3'):
            parser_thread = parser.AMapDrivingParserThread(
                thread_id=parser_thread_id,
                path_queue=PATH_QUEUE,
                db_name=output_file,
                error_file=f_parse_error,
                error_lock=ERROR_LOCK,
                data_batch=data_batch
            )
        parser_thread.start()

        # 等待OD队列清空
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

        # 根据不同的交通方式写入最后不同的内容
        if(str(mode) == '1'):
            cursor.executemany(
                'insert into path_data values (?,?,?,?,?,?,?,?,?,?,?,?)', data_batch)
        elif(str(mode) == '2'):
            cursor.executemany(
                'insert into path_data values (?,?,?,?,?,?,?,?,?,?,?)', data_batch)
        elif(str(mode) == '3'):
            cursor.executemany(
                'insert into path_data values (?,?,?,?,?,?,?,?,?)', data_batch)

        result_data_db.commit()
        data_batch[:] = []
        print('Exiting Main Thread')
        with ERROR_LOCK:
            f_crawl_error.close()
            f_parse_error.close()

    end = time.time()
    print("Time used: {}".format(end - start))




def main():
    """
    Main function
    """

    mode = input('请选择抓取路径的交通模式（1 百度-驾车模式；2 百度-公交模式；3 高德-驾车模式）：')

    # 百度-驾车模式
    if(str(mode) == '1'):
        input_filename = input('待抓取路径的城市组文件名（csv文件名，不需要输入文件拓展名）：')
        output_filename = input('输出文件名（不需要输入文件拓展名）：')

        # 使用百度驾车API时需要输入的参数
        coord_or_name = input('请选择OD数据类型（默认为1）：1 地点名称；2 经纬度')
        key = input('百度开发者密钥：')
        tactics = input('导航策略（默认为12，10 不走高速；11 常规路线；12 距离较短；13 躲避拥堵）：')
        crawl_parameter = {
            'coord_or_name': coord_or_name or '1',
            'tactics': tactics or '12',
            'key': key
        }

    # 百度-公交模式
    elif(str(mode) == '2'):
        input_filename = input(
            '待抓取路径的origin-destination文件名（csv文件名，不需要输入文件拓展名）：')
        output_filename = input('输出文件名（不需要输入文件拓展名）：')
        key = input('请输入开发者密钥：')
        coord_type = input('起终点坐标类型（默认为bd09ll）：')
        tactics_incity = input(
            '市内公交策略（默认为0；0 推荐；1 少换乘；2 少步行；3 不坐地铁；4 时间短；5 地铁优先）：')
        tactics_intercity = input('跨城公交换乘策略（默认为0；0 时间短；1 出发早；2 价格低）：')
        trans_type_intercity = input('跨城交通方式策略（默认为0，0 火车优先；1 飞机优先；2 大巴优先）')
        ret_coordtype = input('返回值的坐标类型（默认为bd09ll）：')

        crawl_parameter = {
            'coord_type': coord_type or 'bd09ll',
            'tactics_incity': tactics_incity or '0',
            'tactics_intercity': tactics_intercity or '0',
            'trans_type_intercity': trans_type_intercity or '0',
            'ret_coordtype': ret_coordtype or 'bd09ll',
            'key': key
        }

    # 高德-驾车模式
    elif(str(mode == '3')):
        input_filename = input('待抓取路径的城市组文件名（csv文件名，不需要输入文件拓展名）：')
        output_filename = input('输出文件名（不需要输入文件拓展名）：')
        
        key = input('高德开发者密钥：') 
        strategy = input('驾车选择策略（默认为0）：（\n \
            0，不考虑当时路况，返回耗时最短的路线，但是此路线不一定距离最短\n \
            1，不走收费路段，且耗时最少的路线\n \
            2，不考虑路况，仅走距离最短的路线，但是可能存在穿越小路/小区的情况\n \
            4，躲避拥堵的路线，但是可能会存在绕路的情况，耗时可能较长\n \
            5，不走高速，但是不排除走其余收费路段\n \
            6，不走高速且避免所有收费路段\n \
            7，躲避收费和拥堵，可能存在走高速的情况，并且考虑路况不走拥堵路线，但有可能存在绕路和时间较长\n \
            8，不走高速且躲避收费和拥堵）：')
        
        crawl_parameter = {
            'strategy': strategy or '0',
            'key': key
        }

    run_spider(mode, input_filename, output_filename, crawl_parameter)




        # start = time.time()

        # # 默认输入文件类型为.csv，默认输出文件类型为.db
        # od_file = global_settings.OD_URL + input_filename + '.csv'
        # path_data_file = global_settings.PATH_DATA_URL + output_filename + '.db'

        # # 判断待抓取的OD文件是否存在
        # if(not os.path.exists(od_file)):
        #     print('The OD file is not existed!')
        #     return

        # # 判断是否已经抓取过该OD文件的路径
        # if(os.path.exists(path_data_file)):
        #     print('The path data have been crawled.')
        #     return

        # data_reader = ODReader(od_file, OD_QUEUE)
        # data_reader.read_data()

        # path_data_db = sqlite3.connect(path_data_file)
        # with path_data_db:
        #     cursor = path_data_db.cursor()
        #     try:
        #         cursor.execute('create table path_data(id int primary key, origin_city varchar(20), destination_city varchar(20), origin_region varchar(20), destination_region varchar(20), duration_s double, distance_km double, path varchar(255))')
        #     except Exception as create_db_error:
        #         print('create database error: {}'.format(create_db_error))

        # parse_error_path = global_settings.PARSE_ERROR_URL + output_filename
        # if(os.path.exists(parse_error_path)):
        #     parse_error_file = parse_error_path + '/parse_error.csv'
        # else:
        #     os.mkdir(parse_error_path)
        #     parse_error_file = parse_error_path + '/parse_error.csv'
        
        # crawl_error_path = global_settings.CRAWL_ERROR_URL + output_filename
        # if(os.path.exists(crawl_error_path)):
        #     crawl_error_file = crawl_error_path + '/crawl_error.csv'
        # else:
        #     os.mkdir(crawl_error_path)
        #     crawl_error_file = crawl_error_path + '/crawl_error.csv'

        # with open(crawl_error_file, mode='w', encoding='utf-8') as crawl_error_file, open(parse_error_file, mode='w', encoding='utf-8') as parse_error_file:
        #     crawler_threads = []
        #     crawler_list = ['crawl_thread' + str(num) for num in range(50)]

        #     crawl_error_file.write(
        #         'id,origin_city,destination_city,origin_region,destination_region\n')
        #     parse_error_file.write(
        #         'id,origin_city,destination_city,origin_region,destination_region\n')

        #     for crawler_thread_id in crawler_list:
        #         crawler_thread = crawler.BaiduDrivingCrawlerThread(
        #             thread_id=crawler_thread_id,
        #             od_queue=OD_QUEUE,
        #             path_queue=PATH_QUEUE,
        #             crawl_parameter=crawl_parameter,
        #             error_file=crawl_error_file,
        #             error_lock=ERROR_LOCK
        #         )
        #         crawler_thread.start()
        #         crawler_threads.append(crawler_thread)

        #     data_batch = []
        #     parser_thread_id = 'parser_thread'
        #     parser_thread = parser.BaiduDrivingParserThread(
        #         thread_id=parser_thread_id,
        #         path_queue=PATH_QUEUE,
        #         db_name=path_data_file,
        #         error_file=parse_error_file,
        #         error_lock=ERROR_LOCK,
        #         data_batch=data_batch
        #     )
        #     parser_thread.start()

        #     # 等待OD队列清空
        #     while not OD_QUEUE.empty():
        #         pass

        #     # 等待所有抓取线程完成
        #     for crawler_thread in crawler_threads:
        #         crawler_thread.join()

        #     # 等待路径json队列清空
        #     while not PATH_QUEUE.empty():
        #         pass

        #     # 当所有队列清空后，将解析线程退出标记置为True，通知其退出
        #     parser.PARSER_EXIT_FLAG = True

        #     # 等待解析线程完成
        #     parser_thread.join()

        #     cursor.executemany('insert into path_data values (?,?,?,?,?,?,?,?)', data_batch)
        #     path_data_db.commit()
        #     data_batch[:] = []
        #     print('Exiting Main Thread')
        #     with ERROR_LOCK:
        #         crawl_error_file.close()
        #         parse_error_file.close()

        # end = time.time()
        # print("Time used: {}".format(end - start))








    # # 公交模式
    # elif(str(mode) == '2'):
    #     input_filename = input(
    #         '待抓取路径的origin-destination文件名（csv文件名，不需要输入文件拓展名）：')
    #     output_filename = input('输出文件名（不需要输入文件拓展名）：')
    #     key = input('请输入开发者密钥：')
    #     coord_type = input('起终点坐标类型（默认为bd09ll）：')
    #     tactics_incity = input(
    #         '市内公交策略（默认为0；0 推荐；1 少换乘；2 少步行；3 不坐地铁；4 时间短；5 地铁优先）：')
    #     tactics_intercity = input('跨城公交换乘策略（默认为0；0 时间短；1 出发早；2 价格低）：')
    #     trans_type_intercity = input('跨城交通方式策略（默认为0，0 火车优先；1 飞机优先；2 大巴优先）')
    #     ret_coordtype = input('返回值的坐标类型（默认为bd09ll）：')

    #     crawl_parameter = {
    #         'coord_type': coord_type or 'bd09ll',
    #         'tactics_incity': tactics_incity or '0',
    #         'tactics_intercity': tactics_intercity or '0',
    #         'trans_type_intercity': trans_type_intercity or '0',
    #         'ret_coordtype': ret_coordtype or 'bd09ll',
    #         'key': key
    #     }

        # start = time.time()

        # # 默认输入文件类型为.csv，默认输出文件类型为.db
        # od_file = global_settings.OD_URL + input_filename + '.csv'
        # path_data_file = global_settings.PATH_DATA_URL + output_filename + '.db'

        # # 判断待抓取的OD文件是否存在
        # if(not os.path.exists(od_file)):
        #     print('The OD file is not existed!')
        #     return

        # # 判断是否已经抓取过该OD文件的路径
        # if(os.path.exists(path_data_file)):
        #     print('The path data have been crawled.')
        #     return

        # data_reader = ODReader(od_file, OD_QUEUE)
        # data_reader.read_data()

        # # TODO: 不同交通方式第一处不同：需要创建不同的写入文件格式
        # path_data_db = sqlite3.connect(path_data_file)
        # with path_data_db:
        #     cursor = path_data_db.cursor()
        #     try:
        #         cursor.execute('create table path_data(id int primary key, origin_coord varchar(30), des_coord varchar(30), origin_city varchar(20), destination_city varchar(20), duration_s double, distance_km double, price_yuan double, path varchar(255))')
        #     except Exception as create_db_error:
        #         print('create database error: {}'.format(create_db_error))

        # # 解析错误的OD数据存储的文件
        # parse_error_path = global_settings.PARSE_ERROR_URL + output_filename
        # if(os.path.exists(parse_error_path)):
        #     parse_error_file = parse_error_path + '/parse_error.csv'
        # else:
        #     os.mkdir(parse_error_path)
        #     parse_error_file = parse_error_path + '/parse_error.csv'

        # # 抓取错误的OD数据存储的文件
        # crawl_error_path = global_settings.CRAWL_ERROR_URL + output_filename
        # if(os.path.exists(crawl_error_path)):
        #     crawl_error_file = crawl_error_path + '/crawl_error.csv'
        # else:
        #     os.mkdir(crawl_error_path)
        #     crawl_error_file = crawl_error_path + '/crawl_error.csv'

        # # TODO: 不同模式第二处不同：需要使用不同的抓取和解析线程
        # with open(crawl_error_file, mode='w', encoding='utf-8') as crawl_error_file, open(parse_error_file, mode='w', encoding='utf-8') as parse_error_file:
        #     crawler_threads = []
        #     crawler_list = ['crawl_thread' + str(num) for num in range(50)]

        #     crawl_error_file.write(
        #         'id,origin_coord,des_coord\n')
        #     parse_error_file.write(
        #         'id,origin_coord,des_coord\n')

        #     for crawler_thread_id in crawler_list:
        #         crawler_thread = crawler.TransitCrawlerThread(
        #             thread_id=crawler_thread_id,
        #             od_queue=OD_QUEUE,
        #             path_queue=PATH_QUEUE,
        #             crawl_parameter=crawl_parameter,
        #             error_file=crawl_error_file,
        #             error_lock=ERROR_LOCK
        #         )
        #         crawler_thread.start()
        #         crawler_threads.append(crawler_thread)

        #     data_batch = []
        #     parser_thread_id = 'parser_thread'
        #     parser_thread = parser.TransitParserThread(
        #         thread_id=parser_thread_id,
        #         path_queue=PATH_QUEUE,
        #         db_name=path_data_file,
        #         error_file=parse_error_file,
        #         error_lock=ERROR_LOCK,
        #         data_batch=data_batch
        #     )
        #     parser_thread.start()

        #     # 等待OD队列清空
        #     while not OD_QUEUE.empty():
        #         pass

        #     # 等待所有抓取线程完成
        #     for crawler_thread in crawler_threads:
        #         crawler_thread.join()

        #     # 等待路径json队列清空
        #     while not PATH_QUEUE.empty():
        #         pass

        #     # 当所有队列清空后，将解析线程退出标记置为True，通知其退出
        #     parser.PARSER_EXIT_FLAG = True

        #     # 等待解析线程完成
        #     parser_thread.join()

        #     # TODO: 不同模式第三处不同：最后清空data_batch时写入的格式不同
        #     cursor.executemany(
        #         'insert into path_data values (?,?,?,?,?,?,?,?,?)', data_batch)
        #     path_data_db.commit()
        #     data_batch[:] = []
        #     print('Exiting Main Thread')
        #     with ERROR_LOCK:
        #         crawl_error_file.close()
        #         parse_error_file.close()

        # end = time.time()
        # print("Time used: {}".format(end - start))

if(__name__ == '__main__'):
    main()
