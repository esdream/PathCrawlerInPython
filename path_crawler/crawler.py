"""
The Collection of Crawler

Provide many way to crawl different data.
"""

import time
import threading
import requests

# 百度-驾车模式抓取线程
class BaiduDrivingCrawlerThread(threading.Thread):
    """Driving path crawler.

    Crawl the path using driving mode(route planning v1.0).
    """

    def __init__(self, **crawler_args):
        threading.Thread.__init__(self)
        self._thread_id = crawler_args['thread_id']
        self._od_queue = crawler_args['od_queue']
        self._path_queue = crawler_args['path_queue']
        self._error_file = crawler_args['error_file']
        self._error_lock = crawler_args['error_lock']
        self._crawl_parameter = crawler_args['crawl_parameter']

    def run(self):
        print('No.{} crawler start...'.format(self._thread_id))
        self.__baidu_driving_path_crawler()
        print('No.{} crawler finished!'.format(self._thread_id))

    def __baidu_driving_path_crawler(self):
        """Crawler function.

        Send requests to Baidu Map and get the driving mode path data.
        """

        while True:
            if(self._od_queue.empty()):
                break
            else:
                city_coms_data = self._od_queue.get()
                print('path {0[0]}: From {0[1]}(region {0[3]}) to {0[2]}(region {0[4]}) crawled...'.format(
                    city_coms_data))
                url = 'http://api.map.baidu.com/direction/v1?mode=driving&origin={0[1]}&destination={0[2]}&origin_region={0[3]}&destination_region={0[4]}&tactics={tactics}&output=json&ak={key}'.format(
                    city_coms_data, **self._crawl_parameter)

                timeout = 2
                while(timeout > 0):
                    timeout -= 1
                    try:
                        if(self._path_queue.full()):
                            time.sleep(8)

                        path_info = {}
                        path_info['city_com_num'] = city_coms_data[0]
                        path_info['origin'] = city_coms_data[1]
                        path_info['destination'] = city_coms_data[2]
                        path_info['origin_region'] = city_coms_data[3]
                        path_info['destination_region'] = city_coms_data[4]
                        path_info['path_json'] = requests.get(url, timeout=5).json()
                        self._path_queue.put(path_info)
                        print('path {0[0]}: From {0[1]}(region {0[3]}) to {0[2]}(region {0[4]}) crawl succeed.'.format(
                            city_coms_data))
                        break
                    except Exception as crawl_error:
                        if(timeout == 0):
                            print(crawl_error)
                            self._od_queue.put(city_coms_data)
                            # with self._error_lock:
                            #     self._error_file.write('{0[0]},{0[1]},{0[2]},{0[3]},{0[4]}\n'.format(
                            #         city_coms_data))
                            #     print('Crawl path {0[0]} failed!'.format(
                            #         city_coms_data))
                            #     print(crawl_error)

# 百度-公交模式抓取线程
class BaiduTransitCrawlerThread(threading.Thread):
    """Transit path crawler.

    Crawl the path using transit mode(route planning v2.0).
    """

    def __init__(self, **crawler_args):
        threading.Thread.__init__(self)
        self._thread_id = crawler_args['thread_id']
        self._od_queue = crawler_args['od_queue']
        self._path_queue = crawler_args['path_queue']
        self._error_file = crawler_args['error_file']
        self._error_lock = crawler_args['error_lock']
        self._crawl_parameter = crawler_args['crawl_parameter']

    def run(self):
        print('No.{} crawler start...'.format(self._thread_id))
        self.__baidu_transit_path_crawler()
        print('No.{} crawler finished!'.format(self._thread_id))

    def __baidu_transit_path_crawler(self):
        """Crawler function.

        Send requests to Baidu Map and get the driving mode path data.
        """
        
        while True:
            if(self._od_queue.empty()):
                break
            else:
                od_data = self._od_queue.get()
                # 源文件格式应为：id,origin_lat,origin_lng,destination_lat,destination_lng
                print('path {0[0]}: From {0[1]},{0[2]} to {0[3]},{0[4]} crawled...'.format(
                    od_data))
                # crawl_param_values = []
                # for (_, values) in self._crawl_parameter.items():
                #     crawl_param_values.append(values)

                url = 'http://api.map.baidu.com/direction/v2/transit?origin={0[1]},{0[2]}&destination={0[3]},{0[4]}&coord_type={coord_type}&tactics_incity={tactics_incity}&tactics_intercity={tactics_intercity}&trans_type_intercity={trans_type_intercity}&ret_coordtype={ret_coordtype}&ak={ak}'.format(
                    od_data, **self._crawl_parameter)

                timeout = 2
                while(timeout > 0):
                    timeout -= 1
                    try:
                        if(self._path_queue.full()):
                            time.sleep(8)

                        path_info = {}
                        path_info['od_id'] = od_data[0]
                        path_info['origin_lat'] = od_data[1]
                        path_info['origin_lng'] = od_data[2]
                        path_info['destination_lat'] = od_data[3]
                        path_info['destination_lng'] = od_data[4]
                        path_info['path_json'] = requests.get(url, timeout=5).json()
                        self._path_queue.put(path_info)
                        print('path {0[0]}: From {0[1]},{0[2]} to {0[3]},{0[4]} crawl succeed.'.format(od_data))
                        break
                    except Exception as crawl_error:
                        if(timeout == 0):
                            print(crawl_error)
                            self._od_queue.put(od_data)


# 高德-驾车模式抓取线程
class AMapTransitCrawlerThread(threading.Thread):
    """AMap transit path crawler.

    Crawl the path using AMap driving mode.
    """

    def __init__(self, **crawler_args):
        threading.Thread.__init__(self)
        self._thread_id = crawler_args['thread_id']
        self._od_queue = crawler_args['od_queue']
        self._path_queue = crawler_args['path_queue']
        self._error_file = crawler_args['error_file']
        self._error_lock = crawler_args['error_lock']
        self._crawl_parameter = crawler_args['crawl_parameter']

    def run(self):
        print('No.{} crawler start...'.format(self._thread_id))
        self.__amap_driving_path_crawler()
        print('No.{} crawler finished!'.format(self._thread_id))

    def __amap_driving_path_crawler(self):
        """Crawler function.

        Send requests to AMap and get the driving mode path data.
        """

        while True:
            if(self._od_queue.empty()):
                break
            else:
                od_data = self._od_queue.get()

                # 源文件格式应为：id,origin_lat,origin_lng,destination_lat,destination_lng
                print('path {0[0]}: From {0[1]},{0[2]} to {0[3]},{0[4]} crawled...'.format(
                    od_data))

                url = 'http://restapi.amap.com/v3/direction/driving?key={key}&strategy={strategy}&origin={0[2]},{0[1]}&destination={0[4]},{0[3]}'.format(
                    od_data, **self._crawl_parameter)

                timeout = 2
                while(timeout > 0):
                    timeout -= 1
                    try:
                        if(self._path_queue.full()):
                            time.sleep(8)

                        path_info = {}
                        path_info['od_id'] = od_data[0]
                        path_info['origin_lat'] = od_data[1]
                        path_info['origin_lng'] = od_data[2]
                        path_info['destination_lat'] = od_data[3]
                        path_info['destination_lng'] = od_data[4]
                        path_info['path_json'] = requests.get(
                            url, timeout=5).json()
                        self._path_queue.put(path_info)
                        print('path {0[0]}: From {0[1]},{0[2]} to {0[3]},{0[4]} crawl succeed.'.format(
                            od_data))
                        break
                    except Exception as crawl_error:
                        if(timeout == 0):
                            print(crawl_error)
                            self._od_queue.put(od_data)
