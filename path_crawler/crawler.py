"""
The Collection of Crawler

Provide many way to crawl different data.
"""

import time
import threading
import random
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
        # 源文件格式应为：id,origin_lat,origin_lng,destination_lat,destination_lng,origin,destination,origin_region,destination_region

        while True:
            if(self._od_queue.empty()):
                break
            else:
                # 选择使用地点名称抓取
                if(str(self._crawl_parameter['coord_or_name']) == '1'):
                    city_coms_data = self._od_queue.get()
                    print('path {0[0]}: From {0[5]}(region {0[7]}) to {0[6]}(region {0[8]}) crawled...'.format(
                        city_coms_data))
                    url = 'http://api.map.baidu.com/direction/v1?mode=driving&origin={0[5]}&destination={0[6]}&origin_region={0[7]}&destination_region={0[8]}&tactics={tactics}&output=json&ak={key}'.format(
                        city_coms_data, **self._crawl_parameter)

                    timeout = 2
                    while(timeout > 0):
                        timeout -= 1
                        try:
                            if(self._path_queue.full()):
                                time.sleep(8)

                            path_info = {}
                            path_info['route_id'] = city_coms_data[0]
                            path_info['origin_lat'] =''
                            path_info['origin_lng'] = ''
                            path_info['destination_lat'] = ''
                            path_info['destination_lng'] = ''
                            path_info['origin'] = city_coms_data[5]
                            path_info['destination'] = city_coms_data[6]
                            path_info['origin_region'] = city_coms_data[7]
                            path_info['destination_region'] = city_coms_data[8]
                            path_info['path_json'] = requests.get(url, timeout=5).json()
                            self._path_queue.put(path_info)
                            print('path {0[0]}: From {0[5]}(region {0[7]}) to {0[6]}(region {0[8]}) crawl succeed.'.format(
                                city_coms_data))
                            break
                        except Exception as crawl_error:
                            if(timeout == 0):
                                print(crawl_error)
                                self._od_queue.put(city_coms_data)
                
                # 选择使用经纬度抓取
                elif(str(self._crawl_parameter['coord_or_name']) == '2'):
                    city_coms_data = self._od_queue.get()
                    print('path {0[0]}: From {0[1]}{0[2]}(region {0[7]}) to {0[3]}{0[4]}(region {0[8]}) crawled...'.format(
                        city_coms_data))
                    url = 'http://api.map.baidu.com/direction/v1?mode=driving&origin={0[1]},{0[2]}&destination={0[3]},{0[4]}&origin_region={0[7]}&destination_region={0[8]}&tactics={tactics}&output=json&ak={key}'.format(
                        city_coms_data, **self._crawl_parameter)

                    timeout = 2
                    while(timeout > 0):
                        timeout -= 1
                        try:
                            if(self._path_queue.full()):
                                time.sleep(8)

                            path_info = {}
                            path_info['route_id'] = city_coms_data[0]
                            path_info['origin_lat'] = city_coms_data[1]
                            path_info['origin_lng'] = city_coms_data[2]
                            path_info['destination_lat'] = city_coms_data[3]
                            path_info['destination_lng'] = city_coms_data[4]
                            path_info['origin'] = city_coms_data[5]
                            path_info['destination'] = city_coms_data[6]
                            path_info['origin_region'] = city_coms_data[7]
                            path_info['destination_region'] = city_coms_data[8]
                            path_info['path_json'] = requests.get(
                                url, timeout=5).json()
                            self._path_queue.put(path_info)
                            print('path {0[0]}: From {0[1]},{0[2]}(region {0[7]}) to {0[3]},{0[4]}(region {0[8]}) crawl succeed.'.format(
                                city_coms_data))
                            break
                        except Exception as crawl_error:
                            if(timeout == 0):
                                print(crawl_error)
                                self._od_queue.put(city_coms_data)


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

        Send requests to Baidu Map and get the transit mode path data.
        """
        
        while True:
            if(self._od_queue.empty()):
                break
            else:
                od_data = self._od_queue.get()
                # 源文件格式应为：id,origin_lat,origin_lng,destination_lat,destination_lng,origin_city,des_city
                print('path {0[0]}: From {0[1]},{0[2]} to {0[3]},{0[4]} crawled...'.format(
                    od_data))

                url = 'http://api.map.baidu.com/direction/v2/transit?origin={0[1]},{0[2]}&destination={0[3]},{0[4]}&coord_type={coord_type}&tactics_incity={tactics_incity}&tactics_intercity={tactics_intercity}&trans_type_intercity={trans_type_intercity}&ret_coordtype={ret_coordtype}&ak={key}'.format(
                    od_data, **self._crawl_parameter)

                timeout = 2
                while(timeout > 0):
                    timeout -= 1
                    try:
                        if(self._path_queue.full()):
                            time.sleep(8)

                        time.sleep(random.random())

                        path_info = {}
                        path_info['od_id'] = od_data[0]
                        path_info['origin_lat'] = od_data[1]
                        path_info['origin_lng'] = od_data[2]
                        path_info['destination_lat'] = od_data[3]
                        path_info['destination_lng'] = od_data[4]
                        path_info['origin_city'] = od_data[5]
                        path_info['destination_city'] = od_data[6]
                        path_info['path_json'] = requests.get(url, timeout=5).json()
                        self._path_queue.put(path_info)
                        print('path {0[0]}: From {0[1]},{0[2]}({0[5]}) to {0[3]},{0[4]}({0[6]}) crawl succeed.'.format(
                            od_data))
                        break
                    except Exception as crawl_error:
                        if(timeout == 0):
                            print(crawl_error)
                            self._od_queue.put(od_data)

# 百度-步行模式抓取线程
class BaiduWalkingCrawlerThread(threading.Thread):
    """Walking path crawler.

    Crawl the path using walking mode(route planning v1.0).
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
        self.__baidu_walking_path_crawler()
        print('No.{} crawler finished!'.format(self._thread_id))

    def __baidu_walking_path_crawler(self):
        """Crawler function.

        Send requests to Baidu Map and get the walking mode path data.
        """

        while True:
            if(self._od_queue.empty()):
                break
            else:
                od_data = self._od_queue.get()
                # 源文件格式应为：id,origin_lat,origin_lng,destination_lat,destination_lng,region
                print('path {0[0]}: From {0[1]},{0[2]} to {0[3]},{0[4]} (region: {0[5]}) crawled...'.format(
                    od_data))

                url = 'http://api.map.baidu.com/direction/v1?mode=walking&origin={0[1]},{0[2]}&destination={0[3]},{0[4]}&region={0[5]}&output=json&coord_type={coord_type}&ret_coordtype={ret_coordtype}&ak={key}'.format(
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
                        path_info['region'] = od_data[5]
                        path_info['path_json'] = requests.get(
                            url, timeout=5).json()
                        self._path_queue.put(path_info)
                        print('path {0[0]}: From {0[1]},{0[2]} to {0[3]},{0[4]} (region: {0[5]}) crawl succeed.'.format(
                            od_data))
                        break
                    except Exception as crawl_error:
                        if(timeout == 0):
                            print(crawl_error)
                            self._od_queue.put(od_data)


# 百度-骑行模式抓取线程
class BaiduRidingCrawlerThread(threading.Thread):
    """Riding path crawler.

    Crawl the path using riding mode(route planning v1.0).
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
        self.__baidu_riding_path_crawler()
        print('No.{} crawler finished!'.format(self._thread_id))

    def __baidu_riding_path_crawler(self):
        """Crawler function.

        Send requests to Baidu Map and get the riding mode path data.
        """

        while True:
            if(self._od_queue.empty()):
                break
            else:
                od_data = self._od_queue.get()
                # 源文件格式应为：id,origin_lat,origin_lng,destination_lat,destination_lng,origin_region,destination_region
                print('path {0[0]}: From {0[1]},{0[2]}(region: {0[5]}) to {0[3]},{0[4]} (region: {0[6]}) crawled...'.format(
                    od_data))

                url = 'http://api.map.baidu.com/direction/v1?mode=riding&origin={0[1]},{0[2]}&destination={0[3]},{0[4]}&origin_region={0[5]}&destination_region={0[6]}&output=json&coord_type={coord_type}&ret_coordtype={ret_coordtype}&ak={key}'.format(
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
                        path_info['origin_region'] = od_data[5]
                        path_info['destination_region'] = od_data[6]
                        path_info['path_json'] = requests.get(
                            url, timeout=5).json()
                        self._path_queue.put(path_info)
                        print('path {0[0]}: From {0[1]},{0[2]}(origin_region: {0[5]}) to {0[3]},{0[4]} (destination_region: {0[6]}) crawl succeed.'.format(
                            od_data))
                        break
                    except Exception as crawl_error:
                        if(timeout == 0):
                            print(crawl_error)
                            self._od_queue.put(od_data)


# 高德-驾车模式抓取线程
class AMapDrivingCrawlerThread(threading.Thread):
    """AMap driving path crawler.

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


# 高德-公交模式抓取线程
class AMapTransitCrawlerThread(threading.Thread):
    """AMap transit path crawler.

    Crawl the path using AMap Transit mode.
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
        self.__amap_transit_path_crawler()
        print('No.{} crawler finished!'.format(self._thread_id))

    def __amap_transit_path_crawler(self):
        """Crawler function.

        Send requests to AMap and get the transit mode path data.
        """

        while True:
            if(self._od_queue.empty()):
                break
            else:
                od_data = self._od_queue.get()

                # 源文件格式应为：id,origin_lat,origin_lng,destination_lat,destination_lng,origin_city,des_city
                print('path {0[0]}: From {0[1]},{0[2]}({0[5]}) to {0[3]},{0[4]}({0[6]}) crawled...'.format(
                    od_data))

                url = 'http://restapi.amap.com/v3/direction/transit/integrated?origin={0[2]},{0[1]}&destination={0[4]},{0[3]}&city={0[5]}&cityd={0[6]}&output=json&key={key}'.format(
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
                        path_info['origin_city'] = od_data[5]
                        path_info['des_city'] = od_data[6]
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


# 百度-公交模式1.0抓取线程
class BaiduTransitFirstVersionCrawlerThread(threading.Thread):
    """Transit path crawler.

    Crawl the path using transit mode(route planning v1.0).
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
        self.__baidu_transit_first_path_crawler()
        print('No.{} crawler finished!'.format(self._thread_id))

    def __baidu_transit_first_path_crawler(self):
        """Crawler function.

        Send requests to Baidu Map and get the transit mode path data.
        """
        # 源文件格式应为：id,origin_lat,origin_lng,destination_lat,destination_lng,origin,destination,origin_region,destination_region

        while True:
            if(self._od_queue.empty()):
                break
            else:
                # 选择使用地点名称抓取
                if(str(self._crawl_parameter['coord_or_name']) == '1'):
                    city_coms_data = self._od_queue.get()
                    print('path {0[0]}: From {0[5]}(region {0[7]}) to {0[6]}(region {0[8]}) crawled...'.format(
                        city_coms_data))
                    url = 'http://api.map.baidu.com/direction/v1?mode=transit&origin={0[5]}&destination={0[6]}&region={0[7]}&output=json&ak={key}'.format(
                        city_coms_data, **self._crawl_parameter)

                    timeout = 2
                    while(timeout > 0):
                        timeout -= 1
                        try:
                            if(self._path_queue.full()):
                                time.sleep(8)

                            path_info = {}
                            path_info['route_id'] = city_coms_data[0]
                            path_info['origin_lat'] = city_coms_data[1]
                            path_info['origin_lng'] = city_coms_data[2]
                            path_info['destination_lat'] = city_coms_data[3]
                            path_info['destination_lng'] = city_coms_data[4]
                            path_info['origin'] = city_coms_data[5]
                            path_info['destination'] = city_coms_data[6]
                            path_info['origin_region'] = city_coms_data[7]
                            path_info['destination_region'] = city_coms_data[8]
                            path_info['path_json'] = requests.get(
                                url, timeout=5).json()
                            self._path_queue.put(path_info)
                            print('path {0[0]}: From {0[5]}(region {0[7]}) to {0[6]}(region {0[8]}) crawl succeed.'.format(
                                city_coms_data))
                            break
                        except Exception as crawl_error:
                            if(timeout == 0):
                                print(crawl_error)
                                self._od_queue.put(city_coms_data)

                # 选择使用经纬度抓取
                elif(str(self._crawl_parameter['coord_or_name']) == '2'):
                    city_coms_data = self._od_queue.get()
                    print('path {0[0]}: From {0[1]},{0[2]}(region {0[7]}) to {0[3]},{0[4]}(region {0[8]}) crawled...'.format(
                        city_coms_data))
                    url = 'http://api.map.baidu.com/direction/v1?mode=transit&origin={0[1]},{0[2]}&destination={0[3]},{0[4]}&region={0[7]}&output=json&ak={key}'.format(
                        city_coms_data, **self._crawl_parameter)

                    timeout = 2
                    while(timeout > 0):
                        timeout -= 1
                        try:
                            if(self._path_queue.full()):
                                time.sleep(8)

                            path_info = {}
                            path_info['route_id'] = city_coms_data[0]
                            path_info['origin_lat'] = city_coms_data[1]
                            path_info['origin_lng'] = city_coms_data[2]
                            path_info['destination_lat'] = city_coms_data[3]
                            path_info['destination_lng'] = city_coms_data[4]
                            path_info['origin'] = city_coms_data[5]
                            path_info['destination'] = city_coms_data[6]
                            path_info['origin_region'] = city_coms_data[7]
                            path_info['destination_region'] = city_coms_data[8]
                            path_info['path_json'] = requests.get(
                                url, timeout=5).json()
                            self._path_queue.put(path_info)
                            print('path {0[0]}: From {0[1]}{0[2]}(region {0[7]}) to {0[3]}{0[4]}(region {0[8]}) crawl succeed.'.format(
                                city_coms_data))
                            break
                        except Exception as crawl_error:
                            if(timeout == 0):
                                print(crawl_error)
                                self._od_queue.put(city_coms_data)
