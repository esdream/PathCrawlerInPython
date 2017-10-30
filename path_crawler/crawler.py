"""
The Collection of Crawler

Provide many way to crawl different data.
"""

import time
import threading
import requests

# 驾车模式抓取线程
class DrivingCrawlerThread(threading.Thread):
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
        self.__driving_path_crawler()
        print('No.{} crawler finished!'.format(self._thread_id))

    def __driving_path_crawler(self):
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
                url = 'http://api.map.baidu.com/direction/v1?mode=driving&origin={0[1]}&destination={0[2]}&origin_region={0[3]}&destination_region={0[4]}&output=json&ak={1}'.format(
                    city_coms_data, self._crawl_parameter['ak'])

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
                        path_info['path_json'] = requests.get(
                            url, timeout=5).json()
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

# 公交模式抓取线程
class TransitCrawlerThread(threading.Thread):
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
        self.__transit_path_cralwer()
        print('No.{} crawler finished!'.format(self._thread_id))

    def __transit_path_cralwer(self):
        """Crawler function.

        Send requests to Baidu Map and get the driving mode path data.
        """
        
        while True:
            if(self._od_queue.empty()):
                break
            else:
                od_data = self._od_queue.get()
                print('path {0[0]}: From {0[1]} to {0[2]} crawled...'.format(
                    od_data))
                crawl_param_values = []
                for (key, values) in self._crawl_parameter.items():
                    crawl_param_values.append(values)
                url = 'http://api.map.baidu.com/direction/v2/transit?origin={0[1]}&destination={0[2]}&coord_type={1[0]}&tactics_incity={1[1]}&tactics_intercity={1[2]}&trans_type_intercity={1[3]}&ret_coordtype={1[4]}&ak={1[5]}'.format(
                    od_data, crawl_param_values)

                timeout = 2
                while(timeout > 0):
                    timeout -= 1
                    try:
                        if(self._path_queue.full()):
                            time.sleep(8)

                        path_info = {}
                        path_info['od_id'] = od_data[0]
                        path_info['origin_coord'] = od_data[1]
                        path_info['des_coord'] = od_data[2]
                        path_info['path_json'] = requests.get(url, timeout=5).json()
                        self._path_queue.put(path_info)
                        print('path {0[0]}: From {0[1]} to {0[2]} crawl succeed.'.format(od_data))
                        break
                    except Exception as crawl_error:
                        if(timeout == 0):
                            print(crawl_error)
                            self._od_queue.put(od_data)
