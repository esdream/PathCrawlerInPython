"""
The Collection of Crawler

Provide many way to crawl different data.
"""

import time
import threading
import requests

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
                print('path {0[0]}: From {0[1]} to {0[2]} crawled...'.format(
                    city_coms_data))
                url = 'http://api.map.baidu.com/direction/v1?mode=driving&origin={0[1]}&destination={0[2]}&origin_region={0[1]}&destination_region={0[2]}&output=json&ak=KtmshjiGv5nDDvYwcWbF0GIfhZf1anvE'.format(
                    city_coms_data)

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
                        path_info['path_json'] = requests.get(
                            url, timeout=5).json()
                        self._path_queue.put(path_info)
                        print('path {0[0]}: From {0[1]} to {0[2]} crawl succeed.'.format(
                            city_coms_data))
                        break
                    except Exception as crawl_error:
                        if(timeout == 0):
                            with self._error_lock:
                                self._error_file.write('{0},{1},{2}\n'.format(
                                    city_coms_data[0], city_coms_data[1], city_coms_data[2]))
                                print('Crawl path {0[0]} failed!'.format(
                                    city_coms_data))
                                print(crawl_error)
