"""
The Collection of Parser

Provide different parser of corresponding crawler.
"""

import threading
import sqlite3

PARSER_EXIT_FLAG = False

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
        self._data_batch = parser_args['data_batch']

    def run(self):
        print('No.{} crawler start...'.format(self._thread_id))
        self.__data_parser()
        print('No.{} crawler finished!'.format(self._thread_id))

    # 解析路径、时间、距离
    def __data_parser(self):
        """Parser of json data.

        Parse the data from Baidu Map.
        """

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
                    num_of_steps = len(
                        path_info['path_json'][u'result'][u'routes'][0][u'steps'])
                    path_string = ''
                    for i in range(num_of_steps):
                        if(i == num_of_steps - 1):
                            path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path']
                            break
                        path_list = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path'].split(
                            ';')
                        path_string += ";".join(path_list[0:-1]) + ';'
                    result['path'] = path_string

                    print('From {} to {} parse succeed: duration: {}, distance: {}'.format(
                        result['origin'], result['destination'], result['driving_duration'], result['distance_km']))

                    result_vector = (result['id'], result['origin'], result['destination'],
                                     result['driving_duration'], result['distance_km'], result['path'])
                    self._data_batch.append(result_vector)

                    if(len(self._data_batch) == 50):
                        path_data_db = sqlite3.connect(self._db_name)
                        with path_data_db:
                            cursor = path_data_db.cursor()
                            cursor.executemany(
                                'insert into path_data values (?,?,?,?,?,?)', self._data_batch)
                            path_data_db.commit()
                            self._data_batch[:] = []

                    self._path_queue.task_done()
                    break

            except Exception as parser_error:
                with self._error_lock:
                    self._error_file.write('{0},{1},{2}\n'.format(
                        path_info['city_com_num'], path_info['origin'], path_info['destination']))
                    print('Parse path {} failed!'.format(
                        path_info['city_com_num']))
                    print(parser_error)
