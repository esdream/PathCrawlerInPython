"""
The Collection of Parser

Provide different parser of corresponding crawler.
"""

import threading
import sqlite3

PARSER_EXIT_FLAG = False

# 百度-驾车模式解析线程
class BaiduDrivingParserThread(threading.Thread):
    """Driving path data parser thread.

    Parse the driving path data.
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
        print('No.{} parser start...'.format(self._thread_id))
        self.__data_parser()
        print('No.{} parser finished!'.format(self._thread_id))

    # 解析路径、时间、距离
    def __data_parser(self):
        """Parser of driving json data.

        Parse the driving data.
        """

        while not PARSER_EXIT_FLAG:

            try:
                path_info = self._path_queue.get(True, 20)

                timeout = 2
                while(timeout > 0):
                    timeout -= 1

                    result = {}
                    result['id'] = path_info['city_com_num']
                    result['origin_lat'] = path_info['origin_lat']
                    result['origin_lng'] = path_info['origin_lng']
                    result['destination_lat'] = path_info['destination_lat']
                    result['destination_lng'] = path_info['destination_lng']
                    result['origin'] = path_info['origin']
                    result['destination'] = path_info['destination']
                    result['origin_region'] = path_info['origin_region']
                    result['destination_region'] = path_info['destination_region']
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

                    print('From {origin}#{origin_lat},{origin_lng}(region: {origin_region}) to {destination}#{destination_lat},{destination_lng}(region: {destination_region}) parse succeed: duration: {driving_duration}, distance: {distance_km}'.format(
                        **result))

                    result_vector = (result['id'], result['origin_lat'], result['origin_lng'], result['destination_lat'], result['destination_lng'], result['origin'], result['destination'], result['origin_region'], result['destination_region'], result['driving_duration'], result['distance_km'], result['path'])
                    self._data_batch.append(result_vector)

                    if(len(self._data_batch) == 50):
                        path_data_db = sqlite3.connect(self._db_name)
                        with path_data_db:
                            cursor = path_data_db.cursor()
                            cursor.executemany(
                                'insert into path_data values (?,?,?,?,?,?,?,?,?,?,?,?)', self._data_batch)
                            path_data_db.commit()
                            self._data_batch[:] = []

                    self._path_queue.task_done()
                    break

            except Exception as parser_error:
                with self._error_lock:
                    self._error_file.write(
                        '{city_com_num},{origin_lat},{origin_lng},{destination_lat},{destination_lng},{origin},{destination},{origin_region},{destination_region}\n'.format(**path_info))
                    print('Parse path {} failed!'.format(
                        path_info['city_com_num']))
                    print(parser_error)



# 百度-公交模式解析线程
class BaiduTransitParserThread(threading.Thread):
    """Transit path data parse thread.

    Parse the transit path data from Baidu Map.
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
        print('No.{} parser start...'.format(self._thread_id))
        self.__data_parser()
        print('No.{} parser finished!'.format(self._thread_id))

    def __data_parser(self):
        """Parser of transit json data.

        Parse the transit data.
        """
        while not PARSER_EXIT_FLAG:

            try:
                path_info = self._path_queue.get(True, 20)

                timeout = 2
                while(timeout > 0):
                    timeout -= 1

                    result = {}
                    result['id'] = path_info['od_id']
                    result['origin_lat'] = path_info['origin_lat']
                    result['origin_lng'] = path_info['origin_lng']
                    result['destination_lat'] = path_info['destination_lng']
                    result['destination_lng'] = path_info['destination_lng']
                    result['origin_city'] = path_info['path_json'][u'result'][u'origin'][u'city_name']
                    result['destination_city'] = path_info['path_json'][u'result'][u'destination'][u'city_name']
                    
                    result['distance_km'] = path_info['path_json'][u'result'][u'routes'][0][u'distance'] / 1000
                    result['duration_s'] = path_info['path_json'][u'result'][u'routes'][0][u'duration']
                    result['price_yuan'] = path_info['path_json'][u'result'][u'routes'][0][u'price']

                    # 解析路径
                    path_string = ''

                    num_of_steps = len(
                        path_info['path_json'][u'result'][u'routes'][0][u'steps'])
                    for i in range(num_of_steps):
                        num_of_substeps = len(
                            path_info['path_json'][u'result'][u'routes'][0][u'steps'][i])
                        for j in range(num_of_substeps):
                            if(i == num_of_steps - 1 and j == num_of_substeps - 1):
                                path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][j][u'path']
                                continue
                            elif(i != num_of_steps - 1 and j == num_of_substeps - 1):
                                path_string = path_string + path_info['path_json'][
                                    u'result'][u'routes'][0][u'steps'][i][j][u'path'] + ';'
                                continue
                            path_list = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][j][u'path'].split(
                                ';')
                            path_string += ";".join(path_list[0:-1]) + ';'
                    result['path'] = path_string

                    print('From {origin_lat},{origin_lng}(region: {origin_city}) to {destination_lat},{destination_lng}(region: {destination_city}) parse succeed: duration: {duration_s}, distance: {distance_km}'.format(**result))

                    result_vector = (result['id'], result['origin_lat'], result['origin_lng'], result['destination_lat'], result['destination_lng'], result['origin_city'], result['destination_city'], result['duration_s'], result['distance_km'], result['price_yuan'], result['path'])
                    self._data_batch.append(result_vector)

                    if(len(self._data_batch) == 50):
                        path_data_db = sqlite3.connect(self._db_name)
                        with path_data_db:
                            cursor = path_data_db.cursor()
                            cursor.executemany(
                                'insert into path_data values (?,?,?,?,?,?,?,?,?,?,?)', self._data_batch)
                            path_data_db.commit()
                            self._data_batch[:] = []

                    self._path_queue.task_done()
                    break

            except Exception as parser_error:
                with self._error_lock:
                    self._error_file.write(
                        '{od_id},{origin_lat},{origin_lng},{destination_lat},{destination_lng}\n'.format(**path_info))
                    print('Parse path {} failed!'.format(
                        path_info['od_id']))
                    print(parser_error)


# 百度-步行模式解析线程
class BaiduWalkingParserThread(threading.Thread):
    """Walking path data parse thread.

    Parse the walking path data from Baidu Map.
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
        print('No.{} parser start...'.format(self._thread_id))
        self.__data_parser()
        print('No.{} parser finished!'.format(self._thread_id))

    def __data_parser(self):
        """Parser of walking json data.

        Parse the walking data.
        """
        while not PARSER_EXIT_FLAG:

            try:
                path_info = self._path_queue.get(True, 20)

                timeout = 2
                while(timeout > 0):
                    timeout -= 1

                    result = {}
                    result['id'] = path_info['od_id']
                    result['origin_lat'] = path_info['origin_lat']
                    result['origin_lng'] = path_info['origin_lng']
                    result['destination_lat'] = path_info['destination_lng']
                    result['destination_lng'] = path_info['destination_lng']
                    result['region'] = path_info['region']

                    result['distance_km'] = path_info['path_json'][u'result'][u'routes'][0][u'distance'] / 1000
                    result['duration_s'] = path_info['path_json'][u'result'][u'routes'][0][u'duration']

                    # 解析路径
                    path_string = ''
                    num_of_steps = len(
                        path_info['path_json'][u'result'][u'routes'][0][u'steps'])

                    for i in range(num_of_steps):
                        if(i == num_of_steps - 1):
                            path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path']
                            break
                        path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path'] + ';'
                    result['path'] = path_string

                    print('From {origin_lat},{origin_lng} to {destination_lat},{destination_lng} (region: {region}) parse succeed: duration: {duration_s}, distance: {distance_km}'.format(**result))

                    result_vector = (result['id'], result['origin_lat'], result['origin_lng'], result['destination_lat'], result['destination_lng'], result['region'], result['duration_s'], result['distance_km'], result['path'])
                    self._data_batch.append(result_vector)

                    if(len(self._data_batch) == 50):
                        path_data_db = sqlite3.connect(self._db_name)
                        with path_data_db:
                            cursor = path_data_db.cursor()
                            cursor.executemany(
                                'insert into path_data values (?,?,?,?,?,?,?,?,?)', self._data_batch)
                            path_data_db.commit()
                            self._data_batch[:] = []

                    self._path_queue.task_done()
                    break

            except Exception as parser_error:
                with self._error_lock:
                    self._error_file.write(
                        '{od_id},{origin_lat},{origin_lng},{destination_lat},{destination_lng},{region}\n'.format(**path_info))
                    print('Parse path {} failed!'.format(
                        path_info['od_id']))
                    print(parser_error)


# 百度-骑行模式解析线程
class BaiduRidingParserThread(threading.Thread):
    """Riding path data parse thread.

    Parse the riding path data from Baidu Map.
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
        print('No.{} parser start...'.format(self._thread_id))
        self.__data_parser()
        print('No.{} parser finished!'.format(self._thread_id))

    def __data_parser(self):
        """Parser of riding json data.

        Parse the riding data.
        """
        while not PARSER_EXIT_FLAG:

            try:
                path_info = self._path_queue.get(True, 20)

                timeout = 2
                while(timeout > 0):
                    timeout -= 1

                    result = {}
                    result['id'] = path_info['od_id']
                    result['origin_lat'] = path_info['origin_lat']
                    result['origin_lng'] = path_info['origin_lng']
                    result['destination_lat'] = path_info['destination_lng']
                    result['destination_lng'] = path_info['destination_lng']
                    result['origin_region'] = path_info['origin_region']
                    result['destination_region'] = path_info['destination_region']

                    result['distance_km'] = path_info['path_json'][u'result'][u'routes'][0][u'distance'] / 1000
                    result['duration_s'] = path_info['path_json'][u'result'][u'routes'][0][u'duration']

                    # 解析路径
                    path_string = ''
                    num_of_steps = len(
                        path_info['path_json'][u'result'][u'routes'][0][u'steps'])

                    for i in range(num_of_steps):
                        if(i == num_of_steps - 1):
                            path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path']
                            break
                        path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path'] + ';'
                    result['path'] = path_string

                    print('From {origin_lat},{origin_lng}({origin_region}) to {destination_lat},{destination_lng} ({destination_region}) parse succeed: duration: {duration_s}, distance: {distance_km}'.format(**result))

                    result_vector = (result['id'], result['origin_lat'], result['origin_lng'], result['destination_lat'],
                                     result['destination_lng'], result['origin_region'],result['destination_region'], result['duration_s'], result['distance_km'], result['path'])
                    self._data_batch.append(result_vector)

                    if(len(self._data_batch) == 50):
                        path_data_db = sqlite3.connect(self._db_name)
                        with path_data_db:
                            cursor = path_data_db.cursor()
                            cursor.executemany(
                                'insert into path_data values (?,?,?,?,?,?,?,?,?,?)', self._data_batch)
                            path_data_db.commit()
                            self._data_batch[:] = []

                    self._path_queue.task_done()
                    break

            except Exception as parser_error:
                with self._error_lock:
                    self._error_file.write(
                        '{od_id},{origin_lat},{origin_lng},{destination_lat},{destination_lng},{origin_region},{destination_region}\n'.format(**path_info))
                    print('Parse path {} failed!'.format(
                        path_info['od_id']))
                    print(parser_error)


# 高德-驾车模式解析线程
class AMapDrivingParserThread(threading.Thread):
    """AMap driving path data parser thread.

    Parse the driving path data from AMap.
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
        print('No.{} parser start...'.format(self._thread_id))
        self.__data_parser()
        print('No.{} parser finished!'.format(self._thread_id))

    # 解析路径、时间、距离
    def __data_parser(self):
        """Parser of driving json data.

        Parse the driving data.
        """

        while not PARSER_EXIT_FLAG:

            try:
                path_info = self._path_queue.get(True, 20)

                timeout = 2
                while(timeout > 0):
                    timeout -= 1

                    result = {}
                    result['id'] = path_info['od_id']
                    result['origin_lat'] = path_info['origin_lat']
                    result['origin_lng'] = path_info['origin_lng']
                    result['destination_lat'] = path_info['destination_lat']
                    result['destination_lng'] = path_info['destination_lng']
                    result['duration_s'] = path_info['path_json'][u'route'][u'paths'][0][u'duration']
                    result['distance_km'] = int(path_info['path_json'][u'route'][u'paths'][0][u'distance']) / 1000
                    result['taxi_cost'] = path_info['path_json'][u'route'][u'taxi_cost']

                    # 解析路径
                    num_of_steps = len(
                        path_info['path_json'][u'route'][u'paths'][0][u'steps'])
                    path_string = ''
                    for i in range(num_of_steps):
                        if(i == num_of_steps - 1):
                            path_string += path_info['path_json'][u'route'][u'paths'][0][u'steps'][i][u'polyline']
                            break
                        path_list = path_info['path_json'][u'route'][u'paths'][0][u'steps'][i][u'polyline'].split(
                            ';')
                        path_string += ";".join(path_list[0:-1]) + ';'
                    result['path'] = path_string

                    print('From {origin_lat},{origin_lng} to {destination_lat},{destination_lng} parse succeed: duration: {duration_s}, distance: {distance_km}'.format(**result))

                    result_vector = (result['id'], result['origin_lat'], result['origin_lng'], result['destination_lat'], result['destination_lng'], result['duration_s'], result['distance_km'], result['taxi_cost'], result['path'])

                    self._data_batch.append(result_vector)

                    if(len(self._data_batch) == 50):
                        path_data_db = sqlite3.connect(self._db_name)
                        with path_data_db:
                            cursor = path_data_db.cursor()
                            cursor.executemany(
                                'insert into path_data values (?,?,?,?,?,?,?,?,?)', self._data_batch)
                            path_data_db.commit()
                            self._data_batch[:] = []

                    self._path_queue.task_done()
                    break

            except Exception as parser_error:
                with self._error_lock:
                    self._error_file.write(
                        '{od_id},{origin_lat},{origin_lng},{destination_lat},{destination_lng}\n'.format(**path_info))
                    print('Parse path {} failed!'.format(
                        path_info['od_id']))
                    print(parser_error)
