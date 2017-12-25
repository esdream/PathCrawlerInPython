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
        self._subpath_batch = parser_args['subpath_batch']

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
                    result['id'] = path_info['route_id']
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
                    # path_string = ''
                    # for i in range(num_of_steps):
                    #     if(i == num_of_steps - 1):
                    #         path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path']
                    #         break
                    #     path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path'] + ';'
                    # result['path'] = path_string

                    for i in range(num_of_steps):
                        subresult = {}

                        subresult['route_id'] = result['id']
                        subresult['step_num'] = i + 1
                        subresult['start_lat'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'stepOriginLocation'][u'lat']
                        subresult['start_lng'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'stepOriginLocation'][u'lng']
                        subresult['end_lat'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'stepDestinationLocation'][u'lat']
                        subresult['end_lng'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'stepDestinationLocation'][u'lng']
                        subresult['sub_s'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'duration']
                        subresult['sub_km'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'distance'] / 1000
                        subresult['area'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'area']
                        subresult['traffic_status'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'traffic_condition_detail'][0][u'status']
                        subresult['geo_cnt'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'traffic_condition_detail'][0][u'geo_cnt']
                        subresult['path'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path']

                        subpath_vector = (
                            subresult['route_id'], subresult['step_num'], subresult['start_lat'], subresult['start_lng'], subresult['end_lat'], subresult['end_lng'], subresult['sub_s'], subresult['sub_km'], subresult['area'],  subresult['traffic_status'], subresult['geo_cnt'], subresult['path'])
                        self._subpath_batch.append(subpath_vector)


                    print('From {origin}#{origin_lat},{origin_lng}(region: {origin_region}) to {destination}#{destination_lat},{destination_lng}(region: {destination_region}) parse succeed: duration: {driving_duration}, distance: {distance_km}'.format(
                        **result))

                    result_vector = (result['id'], result['origin_lat'], result['origin_lng'], result['destination_lat'], result['destination_lng'], result['origin'], result['destination'], result['origin_region'], result['destination_region'], result['driving_duration'], result['distance_km'])
                    self._data_batch.append(result_vector)

                    if(len(self._data_batch) == 50):
                        path_data_db = sqlite3.connect(self._db_name)
                        # subpath_db = sqlite3.connect(self._subpathdb_name)

                        with path_data_db:

                            path_data_db.executemany(
                                'insert into path_data values (?,?,?,?,?,?,?,?,?,?,?)', self._data_batch)
                            path_data_db.commit()
                            self._data_batch[:] = []

                            path_data_db.executemany(
                                'insert into subpath values (?,?,?,?,?,?,?,?,?,?,?,?)', self._subpath_batch)
                            path_data_db.commit()
                            self._subpath_batch[:] = []

                    self._path_queue.task_done()
                    break

            except Exception as parser_error:
                with self._error_lock:
                    self._error_file.write(
                        '{route_id},{origin_lat},{origin_lng},{destination_lat},{destination_lng},{origin},{destination},{origin_region},{destination_region}\n'.format(**path_info))
                    print('Parse path {} failed!'.format(
                        path_info['route_id']))
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
        self._subpath_batch = parser_args['subpath_batch']

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
                    result['destination_lat'] = path_info['destination_lat']
                    result['destination_lng'] = path_info['destination_lng']
                    result['origin_city'] = path_info['origin_city']
                    result['destination_city'] = path_info['destination_city']
                    # result['origin_city'] = path_info[['path_json'][u'result'][u'origin'][u'city_name']]
                    # result['destination_city'] = path_info['path_json'][u'result'][u'destination'][u'city_name']
                    
                    result['distance_km'] = path_info['path_json'][u'result'][u'routes'][0][u'distance'] / 1000
                    result['duration_s'] = path_info['path_json'][u'result'][u'routes'][0][u'duration']
                    result['price_yuan'] = path_info['path_json'][u'result'][u'routes'][0][u'price']

                    # 解析子路径

                    num_of_steps = len(path_info['path_json'][u'result'][u'routes'][0][u'steps'])

                    for i in range(num_of_steps):

                        subpath_result = {}
                        subpath_result['route_id'] = result['id']
                        subpath_result['step_num'] = i + 1
                        subpath_result['start_lat'] = path_info['path_json'][u'result'][
                            u'routes'][0][u'steps'][i][0][u'start_location'][u'lat']
                        subpath_result['start_lng'] = path_info['path_json'][u'result'][
                            u'routes'][0][u'steps'][i][0][u'start_location'][u'lng']
                        subpath_result['end_lat'] = path_info['path_json'][u'result'][
                            u'routes'][0][u'steps'][i][0][u'end_location'][u'lat']
                        subpath_result['end_lng'] = path_info['path_json'][u'result'][
                            u'routes'][0][u'steps'][i][0][u'end_location'][u'lng']
                        subpath_result['sub_s'] = path_info['path_json'][u'result'][
                            u'routes'][0][u'steps'][i][0][u'duration']
                        subpath_result['sub_km'] = path_info['path_json'][u'result'][
                            u'routes'][0][u'steps'][i][0][u'distance'] / 1000
                        subpath_result['vehicle_info'] = path_info['path_json'][u'result'][
                            u'routes'][0][u'steps'][i][0][u'vehicle_info'][u'type']
                        # 路线规划v2.0中，只有大陆地区的同城请求，本step中为公交时traffic_condition字段才有意义，其他情况返回空数组，若为空数组
                        if(path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][0][u'traffic_condition']):
                            subpath_result['traffic_cond'] = path_info['path_json'][u'result'][
                                u'routes'][0][u'steps'][i][0][u'traffic_condition'][u'status']
                        else:
                            subpath_result['traffic_cond'] = 0

                        subpath_result['path'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][0][u'path']

                        subpath_vector = (
                            subpath_result['route_id'], subpath_result['step_num'], subpath_result['start_lat'], subpath_result['start_lng'], subpath_result['end_lat'], subpath_result['end_lng'], subpath_result['sub_s'], subpath_result['sub_km'], subpath_result['vehicle_info'],  subpath_result['traffic_cond'], subpath_result['path'])
                        self._subpath_batch.append(subpath_vector)

                    print('From {origin_lat},{origin_lng}(region: {origin_city}) to {destination_lat},{destination_lng}(region: {destination_city}) parse succeed: duration: {duration_s}, distance: {distance_km}'.format(**result))

                    result_vector = (result['id'], result['origin_lat'], result['origin_lng'], result['destination_lat'], result['destination_lng'], result['origin_city'], result['destination_city'], result['duration_s'], result['distance_km'], result['price_yuan'])
                    self._data_batch.append(result_vector)

                    if(len(self._data_batch) == 50):
                        path_data_db = sqlite3.connect(self._db_name)

                        with path_data_db:
                            path_data_db.executemany(
                                'insert into path_data values (?,?,?,?,?,?,?,?,?,?)', self._data_batch)
                            path_data_db.commit()
                            self._data_batch[:] = []

                            path_data_db.executemany(
                                'insert into subpath values (?,?,?,?,?,?,?,?,?,?,?)', self._subpath_batch)
                            path_data_db.commit()
                            self._subpath_batch[:] = []

                    self._path_queue.task_done()
                    break

            except Exception as parser_error:
                with self._error_lock:
                    self._error_file.write(
                        '{od_id},{origin_lat},{origin_lng},{destination_lat},{destination_lng},{origin_city},{destination_city}\n'.format(**path_info))
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
                    result['distance_km'] = float(path_info['path_json'][u'route'][u'paths'][0][u'distance']) / 1000
                    result['taxi_cost'] = path_info['path_json'][u'route'][u'taxi_cost']

                    # 解析路径
                    num_of_steps = len(
                        path_info['path_json'][u'route'][u'paths'][0][u'steps'])
                    path_string = ''
                    for i in range(num_of_steps):
                        if(i == num_of_steps - 1):
                            path_string += path_info['path_json'][u'route'][u'paths'][0][u'steps'][i][u'polyline']
                            break
                        path_string += path_info['path_json'][u'route'][u'paths'][0][u'steps'][i][u'polyline'] + ';'
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


# 高德-公交模式解析线程
class AMapTransitParserThread(threading.Thread):
    """AMap transit path data parser thread.

    Parse the transit path data from AMap.
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
                    result['destination_lat'] = path_info['destination_lat']
                    result['destination_lng'] = path_info['destination_lng']
                    result['origin_city'] = path_info['origin_city']
                    result['des_city'] = path_info['des_city']
                    result['duration_s'] = float(
                        path_info['path_json'][u'route'][u'transits'][0][u'duration'])
                    result['distance_km'] = float(
                        path_info['path_json'][u'route'][u'transits'][0][u'distance']) / 1000
                    result['transit_cost'] = float(
                        path_info['path_json'][u'route'][u'transits'][0][u'cost'])
                    result['taxi_cost'] = float(
                        path_info['path_json'][u'route'][u'taxi_cost'])

                    # # 解析路径
                    # num_of_steps = len(
                    #     path_info['path_json'][u'route'][u'paths'][0][u'steps'])
                    # path_string = ''
                    # for i in range(num_of_steps):
                    #     if(i == num_of_steps - 1):
                    #         path_string += path_info['path_json'][u'route'][u'paths'][0][u'steps'][i][u'polyline']
                    #         break
                    #     path_string += path_info['path_json'][u'route'][u'paths'][0][u'steps'][i][u'polyline'] + ';'
                    # result['path'] = path_string

                    print('From {origin_lat},{origin_lng} to {destination_lat},{destination_lng} parse succeed: duration: {duration_s}, distance: {distance_km}, transit_cost: {transit_cost}'.format(
                        **result))

                    result_vector = (result['id'], result['origin_lat'], result['origin_lng'], result['destination_lat'],
                                     result['destination_lng'], result['origin_city'], result['des_city'], result['duration_s'], result['distance_km'], result['transit_cost'], result['taxi_cost'])

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
                        '{od_id},{origin_lat},{origin_lng},{destination_lat},{destination_lng},{origin_city},{des_city}\n'.format(**path_info))
                    print('Parse path {} failed!'.format(
                        path_info['od_id']))
                    print(parser_error)


# 百度-公交模式1.0解析线程
class BaiduTransitFirstVersionParserThread(threading.Thread):
    """Transit path data parser thread.

    Parse the transit path data.
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
                    result['id'] = path_info['route_id']
                    result['origin_lat'] = path_info['origin_lat']
                    result['origin_lng'] = path_info['origin_lng']
                    result['destination_lat'] = path_info['destination_lat']
                    result['destination_lng'] = path_info['destination_lng']
                    result['origin'] = path_info['origin']
                    result['destination'] = path_info['destination']
                    result['origin_region'] = path_info['origin_region']
                    result['destination_region'] = path_info['destination_region']
                    result['duration'] = path_info['path_json'][u'result'][u'routes'][0][u'scheme'][0][u'duration']
                    result['distance_km'] = path_info['path_json'][u'result'][u'routes'][0][u'scheme'][0][u'distance'] / 1000
                    result['price'] = path_info['path_json'][u'result'][u'routes'][0][u'scheme'][0][u'price']

                    # # 解析路径
                    # num_of_steps = len(
                    #     path_info['path_json'][u'result'][u'routes'][0][u'scheme'][0][u'steps'])
                    # # path_string = ''
                    # # for i in range(num_of_steps):
                    # #     if(i == num_of_steps - 1):
                    # #         path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path']
                    # #         break
                    # #     path_string += path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path'] + ';'
                    # # result['path'] = path_string

                    # for i in range(num_of_steps):
                    #     subresult = {}

                    #     subresult['route_id'] = result['id']
                    #     subresult['step_num'] = i + 1
                    #     subresult['start_lat'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'stepOriginLocation'][u'lat']
                    #     subresult['start_lng'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'stepOriginLocation'][u'lng']
                    #     subresult['end_lat'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'stepDestinationLocation'][u'lat']
                    #     subresult['end_lng'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'stepDestinationLocation'][u'lng']
                    #     subresult['sub_s'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'duration']
                    #     subresult['sub_km'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'distance'] / 1000
                    #     subresult['area'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'area']
                    #     subresult['traffic_status'] = path_info['path_json'][u'result'][
                    #         u'routes'][0][u'steps'][i][u'traffic_condition_detail'][0][u'status']
                    #     subresult['geo_cnt'] = path_info['path_json'][u'result'][u'routes'][
                    #         0][u'steps'][i][u'traffic_condition_detail'][0][u'geo_cnt']
                    #     subresult['path'] = path_info['path_json'][u'result'][u'routes'][0][u'steps'][i][u'path']

                    #     subpath_vector = (
                    #         subresult['route_id'], subresult['step_num'], subresult['start_lat'], subresult['start_lng'], subresult['end_lat'], subresult['end_lng'], subresult['sub_s'], subresult['sub_km'], subresult['area'],  subresult['traffic_status'], subresult['geo_cnt'], subresult['path'])
                    #     self._subpath_batch.append(subpath_vector)

                    print('From {origin}#{origin_lat},{origin_lng}(region: {origin_region}) to {destination}#{destination_lat},{destination_lng}(region: {destination_region}) parse succeed: duration: {duration}, distance: {distance_km}, price:{price}'.format(
                        **result))

                    result_vector = (result['id'], result['origin_lat'], result['origin_lng'], result['destination_lat'], result['destination_lng'], result['origin'],
                                     result['destination'], result['origin_region'], result['destination_region'], result['duration'], result['distance_km'], result['price'])
                    self._data_batch.append(result_vector)

                    if(len(self._data_batch) == 50):
                        path_data_db = sqlite3.connect(self._db_name)
                        # subpath_db = sqlite3.connect(self._subpathdb_name)

                        with path_data_db:

                            path_data_db.executemany(
                                'insert into path_data values (?,?,?,?,?,?,?,?,?,?,?,?)', self._data_batch)
                            path_data_db.commit()
                            self._data_batch[:] = []

                            # path_data_db.executemany(
                            #     'insert into subpath values (?,?,?,?,?,?,?,?,?,?,?,?)', self._subpath_batch)
                            # path_data_db.commit()
                            # self._subpath_batch[:] = []

                    self._path_queue.task_done()
                    break

            except Exception as parser_error:
                with self._error_lock:
                    self._error_file.write(
                        '{route_id},{origin_lat},{origin_lng},{destination_lat},{destination_lng},{origin},{destination},{origin_region},{destination_region}\n'.format(**path_info))
                    print('Parse path {} failed!'.format(
                        path_info['route_id']))
                    print(parser_error)
