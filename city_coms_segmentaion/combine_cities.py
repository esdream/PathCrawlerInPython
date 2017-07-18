# _*_ coding: utf-8 _*_

"""Combine the cities.

Combine the primary cities and county-level cities. Save the combinations in database.
Args:
    city_filename: The file of cities.
    database: A database used to save combinations of city.
"""

import os
import json
import sqlite3


# 组合城市
class CombineCity(object):
    """Combine the cities.

    The class used to combine the cities.
    """

    def __init__(self, city_filename):
        self.__city_filename = city_filename

    def run(self):
        """Initiator of combination.

        Run the program of combination.
        """
        print('City combination start...')
        self.city_data_saver()
        print('City combination finished.')

    def city_data_saver(self):
        """Saver of city data.

        save the combinations in database.
        """

        self.init_database()
        prefecture_level_city_list, county_level_city_list = self.city_data_parser()

        db_name = '{}.db'.format(self.__city_filename)
        db_file = os.path.join(os.path.dirname(__file__), db_name)
        connection_of_citydb = sqlite3.connect(db_file)

        with connection_of_citydb:
            cur = connection_of_citydb.cursor()
            count_id = 1

            for pre_city in prefecture_level_city_list:
                for cou_city in county_level_city_list:
                    cur.execute('insert into city_combination values (?,?,?)', (count_id, pre_city, cou_city))
                    count_id += 1

            len_of_pre_city = len(prefecture_level_city_list)
            for i in range(len_of_pre_city):
                for j in range(i + 1, len_of_pre_city):
                    cur.execute('insert into city_combination values (?,?,?)', (count_id, prefecture_level_city_list[i], prefecture_level_city_list[j]))
                    count_id += 1

    def init_database(self):
        """Init the database.

        Init the database, using city filename as database name.
        """

        db_name = '{}.db'.format(self.__city_filename)
        db_file = os.path.join(os.path.dirname(__file__), db_name)
        if(os.path.isfile(db_file)):
            os.remove(db_file)
        conn_db = sqlite3.connect(db_file)
        cursor = conn_db.cursor()
        try:
            cursor.execute('create table city_combination(id int primary key, '
                           'origin_city varchar(20), destination_city varchar(20))')
            conn_db.commit()
        except IOError as db_e:
            print(db_e)
        finally:
            cursor.close()
            conn_db.close()

    def city_data_parser(self):
        """Parser of city data.

        Parse the data of cities, combine each primary city with each county-level city.
        """

        prefecture_level_city_list = []
        county_level_city_list = []
        city_file = './{}.json'.format(self.__city_filename)

        with open(city_file, 'r') as city_data_json:
            city_data = json.loads(city_data_json.read())

            for (provincial_level_region, city_list) in city_data.items():
                if(provincial_level_region in ['北京市', '上海市', '天津市']):
                    prefecture_level_city_list.append(provincial_level_region)
                    continue
                if(provincial_level_region in ['香港特别行政区', '澳门特别行政区', '台湾省']):
                    continue
                prefecture_level_city_list.extend(city_list['prefecture_level_city'])
                county_level_city_list.extend(city_list['county_level_city'])

        return (prefecture_level_city_list, county_level_city_list)


# 分表
class SegmentCityComs(object):
    """Segment the city combinations.

    Segment the city combinations into several tables.
    """

    def __init__(self, **divider_args):
        self.__city_coms_db = divider_args['city_coms_db']

    def run(self):
        """Initiator of segementation.

        Run the segementation of city combinations.
        """
        print('segmentation program start...')
        self.__segment_data()
        print('segementation program finished.')

    def __segment_data(self):
        """Segment function.

        Segment the city combinations from one database into several tables.
        """
        try:
            connection_of_comdb = sqlite3.connect(self.__city_coms_db)
        except IOError as no_db_error:
            print(no_db_error)
            print('The city coms database is not existed!')

        with connection_of_comdb:
            cursor_of_comdb = connection_of_comdb.cursor()
            coms_num = cursor_of_comdb.execute('select MAX(id) from city_combination').fetchall()

            city_coms_dir_path = os.path.join(os.path.dirname(__file__), 'city_coms')
            if not os.path.exists(city_coms_dir_path):
                os.mkdir(city_coms_dir_path)

            for coms_db_start_id in range(0, coms_num[0][0], 200000):
                coms_db_end_id = coms_db_start_id + 200000
                one_coms_db_data = cursor_of_comdb.execute('select * from city_combination where id >= ? and id < ?', (coms_db_start_id, coms_db_end_id)).fetchall()

                one_coms_db_name = 'city_coms_{0}_to_{1}.db'.format(coms_db_start_id, coms_db_end_id)
                connection_of_one_coms_db = sqlite3.connect(os.path.join(city_coms_dir_path, one_coms_db_name))

                with connection_of_one_coms_db:
                    cursor_of_one_comdb = connection_of_one_coms_db.cursor()
                    cursor_of_one_comdb.execute('create table city_combination(id int primary key, origin_city varchar(20), destination_city varchar(20))')
                    cursor_of_one_comdb.executemany('insert into city_combination values (?, ?, ?)', one_coms_db_data)
                    connection_of_one_coms_db.commit()

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
    main function
    """
    city_com = CombineCity('city_dataset')
    city_com.run()

    segmentation = SegmentCityComs(city_coms_db='city_dataset.db')
    segmentation.run()

if(__name__ == '__main__'):
    main()
