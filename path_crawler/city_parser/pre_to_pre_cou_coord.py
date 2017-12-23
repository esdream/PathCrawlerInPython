# _*_ coding: utf-8 _*_

"""Combine the cities.

Combine the prefecture city to county city and prefecutre_city to each other in coords. Save the combinations in csv files.
Args:
    city_filename: The file of cities.
"""

import os
import csv
from path_crawler.conf import global_settings

# 组合城市
class CombineCity(object):
    """Combine the cities.

    The class used to combine the cities.
    """

    def __init__(self, origin_city, des_city):
        self._origin_city = origin_city
        self._des_city = des_city

    def run(self):
        """Initiator of combination.

        Run the program of combination.
        """
        print('City combination start...')
        self.__city_combiner()
        print('City combination finished.')

    def __city_combiner(self):
        """Combine the cities.

        Combine prefecture level cities with prefecture level cities or county level cities. And save them in a number of csv files.
        """
        
        pre_city_list = []
        cou_city_list = []

        prefecture_cities_file = os.path.join(
            global_settings.CITIES_URL, self._origin_city + '.csv')
        county_cities_file = os.path.join(global_settings.CITIES_URL, self._des_city + '.csv')

        with open(county_cities_file, mode='r', encoding='utf-8') as f_county_cities_file, open(prefecture_cities_file, mode='r', encoding='utf-8') as f_prefecture_cities_file:
            county_csv = csv.reader(f_county_cities_file)
            prefecture_csv = csv.reader(f_prefecture_cities_file)
            next(county_csv)
            next(prefecture_csv)
            for row in county_csv:
                cou_city_list.append(row)
            for row in prefecture_csv:
                pre_city_list.append(row)

        count_id = 0
        city_coms_all_file = global_settings.OD_URL + '/city_coms_all.csv'

        with open(city_coms_all_file, mode='w', encoding='utf-8') as f_city_coms:
            f_city_coms.write(
                'id,origin_lat,origin_lng,destination_lat,destination_lng,origin,destination,origin_region,destination_region\n')

            # 地级市与县级市之间组合
            len_of_pre_city = len(pre_city_list)
            len_of_cou_city = len(cou_city_list)

            for i in range(len_of_pre_city):
                for j in range(i+1, len_of_pre_city):
                    f_city_coms.write('{0},{1[3]},{1[4]},{2[3]},{2[4]},{1[1]},{2[1]},{1[2]},{2[2]}\n'.format(
                        count_id, pre_city_list[i], pre_city_list[j]))
                    count_id += 1

            for i in range(len_of_pre_city):
                for j in range(len_of_cou_city):
                    f_city_coms.write('{0},{1[3]},{1[4]},{2[3]},{2[4]},{1[1]},{2[1]},{1[2]},{2[2]}\n'.format(
                        count_id, pre_city_list[i], cou_city_list[j]))
                    count_id += 1

        # 分表
        with open(city_coms_all_file, mode='r', encoding='utf-8') as f_city_coms_all:
            city_coms_data_all = f_city_coms_all.readlines()
            print(count_id)
            for city_coms_start in range(0, count_id, 200000):
                city_coms_name = global_settings.OD_URL + \
                    'city_coms_{}.csv'.format(city_coms_start)
                city_coms_path = os.path.join(
                    global_settings.OD_URL, city_coms_name)
                with open(city_coms_path, mode='w', encoding='utf-8') as f_city_coms:
                    f_city_coms.write(
                        'id,origin_lat,origin_lng,destination_lat,destination_lng,origin,destination,origin_region,destination_region\n')
                    print(city_coms_start, city_coms_start + 200000)
                    city_coms_data = city_coms_data_all[city_coms_start:city_coms_start + 200000]
                    for city_com in city_coms_data:
                        f_city_coms.write(city_com)

def main():
    """
    main function
    """
    city_com = CombineCity('formatted_prefecture_coord',
                           'formatted_county_coord')
    city_com.run()

if(__name__ == '__main__'):
    main()
