# _*_ coding: utf-8 _*_

"""Combine the cities.

Combine the primary cities and county-level cities. Save the combinations in database.
Args:
    city_filename: The file of cities.
"""

import os
import json
from path_crawler.conf import global_settings

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
        self.__city_combiner()
        print('City combination finished.')

    def __city_combiner(self):
        """Combine the cities.

        Combine prefecture level cities with prefecture level cities or county level cities. And save them in a number of csv files.
        """

        prefecture_level_city_list, county_level_city_list = self.__city_data_parser()
        count_id = 0
        
        # 组合
        city_coms_all_filepath = global_settings.CITY_COMS_URL + 'city_coms_all.csv'
        
        with open(city_coms_all_filepath, mode='w', encoding='utf-8') as f_city_coms:
            f_city_coms.write('id,origin_city,destination_city\n')

            # 地级市之间组合
            len_of_pre_city = len(prefecture_level_city_list)
            for i in range(len_of_pre_city):
                for j in range(i + 1, len_of_pre_city):
                    f_city_coms.write('{0},{1},{2}\n'.format(count_id, prefecture_level_city_list[i], prefecture_level_city_list[j]))
                    count_id += 1

            # 地级市与县级市之间组合
            for pre_city in prefecture_level_city_list:
                for cou_city in county_level_city_list:
                    f_city_coms.write('{0},{1},{2}\n'.format(count_id, pre_city, cou_city))
                    count_id += 1

        # 分表
        with open(city_coms_all_filepath, mode='r', encoding='utf-8') as f_city_coms_all:
            city_coms_data_all = f_city_coms_all.readlines()
            print(count_id)
            for city_coms_start in range(0, count_id, 200000):
                city_coms_name = 'city_coms_{}.csv'.format(city_coms_start)
                city_coms_path = os.path.join(
                    global_settings.CITY_COMS_URL, city_coms_name)
                with open(city_coms_path, mode='w', encoding='utf-8') as f_city_coms:
                    f_city_coms.write('id,origin_city,destination_city\n')
                    print(city_coms_start + 1, city_coms_start + 200001)
                    city_coms_data = city_coms_data_all[city_coms_start+1:city_coms_start+200001]
                    for city_com in city_coms_data:
                        f_city_coms.write(city_com)

    def __city_data_parser(self):
        """Parser of city data.

        Parse the data of cities, store the cities in prefecture level city list and county level city list.

        Returns:
            A tuple of different type city lists. Each list stores the corresponding cities. For example:

            ['北京市', '上海市', '天津市', '吉林市', ...]
        """

        prefecture_level_city_list = []
        county_level_city_list = []
        city_file = os.path.join(global_settings.CITIES_URL, '{}.json'.format(self.__city_filename))

        with open(city_file, mode='r', encoding='utf-8') as city_data_json:
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

def main():
    """
    main function
    """
    city_com = CombineCity('city_dataset')
    city_com.run()

if(__name__ == '__main__'):
    main()
