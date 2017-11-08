# _*_ coding: utf-8 _*_

"""Combine the cities.

Combine the county-level cities each other. Save the combinations in csv files.
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

        prefecture_level_city_list, county_level_city_list = self.city_data_parser()
        count_id = 0
        
        od_dir = global_settings.OD_URL + 'county_to_county'
        if(os.path.exists(od_dir)):
            city_coms_all_filepath = od_dir + '/city_coms_all.csv'
        else:
            os.mkdir(od_dir)
            city_coms_all_filepath = od_dir + '/city_coms_all.csv'
        # city_coms_all_filepath = global_settings.OD_URL + 'city_coms_all.csv'
        
        with open(city_coms_all_filepath, mode='w', encoding='utf-8') as f_city_coms:
            f_city_coms.write('id,origin_city,destination_city,origin_region,destination_region\n')

            # # 地级市之间组合
            # len_of_pre_city = len(prefecture_level_city_list)
            # for i in range(len_of_pre_city):
            #     for j in range(i + 1, len_of_pre_city):
            #         f_city_coms.write('{0},{1},{2},{3},{4}\n'.format(count_id, prefecture_level_city_list[i][0], prefecture_level_city_list[j][0], prefecture_level_city_list[i][1], prefecture_level_city_list[j][1]))
            #         count_id += 1

            # # 地级市与县级市之间组合
            # for pre_city in prefecture_level_city_list:
            #     for cou_city in county_level_city_list:
            #         f_city_coms.write('{0},{1},{2},{3},{4}\n'.format(
            #             count_id, pre_city[0], cou_city[0], pre_city[1], cou_city[1]))
            #         count_id += 1

            # 县级市与县级市之间组合
            len_of_cou_city = len(county_level_city_list)
            for i in range(len_of_cou_city):
                for j in range(i + 1, len_of_cou_city):
                    f_city_coms.write('{0},{1},{2},{3},{4}\n'.format(count_id, county_level_city_list[i][0], county_level_city_list[j][0], county_level_city_list[i][1], county_level_city_list[j][1]))
                    count_id += 1

        # 分表
        with open(city_coms_all_filepath, mode='r', encoding='utf-8') as f_city_coms_all:
            city_coms_data_all = f_city_coms_all.readlines()
            print(count_id)
            for city_coms_start in range(0, count_id, 200000):
                city_coms_name = 'city_coms_{}.csv'.format(city_coms_start)
                city_coms_path = os.path.join(
                    global_settings.OD_URL, city_coms_name)
                with open(city_coms_path, mode='w', encoding='utf-8') as f_city_coms:
                    f_city_coms.write('id,origin_city,destination_city,origin_region,destination_region\n')
                    print(city_coms_start + 1, city_coms_start + 200001)
                    city_coms_data = city_coms_data_all[city_coms_start+1:city_coms_start+200001]
                    for city_com in city_coms_data:
                        f_city_coms.write(city_com)

    def city_data_parser(self):
        """Parser of city data.

        Parse the data of cities, store the cities in prefecture level city list and county level city list.

        Returns:
            A tuple of different type city lists. Each list stores the corresponding cities. For example:

            (['北京市', '上海市', '天津市', '吉林市', ...], ['崇文区', '台山市', '广宁县', ])
        """

        prefecture_level_city_list = []
        county_level_city_list = []
        city_file = os.path.join(global_settings.CITIES_URL, '{}.json'.format(self.__city_filename))

        with open(city_file, mode='r', encoding='utf-8') as city_data_json:
            city_data = json.loads(city_data_json.read())

            for (provincial_level_region, city_list) in city_data.items():
                if(provincial_level_region in ['香港特别行政区', '澳门特别行政区', '台湾省']):
                    continue
                # 给直辖市添加region
                if(provincial_level_region in ['北京市', '上海市', '天津市', '重庆市']):
                    city_region_group = [
                        provincial_level_region, provincial_level_region]
                    prefecture_level_city_list.append(city_region_group)

                # 给地级市添加region
                if(city_list['prefecture_level_city'] != []):
                    for pre_city in city_list['prefecture_level_city']:
                        city_region_group = [pre_city[1], pre_city[1]]
                        prefecture_level_city_list.append(city_region_group)

                # 给县级市添加region
                for cou_city in city_list['county_level_city']:
                    city_region_group = []
                    if(city_list['prefecture_level_city'] == []):
                        city_region_group = [cou_city[1], provincial_level_region]
                    else:
                        # 先指定cou_city以自身为region
                        city_region_group = [cou_city[1], cou_city[1]]
                        # 如果在prefecture level city列表中发现了地级行政区号相同的地级市，再将cou_city的region指定为该地级市
                        for pre_city in city_list['prefecture_level_city']:
                            if(cou_city[0][0:4] == pre_city[0][0:4]):
                                city_region_group = [cou_city[1], pre_city[1]]
                                break
                    county_level_city_list.append(city_region_group)

        return (prefecture_level_city_list, county_level_city_list)

def main():
    """
    main function
    """
    city_com = CombineCity('city_dataset')
    city_com.run()

if(__name__ == '__main__'):
    main()
