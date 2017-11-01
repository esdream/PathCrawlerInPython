# _*_ coding: utf-8 _*_

"""Format the city list.

Add prefecture city as region to prefecture city and county city, then list all cities.
Args:
    city_filename: the file of cities.
"""

import os
from path_crawler.conf import global_settings
from path_crawler.city_parser.combine_cities import CombineCity

class FormatCity(object):
    """Format the cities.

    The class used to format the cities.
    """

    def __init__(self, city_filename):
        self.__city_filename = city_filename

    def run(self):
        """
        Initiator of formatting.
        """
        print('City formatting start...')
        self.__city_formater()
        print('City formatting finished.')

    def __city_formater(self):
        city_combiner = CombineCity(self.__city_filename)
        prefecture_level_city_list, county_level_city_list = city_combiner.city_data_parser()
        count_id = 1

        formatting_cities_filepath = global_settings.CITIES_URL + 'formatting_cities.csv'
        with open(formatting_cities_filepath, mode='w', encoding='utf-8') as f_format:
            f_format.write('id,city,region\n')

            for pre_city in prefecture_level_city_list:
                f_format.write('{0},{1},{2}\n'.format(count_id, pre_city[0], pre_city[1]))
                count_id += 1

            for cou_city in county_level_city_list:
                f_format.write('{0},{1},{2}\n'.format(count_id, cou_city[0], cou_city[1]))
                count_id += 1

def main():
    city_formatter = FormatCity('city_dataset')
    city_formatter.run()

if(__name__ == '__main__'):
    main()
