'''Search Poi

Search poi info from AMap.
'''

import csv
import requests

from path_crawler.conf import global_settings

def search_poi(search_param):
    pass

def main():
    search_type = input('请输入搜索方式：1 关键字搜索；2 周边搜索；3 多边形搜索')
    search_param = {}
    if(int(search_type) == 1):
        search_file = input('请输入使用关键字搜索的文件，文件格式为.csv：')
    elif(int(search_type) == 2):
        search_file = input('请输入使用周边搜索的文件，文件格式为.csv：')
    elif(int(search_type) == 3):
        search_file = input('请输入使用多边形搜索的文件，文件格式为.csv：')

    key = input('请输入高德地图开发者密钥：')


if(__name__ == '__main__'):
    main()
