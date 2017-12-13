'''Convert Geo Coord.

Convert geo coord to baidu coordinate.
'''

import os
import csv
import requests

from path_crawler.conf import global_settings

def geoconv(geoconv_params):
    source_file = os.path.join(
        global_settings.OD_URL, geoconv_params['source_coords'] + '.csv')
    conv_file = os.path.join(
        global_settings.OD_URL, geoconv_params['source_coords'] + '_conv.csv')
    crawl_error_file = os.path.join(
        global_settings.CRAWL_ERROR_URL, geoconv_params['source_coords'] + '_crawl_error.csv')
    parse_error_file = os.path.join(
        global_settings.PARSE_ERROR_URL, geoconv_params['source_coords'] + '_parse_error.csv')

    with open(source_file, mode='r', encoding='utf-8') as f_source, open(conv_file, mode='w', encoding='utf-8') as f_conv, open(parse_error_file, mode='w', encoding='utf-8') as f_parse_error, open(crawl_error_file, mode='w', encoding='utf-8') as f_crawl_error:

        f_conv.write('id,lat,lng\n')
        f_parse_error.write('id,lat,lng\n')
        f_crawl_error.write('id,lat,lng\n')

        source_csv = csv.reader(f_source)
        next(source_csv)
        coords = []
        for row in source_csv:
            coords.append(row)

        for coord in coords:

            url = 'http://api.map.baidu.com/geoconv/v1/?coords={0[2]},{0[1]}&from={source_type}&to=5&ak={ak}'.format(coord, **geoconv_params)

            # 抓取
            crawl_timeout = 2
            while(crawl_timeout > 0):
                crawl_timeout -= 1
                try:
                    response = requests.get(url, timeout=5).json()
                    print('crawl coords {0[0]} succeed!'.format(coord))
                    break
                except Exception as crawl_error:
                    if(crawl_timeout == 0):
                        print('crawl coords {0[0]} error!'.format(coord))
                        print(crawl_error)
                        f_crawl_error.write(
                            '{0[0]},{0[1]},{0[2]}\n'.format(coord))

            # 解析
            try:
                lat = response[u'result'][0][u'y']
                lng = response[u'result'][0][u'x']
                f_conv.write('{0[0]},{1},{2}\n'.format(coord, lat, lng))

                print(
                    'parse coords {0[0]} succeed!'.format(coord))
            except Exception as parse_error:
                print('parse coords {0[0]} error!'.format(coord))
                print(parse_error)
                f_parse_error.write('{0[0]},{0[1]},{0[2]}\n'.format(coord))


def main():
    source_coords = input('请输入需要转换的源坐标文件名（不需要输入文件拓展名，默认为.csv）：')
    source_type = input('请输入源坐标类型：\n \
        1：GPS设备获取的角度坐标，wgs84坐标\n \
        2：GPS获取的米制坐标、sogou地图所用坐标\n \
        3：google地图、soso地图、aliyun地图、mapabc地图和amap地图所用坐标，国测局（gcj02）坐标\n \
        4：3中列表地图坐标对应的米制坐标\n \
        5：百度地图采用的经纬度坐标\n \
        6：百度地图采用的米制坐标\n \
        7：mapbar地图坐标\n \
        8：51地图坐标：')
    ak = input('请输入百度开发者密钥：')

    geoconv_params = {
        'source_coords': source_coords,
        'source_type': source_type,
        'ak': ak
    }

    geoconv(geoconv_params)

if(__name__ == '__main__'):
    main()
