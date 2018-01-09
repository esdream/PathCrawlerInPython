import csv
import requests

from path_crawler.conf import global_settings

# 百度地图地址转坐标
def baidu_address_to_coord(encoding_param):
    """Address to coordinate using Baidu Map.
        Encoding the address to coordinate using Baidu Map. The format of input file as follow.
            id,address,city
        Args:
            encoding_param: Parameters of input file and geo-encoding parameters.
    """

    address_file = global_settings.GEO_ENCODING_URL + encoding_param['input_file'] + '.csv'
    coord_file = global_settings.GEO_ENCODING_URL + encoding_param['output_file'] + '.csv'
    crawl_error_file = global_settings.GEO_ENCODING_URL + encoding_param['output_file'] + '_crawl_error' + '.csv'
    parse_error_file = global_settings.GEO_ENCODING_URL + encoding_param['output_file'] + '_parse_error' + '.csv'

    with open(address_file, mode='r', encoding='utf-8') as f_address_file, open(coord_file, mode='w', encoding='utf-8') as f_coord_file, open(crawl_error_file, mode='w', encoding='utf-8') as f_crawl_error, open(parse_error_file, mode='w', encoding='utf-8') as f_parse_error:
        
        f_coord_file.write('id,address,city,lat,lng,level\n')
        f_parse_error.write('id,address,city\n')
        f_crawl_error.write('id,address,city\n')

        address_csv = csv.reader(f_address_file)
        next(address_csv)
        addresses = []
        for row in address_csv:
            addresses.append(row)
        
        for address in addresses:

            url = 'http://api.map.baidu.com/geocoder/v2/?address={0}&city={1}&output=json&ak={2}&ret_coordtype={3}'.format(
                address[1], address[2], encoding_param['key'], encoding_param['ret_coordtype'])

            crawl_timeout = 2
            while(crawl_timeout > 0):
                crawl_timeout -= 1
                try:
                    response = requests.get(url, timeout=5).json()
                    break
                except Exception as crawl_error:
                    if(crawl_timeout == 0):
                        print('crawl {0}(city{1}) error!'.format(
                            address[1], address[2]))
                        print(crawl_error)
                        f_crawl_error.write(
                            '{0[0]},{0[1]},{0[2]}\n'.format(address))
                            
            try:
                parse_timeout = 2
                while(parse_timeout > 0):
                    # 如果返回值是模糊打点，直接按解析错误记录
                    precise = response[u'result'][u'precise']
                    if(str(precise) == '0'):
                        print('address {0}(city{1}) precise is 0!'.format(
                            address[1], address[2]))
                        f_parse_error.write('{0[0]},{0[1]},{0[2]}\n'.format(address))
                        break

                    parse_timeout -= 1
                    lat = response[u'result'][u'location'][u'lat']
                    lng = response[u'result'][u'location'][u'lng']
                    level = response[u'result'][u'level']
                    f_coord_file.write(
                        '{0[0]},{0[1]},{0[2]},{1},{2},{3}\n'.format(address, lat, lng, level))
                    print('Geocode {0}(city{1}) succeed: {2}, {3}, {4}'.format(
                        address[1], address[2], lat, lng, level))
                    break
            except Exception as parse_error:
                print('parse {0}(city{1}) error!'.format(
                    address[1], address[2]))
                print(parse_error)
                f_parse_error.write('{0[0]},{0[1]},{0[2]}\n'.format(address))

# 高德地图地址转坐标
def amap_address_to_coord(encoding_param):
    """Address to coordinate using AMap.
        Encoding the address to coordinate using AMap. The format of input file as follow.
            id,address,city
        Args:
            encoding_param: Parameters of input file and geo-encoding parameters.
    """

    address_file = global_settings.GEO_ENCODING_URL + encoding_param['input_file'] + '.csv'
    coord_file = global_settings.GEO_ENCODING_URL + encoding_param['output_file'] + '.csv'
    crawl_error_file = global_settings.GEO_ENCODING_URL + encoding_param['output_file'] + '_crawl_error' + '.csv'
    parse_error_file = global_settings.GEO_ENCODING_URL + encoding_param['output_file'] + '_parse_error' + '.csv'

    with open(address_file, mode='r', encoding='utf-8') as f_address_file, open(coord_file, mode='w', encoding='utf-8') as f_coord_file, open(crawl_error_file, mode='w', encoding='utf-8') as f_crawl_error, open(parse_error_file, mode='w', encoding='utf-8') as f_parse_error:
        
        f_coord_file.write('id,address,city,lat,lng,level\n')
        f_parse_error.write('id,address,city\n')
        f_crawl_error.write('id,address,city\n')

        address_csv = csv.reader(f_address_file)
        next(address_csv)
        addresses = []
        for row in address_csv:
            addresses.append(row)
        
        for address in addresses:
            url = 'http://restapi.amap.com/v3/geocode/geo?key={key}&address={0[1]}&city={0[2]}'.format(address, **encoding_param)

            crawl_timeout = 2
            while(crawl_timeout > 0):
                crawl_timeout -= 1
                try:
                    response = requests.get(url, timeout=5).json()
                    break
                except Exception as crawl_error:
                    if(crawl_timeout == 0):
                        print('crawl {0}(city{1}) error!'.format(
                            address[1], address[2]))
                        print(crawl_error)
                        f_crawl_error.write(
                            '{0[0]},{0[1]},{0[2]}\n'.format(address))
                            
            try:
                parse_timeout = 2
                while(parse_timeout > 0):
                    parse_timeout -= 1

                    # 如果response中的adcode和本地的adcode相等，则记录。否则记为错误
                    if(str(response[u'geocodes'][0][u'adcode']) == str(address[2])):
                        location = response[u'geocodes'][0][u'location']
                        lng, lat = location.split(',')
                        level = response[u'geocodes'][0][u'level']
                        f_coord_file.write(
                            '{0[0]},{0[1]},{0[2]},{1},{2},{3}\n'.format(address, lat, lng, level))
                        print('Geocode {0}(city{1}) succeed: {2}, {3}'.format(
                            address[1], address[2], lat, lng))
                        break
                
                    else:
                        f_parse_error.write(
                            '{0[0]},{0[1]},{0[2]}\n'.format(address))
                        break

            except Exception as parse_error:
                print('parse {0}(city{1}) error!'.format(
                    address[1], address[2]))
                print(parse_error)
                f_parse_error.write('{0[0]},{0[1]},{0[2]}\n'.format(address))
    
def main():
    input_file = input('请输入进行地理编码的文件（不需要输入文件类型）：')
    output_file = input('请输入输出文件名（不需要输入文件类型）：')
    api_name = input('请选择地理编码平台（1 Baidu Map；2 AMap）：')
    if(str(api_name) == '1'):
        encoding_type = input('请选择地理编码方式（默认为1。1 地址转坐标；2 坐标转地址）：')
        ret_coordtype = input('请输入返回的坐标系（ 默认为1\n\
            1 bd09ll（百度经纬度坐标）；\n\
            2 gcj02ll（国测局坐标）；\n\
            3 bd09mc（百度墨卡托米制坐标））: ')
        coordtype = {
            '1': 'bd09ll',
            '2': 'gcj02ll',
            '3': 'bd09mc'
        }
        if(str(encoding_type) == '1'):
            key = input('请输入百度地图开发者密钥：')
            encoding_param = {
                'input_file': input_file,
                'output_file': output_file,
                'key': key,
                'ret_coordtype': coordtype[str(ret_coordtype)] or 'bd09ll'
            }
            baidu_address_to_coord(encoding_param)

    elif(str(api_name) == '2'):
        encoding_type = input('请选择地理编码方式（默认为1。1 地址转坐标；2 坐标转地址）：')
        if(str(encoding_type) == '1'):
            key = input('请输入高德地图开发者密钥：')
            encoding_param = {
                'input_file': input_file,
                'output_file': output_file,
                'key': key
            }
            amap_address_to_coord(encoding_param)


if(__name__ == '__main__'):
    main()
