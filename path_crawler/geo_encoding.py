import csv
import requests

from path_crawler.conf import global_settings

def address_to_coord(encoding_param):

    address_file = global_settings.GEO_ENCODING_URL + encoding_param['input_file'] + '.csv'
    coord_file = global_settings.GEO_ENCODING_URL + encoding_param['output_file'] + '_coord' + '.csv'
    crawl_error_file = global_settings.GEO_ENCODING_URL + encoding_param['output_file'] + '_crawl_error' + '.csv'
    parse_error_file = global_settings.GEO_ENCODING_URL + encoding_param['output_file'] + '_parse_error' + '.csv'

    with open(address_file, mode='r', encoding='utf-8') as f_address_file, open(coord_file, mode='w', encoding='utf-8') as f_coord_file, open(crawl_error_file, mode='w', encoding='utf-8') as f_crawl_error, open(parse_error_file, mode='w', encoding='utf-8') as f_parse_error:
        
        f_coord_file.write('id,address,region,lat,lng\n')
        f_parse_error.write('id,address,region\n')
        f_crawl_error.write('id,address,region\n')

        address_csv = csv.reader(f_address_file)
        next(address_csv)
        address = []
        for row in address_csv:
            address.append(row)
        
        for address in address:

            url = 'http://api.map.baidu.com/geocoder/v2/?address={0}&city={1}&output=json&ak={2}'.format(
                address[1], address[2], encoding_param['key'])

            crawl_timeout = 2
            while(crawl_timeout > 0):
                crawl_timeout -= 1
                try:
                    response = requests.get(url, timeout=5).json()
                    break
                except Exception as crawl_error:
                    if(crawl_timeout == 0):
                        print('crawl {0}(region{1}) error!'.format(
                            address[1], address[2]))
                        print(crawl_error)
                        f_crawl_error.write(
                            '{0[0]},{0[1]},{0[2]}\n'.format(address))
                            
            try:
                parse_timeout = 2
                while(parse_timeout > 0):
                    parse_timeout -= 1
                    lat = response[u'result'][u'location'][u'lat']
                    lng = response[u'result'][u'location'][u'lng']
                    f_coord_file.write(
                        '{0[0]},{0[1]},{0[2]},{1},{2}\n'.format(address, lat, lng))
                    print('Geocode {0}(region{1}) succeed: {2}, {3}'.format(
                        address[1], address[2], lat, lng))
                    break
            except Exception as parse_error:
                print('parse {0}(region{1}) error!'.format(
                    address[1], address[2]))
                print(parse_error)
                f_parse_error.write('{0[0]},{0[1]},{0[2]}\n'.format(address))

def main():
    input_file = input('请输入进行地理编码的文件（不需要输入文件类型）：')
    output_file = input('请输入输出文件名（不需要输入文件类型）：')
    api_name = input('请选择地理编码平台（1 Baidu Map；2 AMap）：')
    if(str(api_name) == '1'):
        encoding_type = input('请选择地理编码方式（默认为1。1 地址转坐标；2 坐标转地址）：')
        key = input('请输入百度地图开发者密钥：')

        encoding_param = {
            'input_file': input_file,
            'output_file': output_file,
            'encoding_type': encoding_type,
            'key': key
        }
    address_to_coord(encoding_param)

if(__name__ == '__main__'):
    main()
