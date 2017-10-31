# _*_ coding: utf-8 _*_
# !/usr/bin/python

import time
import requests
import json

# 结构化数据
def dataStructured(response):
    if(response[u'status'] not in [0]):
        # 如果返回值状态码不为0（成功），则应记录返回值对应的url
        print(response[u'status'])

    result = {}
    result['origin_city'] = response[u'result'][u'origin'][u'city_name']
    result['destination_city'] = response[u'result'][u'destination'][u'city_name']
    result['distance'] = response[u'result'][u'routes'][0][u'distance'] / 1000
    result['duration'] = response[u'result'][u'routes'][0][u'duration']
    result['price'] = response[u'result'][u'routes'][0][u'price']
    
    path_string = ''
    count_of_substeps = 0

    with open('test.txt', 'w') as f1:
        num_of_steps = len(response[u'result'][u'routes'][0][u'steps'])
        for i in range(num_of_steps):
            num_of_substeps = len(
                response[u'result'][u'routes'][0][u'steps'][i])
            for j in range(num_of_substeps):

                count_of_substeps += 1

                if(i == num_of_steps - 1 and j == num_of_substeps - 1):
                    path = response[u'result'][u'routes'][0][u'steps'][i][j][u'path']
                    f1.write(path)
                    continue
                elif(i != num_of_steps - 1 and j == num_of_substeps - 1):
                    path = response[u'result'][u'routes'][0][u'steps'][i][j][u'path'] + ';'
                    f1.write(path)
                    continue
                path = response[u'result'][u'routes'][0][u'steps'][i][j][u'path'].split(';')
                path_string = ";".join(path[0:-1]) + ';'
                f1.write(path_string)
        # 路径全部写入完成后换行
        f1.write('\n')

    for (key, value) in result.items():
        print(key + ':' + str(value))

    print('count_of_substeps: ' + str(count_of_substeps))

url = 'http://api.map.baidu.com/direction/v2/transit?origin=40.056878,116.30815&destination=31.222965,121.505821&ak=YtsG0tZOwjVgDkcLZDuEiSL2PbKzP9HG'

# time_collection = []
# for i in range(50):
#     start = time.time()
#     response = requests.get(url).json()
#     end = time.time()
#     time_collection.append(end - start)

# print("average time:{}".format(sum(time_collection) / 50))

response = requests.get(url).json()
dataStructured(response)
