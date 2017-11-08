# _*_ coding: utf-8 _*_
# !/usr/bin/python

import time
import requests
import json

# 结构化数据
def dataStructured(response):
    if(str(response[u'status']) != '1'):
        # 如果返回值状态码不为0（成功），则应记录返回值对应的url
        print(response[u'status'])
        print(response[u'info'])

    result = {}
    result['distance_km'] = int(response[u'route'][u'paths'][0][u'distance']) / 1000
    result['duration_s'] = response[u'route'][u'paths'][0][u'duration']
    result['texi_cost'] = response[u'route'][u'taxi_cost']
    path_string = ''
    count_of_substeps = 0

    with open('test.txt', mode='w', encoding='utf-8') as f1:
        num_of_steps = len(response[u'route'][u'paths'][0][u'steps'])
        for i in range(num_of_steps):
            count_of_substeps += 1

            if(i == num_of_steps - 1):
                path_string = response[u'route'][u'paths'][0][u'steps'][i][u'polyline']
                f1.write(path_string + '\n')
                continue
            path_list = response[u'route'][u'paths'][0][u'steps'][i][u'polyline'].split(';')
            path_string = ";".join(path_list[0:-1]) + ';'
            f1.write(path_string)

    # for (key, value) in result.items():
    #     print(key + ':' + str(value))

    # print('count_of_substeps: ' + str(count_of_substeps))


url = 'http://restapi.amap.com/v3/direction/driving?key=08f38ccac051f9d5e32d1a121db39ab4&origin=120.384428,36.105214&destination=115.782601,36.782069'

time_collection = []
try:
    for i in range(50):
        start = time.time()
        response = requests.get(url).json()
        end = time.time()
        time_collection.append(end - start)
except Exception as err:
    print(err)

print("average time:{}".format(sum(time_collection) / 50))

dataStructured(response)
