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

    result['distance'] = response[u'result'][u'routes'][0][u'distance'] / 1000
    result['duration'] = response[u'result'][u'routes'][0][u'duration']

    path_string = ''
    count_of_substeps = 0

    with open('test.txt', 'w') as f1:
        num_of_steps = len(response[u'result'][u'routes'][0][u'steps'])
        for i in range(num_of_steps):

            count_of_substeps += 1

            if(i == num_of_steps - 1):

                path_string = response[u'result'][u'routes'][0][u'steps'][i][u'path']
                f1.write(path_string + '\n')
                break

            path_string = response[u'result'][u'routes'][0][u'steps'][i][u'path']
            f1.write(path_string + ';')

    for (key, value) in result.items():
        print(key + ':' + str(value))

    print('count_of_substeps: ' + str(count_of_substeps))


url = 'http://api.map.baidu.com/direction/v1?mode=walking&origin=清华大学&destination=北京大学&region=北京&output=json&ak=p4dXMFdFyG7qEtmhrLNGd1I8qoRlCzcX'

# time_collection = []
# for i in range(50):
#     start = time.time()
#     response = requests.get(url).json()
#     end = time.time()
#     time_collection.append(end - start)

# print("average time:{}".format(sum(time_collection) / 50))

response = requests.get(url).json()
dataStructured(response)
