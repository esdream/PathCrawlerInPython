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
    result['drivingDuration'] = response[u'result'][u'routes'][0][u'duration']
    result['distanceKM'] = response[u'result'][u'routes'][0][u'distance'] / 1000

    numOfSteps = len(response[u'result'][u'routes'][0][u'steps'])
    pathString = ''
    with open('test.txt', 'w') as f1:
        for i in range(numOfSteps):
            if(i == numOfSteps - 1):
                pathString = response[u'result'][u'routes'][0][u'steps'][i][u'path']
                f1.write(pathString + '\n')
                break
            pathList = response[u'result'][u'routes'][0][u'steps'][i][u'path'].split(';')
            pathString = ";".join(pathList[0:-1]) + ';'
            f1.write(pathString)

    # with open('test.txt', 'r') as f1, open('validation.txt', 'w') as f2:
    #     path = f1.read()
    #     pathpoints = path.split(';')
    #     for point in pathpoints:
    #         f2.write(point + '\n')


url1 = 'http://api.map.baidu.com/direction/v1?mode=driving&origin=青岛市&destination=临清市&origin_region=青岛市&destination_region=临清市&output=json&ak=p4dXMFdFyG7qEtmhrLNGd1I8qoRlCzcX'

url2 = 'http://api.map.baidu.com/direction/v1?mode=driving&origin=36.105214,120.384428&destination=36.782069,115.782601&origin_region=青岛市&destination_region=临清市&output=json&ak=p4dXMFdFyG7qEtmhrLNGd1I8qoRlCzcX'

url3 = 'http://api.map.baidu.com/direction/v1?mode=driving&origin=36.10521490127382,120.38442818368189&destination=36.78206947311332,115.78260175172889&origin_region=青岛市&destination_region=临清市&output=json&ak=p4dXMFdFyG7qEtmhrLNGd1I8qoRlCzcX'

time_collection = []
try:
    for i in range(50):
        start = time.time()
        response = requests.get(url1).json()
        end = time.time()
        time_collection.append(end - start)
except Exception as err:
    print(err)

print("average time:{}".format(sum(time_collection) / 50))
try:
    dataStructured(response)
except Exception as err:
    print(err)
