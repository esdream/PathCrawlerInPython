# -*- coding: utf-8 -*-
# !/usr/bin/python

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

    for step in response[u'result'][u'routes'][0][u'steps']:
        print(step[u'path'][-31:-1])

url = 'http://api.map.baidu.com/direction/v1?mode=driving&origin=青岛市&destination=临清市&origin_region=青岛市&destination_region=临清市&output=json&ak=YtsG0tZOwjVgDkcLZDuEiSL2PbKzP9HG'

response = requests.get(url).json()
dataStructured(response)
