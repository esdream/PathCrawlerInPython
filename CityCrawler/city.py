# -*- coding: utf-8 -*-
# !/usr/bin/python

import urllib2
import urllib
from bs4 import BeautifulSoup
import json
import re

# 从国家统计局官网抓取县及县以上行政区划代码
def cityCrawler():

    with open('cityText.txt', 'w') as f:
        url = 'http://www.stats.gov.cn/tjsj/tjbz/xzqhdm/201703/t20170310_1471429.html'
        sendHeaders = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest'
        }
        request = urllib2.Request(url, headers = sendHeaders)
        response = urllib2.urlopen(request)
        htmlDoc = response.read()
        html = BeautifulSoup(htmlDoc, 'lxml')

        # 获得县及以上行政区名称与行政区划代码
        allSpanContents = html.select('.MsoNormal span')
        spanNum = len(allSpanContents)

        for i in range(spanNum):
            # 除去文本中的空白字符，这一步是将保存下来的文本中的空白字符复制为strip函数的参数
            spanText = allSpanContents[i].get_text().encode('utf-8').strip('　  ')
            # 除去空白字符后，若非空，则为行政区划代码或名称，写入cityText.txt
            if(spanText):
                f.write(spanText + '\n')

# 结构化数据
def dataToJson():

    cityDataset = {}

    cityList = []
    cityCodes = []
    cityNames = []

    with open('cityText.txt', 'r') as f:

        # 按行读取cityText.txt，将数据分至行政区划代码(cityCodes)和名称(cityNames)两个列表中
        for line in f:
            matchObj  = re.search(r'\d{6}', line)
            if(matchObj):
                cityCodes.append(line.strip())
            else:
                cityNames.append(line.strip())

    # 使用列表暂时存储cityCodes和cityNames对，保持数据有序，便于以下结构化操作
    lenOfCity = len(cityCodes)
    for i in range(lenOfCity):
        cityList.append((cityCodes[i], cityNames[i]))

    with open('cityDataset.json', 'w') as f:

        provinceNow = ''
        cityListNum = len(cityList)

        for i in range(cityListNum):
            # 省级行政区号为'xx0000'
            if(re.search(r'\d{2}[0]{4}', cityList[i][0])):
                provinceNow = cityList[i][1]
                cityDataset[provinceNow] = {
                    'prefectureLevelCity': [],
                    'countyLevelCity': []
                }
                continue
            # 地级市行政区号为'xxxx00'
            elif(re.search(r'\d{4}[0]{2}', cityList[i][0])):
                if(cityList[i][1] == '省直辖县级行政区划' or cityList[i][1] == '市辖区'):
                    continue
                cityDataset[provinceNow]['prefectureLevelCity'].append(cityList[i][1])
                continue

            # 县级行政区
            elif(cityList[i][1] == '市辖区' or cityList[i][1] == '县'):
                continue
            cityDataset[provinceNow]['countyLevelCity'].append(cityList[i][1])

        data = json.dumps(cityDataset, ensure_ascii=False)
        f.write(data)

if(__name__ == '__main__'):
    cityCrawler()
    dataToJson()
