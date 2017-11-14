# Path Crawler

![PyPI](https://img.shields.io/pypi/pyversions/Django.svg)

Crawl the path data using Web Map API.

## Getting Started

### Prerequisites

First, clone this repo.
```shell
$ git clone https://github.com/esdream/PathCrawlerInPython.git
``` 

This code is written in **Python 3.6** and using the sqlite3. You'd better to install the Python 3.6 so that you will install the sqlite3 automatically.

Then install requirements.
```shell
$ cd PATHCRAWLERINPYTHON
$ pip install -r requirements.txt
```

### Format and Place Origin-Destination Data

You should format your OD data as follow and save it as a **`.csv`** file(**split in comma `,`**).

#### Baidu Map API driving mode

|`id`|`origin_lat`|`origin_lng`|`destination_lat`|`destination_lng`|`origin*`|`destination*`|`origin_region`|`destination_region`|
|---|---|---|---|---|---|---|---|---|
||||||||||

*If you do not set place name as origin and destination, you can set this column value as ''.

#### Baidu Map API transit mode

|`id`|`origin_lat`|`origin_lng`|`destination_lat`|`destination_lng`|
|---|---|---|---|---|
|||||

#### Baidu Map API walking mode**

|`id`|`origin_lat`|`origin_lng`|`destination_lat`|`destination_lng`|`region`|
|---|---|---|---|---|---|
||||||

**Walking mode can only use inner city.

#### AMap API driving mode

|`id`|`origin_lat`|`origin_lng`|`destination_lat`|`destination_lng`|
|---|---|---|---|---|
|||||

After formatting, place your OD data in `path_crawler/data/od/` directory.

### Crawl path data

To crawl path data, run command as follow.
```shell
$ python -m path_crawler.path_spider
```

In path crawler, we provide two Web Map API -- Baidu Map API and AMap API(GaoDe Map API), which include 2 transport schemes -- driving and transit. You can choose precise schemes and crawling parameters for your OD data and your purposes. 

After data crawling, the path data will be stored in `path_crawler/data/path_data/` directory.

## Deal with Error

### Deal with Crawl Error

In version 1.5.1, the crawler will resend requests of od data which was crawled failed. So you do not need to deal with crawl error manually.

### Deal with Parse Error

The od data which was parsed failed will be recorded in a csv file named `input-filename.csv` and be stored in `path_crawler/data/parse_error/` directory. Normally, you should copy this parse error file to `path_crawler/data/od/` directory and **use another Web Map API to crawl it again**. For instance, you crawl a od data using Baidu Map API and get a parse error file at first time, you should copy this file to od data and crawl it again using Amap API.

## Other Tools

### Geo Encoding

Transform address and coordinates each other.

#### Transform Addresses to Coordinates

First, format your address data as follow and store them into a `.csv` file(encoded in utf-8).

|id|address|city|
|---|---|---|
||||

Then place it in `path_crawler/data/geo_encoding/` directory.

Finally, run command below.
```shell
$ python -m path_crawler.geo_encodings
```

#### Transform Coordinates to Addresses

WIP...