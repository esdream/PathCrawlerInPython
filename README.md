# Path Crawler

![PyPI](https://img.shields.io/pypi/pyversions/Django.svg)
![license](https://img.shields.io/github/license/mashape/apistatus.svg)

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

### Place Origin-Destination Data

Place your OD data in `path_crawler/data/od/` directory.
You should format your OD data as follow and save it as a **`.csv`** file(**split in comma `,`**).

|`id`|`origin_lat`|`origin_lng`|`destination_lat`|`destination_lng`|`origin**`|`destination**`|`origin_region*`|`destination_region*`|
|---|---|---|---|---|---|---|---|---|
||||||||||

*Neccessary if use Baidu Map API driving mode.

**Neccessary if use Baidu Map API driving mode and set place name as origin and destination.

### Crawl path data

To crawl path data, run command as follow.
```shell
$ python -m path_crawler.crawler
```

In path crawler, we provide two Web Map API methods: Baidu Map API and AMap API(GaoDe Map API). 