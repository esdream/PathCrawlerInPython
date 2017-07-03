# _*_ coding: utf-8 _*_

"""
install scrpit: python3 setup.py install
"""

from setuptools import setup

setup(
    name="path_crawler",
    version="0.0.1",
    author="Zucky FU",
    keywords=["spider", "crawler", "multi-threads", "path"],
    install_requires=[
        "beautifulsoup4",    # beautifulsoup4, parser for html
        "sqlalchemy",    # sqalchemy, orm map of database
    ]
)
