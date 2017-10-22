import csv
from collections import Iterable
with open('city_coms.csv', 'r', encoding='utf-8') as f:
    f_csv = csv.reader(f)
    print(f_csv)
    # csv.reader读取后的数据是一个可迭代对象
    print(isinstance(f_csv, Iterable))
    headers = next(f_csv)
    for row in f_csv:
        print(row)