import os
import chardet
from path_crawler.conf import global_settings

def to_utf8(to_utf8_param):
    """
    """
    source_filename = global_settings.OD_URL + to_utf8_param['input_file'] + '.csv'
    utf8_filename = global_settings.OD_URL + \
        to_utf8_param['input_file'] + 'utf8.csv'
    source = open(source_filename, 'rb').read()
    source_encoding = chardet.detect(source)['encoding']
    if(source_encoding):
        if(source_encoding != 'utf-8'):
            with open(source_filename, mode='r', encoding=source_encoding) as f_source, open(utf8_filename, mode='w', encoding='utf-8') as f_utf8:
                data = f_source.readlines()
                f_utf8.writelines(data)
                print('Encode data to utf-8 format.')
        else:
            print('The encoing of data is utf-8.')
    else:
        print('Source file encoding is unknown!')

def main():
    input_file = input('请输要编码成utf-8的文件（不需要手动输入文件后缀，默认.csv）：')
    to_utf8_param = {
        'input_file': input_file
    }
    to_utf8(to_utf8_param)

if(__name__ == '__main__'):
    main()
