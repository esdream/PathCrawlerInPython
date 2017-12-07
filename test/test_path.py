import os
from path_crawler.conf import global_settings

if(__name__ == '__main__'):
    print(os.path.join(global_settings.PATH_DATA_URL, 'name_as' + '_file.db'))