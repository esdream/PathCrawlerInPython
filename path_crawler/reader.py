"""
The Collection of Data Reader

See document in docs/topics/reader.md
"""

import os
import sqlite3

# 读取数据库中的城市组合
class CityCombinationsReader(object):
    """city combinations reader.

    Read the combinations of city from combination database.
    """

    def __init__(self, database_name, city_com_queue):
        self._database_name = database_name
        self._city_com_queue = city_com_queue

    def read_data(self):
        """Initiator of CityCombinationsReader.

        Run the program of reader.
        """

        print('Reader of combinations start...')
        self.__combination_database_reader()
        print('Reader of combinations finished. The city combinations have read in memory.')

    def __combination_database_reader(self):
        """Database Reader.

        Read the data from city combinations database.
        """

        try:
            db_name = '{}.db'.format(self._database_name)
            db_file = os.path.join(os.path.dirname(__file__), db_name)
            connection_of_coms_db = sqlite3.connect(db_file)
        except IOError as no_db_error:
            print('The city combinations database is not existed!')
            print(no_db_error)

        with connection_of_coms_db:
            cur = connection_of_coms_db.cursor()
            city_combinations_data = cur.execute(
                'select * from city_combination').fetchall()
            for data in city_combinations_data:
                self._city_com_queue.put(data)
