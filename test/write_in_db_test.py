# _*_ coding: utf-8 _*_

"""Test sql-write operation duration.
"""

import os
import time
import threading
import queue
import sqlite3
import requests


class WriteThread(threading.Thread):

    def __init__(self, **write_args):
        threading.Thread.__init__(self)
        self._db_name = write_args['db_name']
        self._data_queue = write_args['data_queue']
        self._write_func = {
            'per_thread_write': self.__per_thread_write,
            'batch_write': self.__batch_write
        }
        self._data_batch = write_args['data_batch']
        self._run_func = self._write_func[write_args['write_func']]

    def run(self):
        print('Writer Start...')
        self._run_func()
        print('Writer finished.')

    def __per_thread_write(self):
        data_db = sqlite3.connect(self._db_name)

        with data_db:

            global PARSER_EXIT_FLAG
            while not PARSER_EXIT_FLAG:
                cursor = data_db.cursor()

                try:
                    path_info = self._data_queue.get(True, 10)
                    print(path_info)
                    cursor.execute('insert into path_data values (?,?,?,?,?)', path_info)
                    data_db.commit()
                    self._data_queue.task_done()
                except Exception as write_error:
                    print(write_error)

    def __batch_write(self):
        global PARSER_EXIT_FLAG
        while not PARSER_EXIT_FLAG:
            try:
                path_info = self._data_queue.get(True, 10)
                print(path_info)

                self._data_batch.append(path_info)
                if(len(self._data_batch) == 200):
                    conn = sqlite3.connect(self._db_name)
                    with conn:
                        cursor = conn.cursor()
                        cursor.executemany('insert into path_data values (?,?,?,?,?)', self._data_batch)
                        conn.commit()
                        self._data_batch[:] = []

                self._data_queue.task_done()
            except Exception as write_error:
                print(write_error)



DATA_QUEUE = queue.Queue()
PARSER_EXIT_FLAG = False

def main():
    origin_database = 'path_data.db'
    connection = sqlite3.connect(origin_database)
    with connection:
        cursor = connection.cursor()
        data = cursor.execute('select id, origin_city, destination_city, duration, distance from path_data').fetchall()
        for data_unit in data:
            DATA_QUEUE.put(data_unit)

    print(DATA_QUEUE.qsize())

    if(os.path.isfile('new_db.db')):
        os.remove('new_db.db')
    new_db = sqlite3.connect('new_db.db')

    start = time.time()
    data_batch = []
    with new_db:
        cursor = new_db.cursor()
        cursor.execute('create table path_data(id int primary key, origin_city varchar(20), destination_city varchar(20), duration double, distance double)')

        write_thread = WriteThread(db_name='new_db.db', data_queue=DATA_QUEUE, write_func='batch_write', data_batch=data_batch)
        write_thread.start()

        while not DATA_QUEUE.empty():
            pass

        global PARSER_EXIT_FLAG
        PARSER_EXIT_FLAG = True

        write_thread.join()

        cursor.executemany('insert into path_data values (?,?,?,?,?)', data_batch)
        new_db.commit()
        data_batch[:] = []

    end = time.time()

    print('Exiting Main Thread')
    print('Time used: {}'.format(end - start))

if(__name__ == '__main__'):
    main()
