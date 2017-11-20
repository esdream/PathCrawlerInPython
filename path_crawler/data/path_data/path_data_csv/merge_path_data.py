import os

def merge(file_dir):
    merge_filename = os.path.join(file_dir, 'path_data_all.csv')
    with open(merge_filename, mode='w', encoding='utf-8') as f_merge:
        f_merge.write('id, origin_city, destination_city, origin_region, destination_region, duration_s, distance_km\n')
        for filename in os.listdir(file_dir):
            _, extend = os.path.splitext(filename)
            if(filename != 'path_data_all.csv' and extend == '.csv'):
                with open(filename, mode='r', encoding='utf-8') as f_path_data:
                    path_data = f_path_data.readlines()[1:]
                    f_merge.writelines(path_data)

if(__name__ == '__main__'):
    merge('.')
