"""
The steps I take is:
0 preparation for work including design of data structure to store result of each process,
  establish communication domain and open related file.
1 According to size (byte) of the file, I separate the whole file into 8 parts (for 8 process task).
2 Each process begins to deal with different parts at the same time (including reading file, processing data).
3 After each process finishes their work, the master process (rank==0) gather all results from each processes.
4 Master process calculate the final results, and show it.
"""
import json
import mpi4py.MPI as MPI
import os
import math
import sys
from collections import Counter
'''
This function gets coordinates of each twitts and determine which grid this twitts belongs to.
After decision, this function will call function 'count_hashtags' to process hashtags of this twitts.  
'''


def process_data(x, y, hashtag_list):
    for block_index in range(len(map_handle['features'])):

        block_name = map_handle['features'][block_index]['properties']['id']
        block_type = statistics_table[map_handle['features'][block_index]['properties']['id']]['type']
        left_boundary = map_handle['features'][block_index]['properties']['xmin']
        right_boundary = map_handle['features'][block_index]['properties']['xmax']
        lower_boundary = map_handle['features'][block_index]['properties']['ymin']
        upper_boundary = map_handle['features'][block_index]['properties']['ymax']

        # for determining whether this twitts belong to the corresponding gird.
        if block_type == 1:
            if left_boundary <= x <= right_boundary and lower_boundary <= y <= upper_boundary:
                statistics_table[block_name]['total'] += 1
                count_hashtags(block_index, hashtag_list)

        elif block_type == 2:
            if left_boundary < x <= right_boundary and lower_boundary <= y <= upper_boundary:
                statistics_table[block_name]['total'] += 1
                count_hashtags(block_index, hashtag_list)

        elif block_type == 3:
            if left_boundary <= x <= right_boundary and lower_boundary <= y < upper_boundary:
                statistics_table[block_name]['total'] += 1
                count_hashtags(block_index, hashtag_list)

        elif block_type == 4:
            if left_boundary < x <= right_boundary and lower_boundary <= y < upper_boundary:
                statistics_table[block_name]['total'] += 1
                count_hashtags(block_index, hashtag_list)


'''
This function is for counting and organizing the hashtags. 
It will count the numbers of different hashtags in different grids.
The argument 'index' points the current processed grid, 
'hashtag_list' is a list of hashtag extracted from a twitts.
'''


def count_hashtags(index, hashtag_list):
    block_name = map_handle['features'][index]['properties']['id']

    '''
    processing each hashtag in the list. 
    If this hashtag is already exist in the grid,
        increase the number of that hashtag. 
    Otherwise,
        create a new hashtag in that grid and set the number to 1. 
    '''
    for each in hashtag_list:
        if statistics_table[block_name]['hashtags'].get(each.lower()) is not None:
            statistics_table[block_name]['hashtags'][each.lower()] += 1
        else:
            statistics_table[block_name]['hashtags'][each.lower()] = 1


if __name__ == '__main__':
    # Step 0
    comm = MPI.COMM_WORLD  # establish a communication domain
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()
    twitter_name = sys.argv[1]  # bigTwitter.json
    map_name = sys.argv[2]  # melbGrid.json

    '''
    This table is for storing the result of each process's task. 
    The feature 'type' points out the different treatments we should take to different grids.
    For example, if a twitts's coordinates locates at boundary of A1, then this twitts belong A1.
    That means grid A1 own 4 sides. According to requirements, some grids have 1 side, others have 2 sides.
    So I divide them into different type.  
    '''
    statistics_table = {'A1': {'type': 1, 'total': 0, 'hashtags': {}}, 'A2': {'type': 2, 'total': 0, 'hashtags': {}},
                        'A3': {'type': 2, 'total': 0, 'hashtags': {}},
                        'A4': {'type': 2, 'total': 0, 'hashtags': {}},
                        'B1': {'type': 3, 'total': 0, 'hashtags': {}}, 'B2': {'type': 4, 'total': 0, 'hashtags': {}},
                        'B3': {'type': 4, 'total': 0, 'hashtags': {}},
                        'B4': {'type': 4, 'total': 0, 'hashtags': {}},
                        'C1': {'type': 3, 'total': 0, 'hashtags': {}}, 'C2': {'type': 4, 'total': 0, 'hashtags': {}},
                        'C3': {'type': 4, 'total': 0, 'hashtags': {}},
                        'C4': {'type': 4, 'total': 0, 'hashtags': {}}, 'C5': {'type': 2, 'total': 0, 'hashtags': {}},
                        'D3': {'type': 3, 'total': 0, 'hashtags': {}}, 'D4': {'type': 4, 'total': 0, 'hashtags': {}},
                        'D5': {'type': 4, 'total': 0, 'hashtags': {}}}

    # open map file
    with open(map_name, 'r') as map_handle:
        map_handle = json.load(map_handle)

    # open twitts file, note that in hpc, we need 'utf-8'
    with open(twitter_name, 'r', encoding='utf-8') as twitts_data_handle:
        # Step 1
        file_size = os.path.getsize(twitter_name)
        chunk = math.ceil(file_size / comm_size)
        start, end = comm_rank * chunk, (comm_rank + 1) * chunk
        pointer = start
        twitts_data_handle.seek(start)

        '''
        Because the pointer will point to the middle of twitts, so that we cannot get a valid twitts.
        We have to dispatch the first line and start from the second line we read. But don't worry,
        other processes will completely read the twitts we dispatch.
        '''
        twitts_data_handle.readline()

        # Step 2
        while pointer <= end:
            line = twitts_data_handle.readline()
            pointer = twitts_data_handle.tell()

            if line == ']}\n':  # if we meet ']}\n', that means we reach the end
                break
            elif line.endswith(',\n'):
                line = line[0:len(line) - 2]
            elif line.endswith('\n'):
                line = line[0:len(line) - 1]

            twitts = json.loads(line)

            if twitts.get('doc').get('coordinates'):
                # extract coordinates from twitts
                coordinates_x, coordinates_y = twitts.get('doc').get('coordinates').get('coordinates')
                # extract hashtags
                hashtags = []
                for hashtag in twitts.get('doc').get('entities').get('hashtags'):
                    hashtags.append(hashtag['text'])
                # process them
                process_data(coordinates_x, coordinates_y, hashtags)

    # Step 3, master node gather all results
    combine_data = comm.gather(statistics_table, root=0)

    # Step 4
    if comm_rank == 0:
        # calculate the total of each grid's twitts
        for block_index in statistics_table:
            total = 0
            for each in combine_data:
                total += each[block_index]['total']
            statistics_table[block_index]['total'] = total
        # count the number of hashtag in each grid
        for block_index in statistics_table:
            hashtag = {}
            for each in combine_data:
                hashtag = dict(Counter(each[block_index]['hashtags']) + Counter(hashtag))
            statistics_table[block_index]['hashtags'] = hashtag
        # sort grids
        temp_dict = {}
        for _ in statistics_table:
            temp_dict[_] = statistics_table[_]['total']
        temp_list = sorted(temp_dict.items(), key=lambda x: x[1], reverse=True)
        # sort hashtags and print final result
        for block in temp_list:
            print(block[0], ":", block[1])
            if statistics_table[block[0]]['hashtags']:
                statistics_table[block[0]]['hashtags'] = sorted(
                    statistics_table[block[0]]['hashtags'].items(), key=lambda x: x[1],
                    reverse=True)
                temp_set = []
                for hashtag in statistics_table[block[0]]['hashtags']:
                    if hashtag[1] not in temp_set:
                        temp_set.append(hashtag[1])
                    if len(temp_set) > 5:
                        break
                    print(hashtag)
