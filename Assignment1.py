"""
Author   : Jing DU     775074   du2@student.unimelb.edu.au
           Zhijia LU   921715   zhijial@student.unimelb.edu.au
Project  : COMP 90024 - Assignment 1 HPC Twitter GeoProcessing
Purpose  : The purpose in this programming assignment is to implement
           a simple, parallelized application leveraging the University
           of Melbourne HPC facility SPARTAN
"""
import json
import re
from mpi4py import MPI

# Read melbGrid.json and load the id and rang of each grid into a dictionary list
# Each dictionary is used to represent a grid
# ['count'] stores the number of twitters in this grid.
# ['tag_list'] store the tags and the number of tags in this grid
class Grid:
    def __init__(self, data):
        self.num = len(data)
        self.lists = []
        for i in range(self.num):
            grid = {}
            grid['id'] = data[i]['properties']['id']
            grid['x_min'] = data[i]['properties']['xmin']
            grid['x_max'] = data[i]['properties']['xmax']
            grid['y_min'] = data[i]['properties']['ymin']
            grid['y_max'] = data[i]['properties']['ymax']
            # slightly modify the margins for the belongings of nodes on boundaries
            if grid['id'] in ['A1', 'B1', 'C1', 'D3']:
                grid['x_min'] -= 0.000001
            if grid['id'] in ['A1', 'A2', 'A3', 'A4', 'C5']:
                grid['y_max'] += 0.000001
            grid['count'] = 0
            grid['tags'] = {}
            self.lists.append(grid)


# Defining the data structure of each twitter
# Initialing each attributions
class Twitter:
    def __init__(self, twitter):
        # Coordinates can be found from more than one place
        # 1.doc-coordinates-coordinates
        if twitter.get('doc'):
            if twitter['doc'].get('coordinates'):
                if twitter['doc']['coordinates'].get('coordinates'):
                    self.x = twitter['doc']['coordinates']['coordinates'][0]
                    self.y = twitter['doc']['coordinates']['coordinates'][1]
                else:
                    self.x = 0.0
                    self.y = 0.0
            else:
                self.x = 0.0
                self.y = 0.0

            if twitter['doc'].get('text'):
                self.text = twitter['doc']['text'].lower()
            else:
                self.text = " "

        else:
            self.x = 0.0
            self.y = 0.0
            self.text = " "
        # If no coordinate pair is found from doc-coordinaes-coordinaates,
        # geo-coordinates will be search for updating coordinate pair
        if self.x == 0.0:
            if twitter.get('geo'):
                if twitter['geo'].get('coordinates'):
                    self.x = twitter['geo']['coordinates'][0]
                    self.y = twitter['geo']['coordinates'][1]

# Driving mpi4py module and getting current MPI state
class Mpi:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()


# Utility class provides helpful functions to deliver data
class Utility:
    # Loading name and margins of each grid
    def load_grids(self):
        with open("melbGrid.json", 'r', encoding='utf-8') as load_f1:
            melb_grid = json.load(load_f1)
            grid = Grid(melb_grid['features'])
        return grid

    # Different cores committing their own work in turn
    # read the twitters line by line and determine which grid the current twitter belongs to
    # tags in each twitter text also need to be extracted and update grid ['tag_list']
    def get_posts_num(self, mpi):
        line_index = 0
        with open("bigTwitter.json", 'r', encoding='utf-8') as load_f2:
            for twitter_l in load_f2:
                line_index += 1
                if mpi.rank == line_index % mpi.size:
                    try:
                        # Line structures are different between the last line and others
                        if twitter_l[-2:-1] == ',':
                            twitter_d = json.loads(twitter_l[:-2])
                        else:
                            twitter_d = json.loads(twitter_l[:-1])
                    # Throwing exception(Reading the next line directly) when No Json structure is unmarshalled
                    except Exception:
                        continue
                    twitter = Twitter(twitter_d)
                    for i in range(len(grid.lists)):
                        if grid.lists[i]['x_min'] < twitter.x <= grid.lists[i]['x_max'] and grid.lists[i]['y_min'] <= \
                                twitter.y < grid.lists[i]['y_max']:
                            grid.lists[i]['count'] += 1
                            # Hash tags are defined as with the structure of'<Space><String><Space>'
                            tag1 = re.findall(r'\s#\w+', twitter.text)
                            tag2 = re.findall(r'#\w+\s', twitter.text)
                            tag3 = [t[1:] for t in tag1]
                            tag4 = [t[:-1] for t in tag2]
                            tag = list(set(tag3).intersection(tag4))
                            for k in range(len(tag)):
                                if tag[k] in grid.lists[i]['tags'].keys():
                                    grid.lists[i]['tags'][tag[k]] += 1
                                else:
                                    grid.lists[i]['tags'][tag[k]] = 1
        return grid.lists

    # Rank the grid based on the total number of twitters in each grid
    # print each grid id and the total number of twitters in this grid
    def rank_boxes(self,grid_items):
        grid_items = sorted(grid_items, key=lambda e: e.__getitem__('count'), reverse=True)
        for i in range(len(grid_items) - 1):
            print("{0}{1} {2} posts,".format(grid_items[i]['id'], ":", grid_items[i]['count']))

        print("{0}{1} {2} posts".format(grid_items[i + 1]['id'], ":", grid_items[i + 1]['count']))
        return grid_items

    # Rank the tag in each grid based on the number of times tag appears this grid
    # print the top 5 tags in each Grid cells based on the number of occurrences of those tags
    def rank_tags(self,grid_dict):

        for i in range(len(grid_dict)):

            most_com = grid_dict[i]['tags']
            flag = 0
            num = 0
            # No hash tag gotten
            if len(most_com) == 0:
                print(grid_dict[i]['id'] + ": ")
            else:
                print(grid_dict[i]['id'] + ": (", end='')
                output_string = ''
                for j in range(len(most_com)):
                    num = num + 1
                    if num == 5:
                        flag = most_com[j][1]
                    # End loop when no tie rank appears after first five output
                    if num > 5 and most_com[j][1] < flag:
                        break
                    s = str(most_com[j])
                    s = s.replace('\'', '')
                    s = s + ','
                    output_string += s
                # Formatting output
                output_string = output_string[:-1]+')'
                print(output_string)

    # Union of values of two dictionaries merged by key
    def union_dict(self,dict1, dict2):
        for i, j in dict2.items():
            if i in dict1.keys():
                dict1[i] += j
            else:
                dict1[i] = dict2[i]
        return dict1


if __name__ == '__main__':
    mpi = Mpi()
    ut = Utility()

    grid = None
    if mpi.rank == 0:
        grid = ut.load_grids()

    # Broadcast the grid coordinates to every core.
    grid = mpi.comm.bcast(grid, root=0)

    sub_classify = ut.get_posts_num(mpi)

    # After each core doing their jobs simultaneously then the MPI will gather all the results together.
    classify_list = mpi.comm.gather(sub_classify, root=0)
    if mpi.rank == 0:
        twitter_classify = []
        for i in range(len(grid.lists)):
            count_sum = 0
            tag_list = {}
            for j in range(mpi.size):
                count_sum += classify_list[j][i]['count']
                tag_list = ut.union_dict(tag_list, classify_list[j][i]['tags'], )

            tags = sorted(tag_list.items(), key=lambda item: item[1], reverse=True)
            twi_item = {'id': classify_list[j][i]['id'], 'count': count_sum, 'tags': tags}

            twitter_classify.append(twi_item)

        twitter_classify = ut.rank_boxes(twitter_classify)
        ut.rank_tags(twitter_classify)

