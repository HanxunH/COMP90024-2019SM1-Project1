# @Author: hanxunhuang
# @Date:   2019-03-16T20:27:08+11:00
# @Last modified by:   hanxunhuang
# @Last modified time: 2019-03-18T17:22:55+11:00
import json


class search_result:
    def __init__(self):
        self.grid_id = None
        self.num_of_post = 0
        self.hash_tags = []


class grid_data:
    def __init__(self, json_data):
        self.type = json_data['type']
        self.id = json_data["properties"]['id']
        self.xmin = json_data['properties']['xmin']
        self.xmax = json_data['properties']['xmax']
        self.ymin = json_data['properties']['ymin']
        self.ymax = json_data['properties']['ymax']
        self.geometry_type = json_data['geometry']['type']
        self.geometry_coordinates = json_data['geometry']['coordinates'][0]


class twitter_data:
    def __init__(self, json_data):
        self._id = json_data['_id']
        self.id = json_data['id']
        self.text = json_data['text']
        self.geo = None
        if 'geo' in json_data:
            self.geo = json_data['geo']
        self.hashtags = json_data['entities']['hashtags']


class util:
    def load_grid(file_path='data/melbGrid.json'):
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Check File Type
        if 'type' not in data or data['type'] != 'FeatureCollection':
            raise('Incosistent File')

        grid_list = []
        for item in data['features']:
            grid_list.append(grid_data(item))
        return grid_list

    def load_twitter_data(file_path='data/tinyTwitter.json'):
        with open(file_path, 'r') as f:
            data = json.load(f)

        twitter_data_list = []
        for item in data:
            twitter_data_list.append(twitter_data(item))

        return data
