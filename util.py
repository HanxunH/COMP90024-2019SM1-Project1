# @Author: hanxunhuang
# @Date:   2019-03-16T20:27:08+11:00
# @Last modified by:   hanxunhuang
# @Last modified time: 2019-03-20T17:25:05+11:00
import json
import collections


class search_result:
    def __init__(self):
        self.grid_id = None
        self.num_of_post = 0
        self.hash_tags = []
        self.cnt = collections.Counter()
        self.top_5_string = ''

    def increment_num_of_post(self):
        self.num_of_post = self.num_of_post + 1

    def add_num_of_post(self, n):
        self.num_of_post = self.num_of_post + n

    def add_hash_tags(self, hash_tags):
        self.hash_tags = self.hash_tags + hash_tags

    def process_result(self):
        for item in self.hash_tags:
            self.cnt[item] = self.cnt[item] + 1

        for item in self.cnt.most_common(5):
            self.top_5_string = self.top_5_string + str(item)

        return


class grid_data:
    def __init__(self, json_data):
        self.type = json_data['type']
        self.id = json_data["properties"]['id']
        # X for Longitude, Y for Latitude
        self.min_longitude = json_data['properties']['xmin']
        self.max_longitude = json_data['properties']['xmax']
        self.min_latitude = json_data['properties']['ymin']
        self.max_latitude = json_data['properties']['ymax']
        self.geometry_type = json_data['geometry']['type']
        self.geometry_coordinates = json_data['geometry']['coordinates'][0]

    def check_if_coordinates_in_grid(self, coordinates):
        '''
        Requirement:
        An individual post can be considered to occur in the box if its geo-location information (the post latitude and
        longitude given by the post coordinates) is within the box identified by the set of coordinates in melbGrid.json. It
        should be noted that the file bigTwitter.json includes many posts that do not have geocodes or they are not in this
        grid, e.g. they are from other parts of Victoria. You should filter/remove these posts since only the posts in the grid
        boxes identified here are of interest. If a tweet occurs exactly on the boundary of two cells in the same row, e.g. A1
        and A2 then assume that the tweet occurs in the left box (A1). If the boundary the tweet occurs exactly on the
        boundary between two cells in the same column, e.g. A1 and B1, then assume that the tweet occurs in the higher box
        (A1).

        Handle the Cornner Cases
        [0] for Latitude
        [1] for Longitude
        Melbourne around 144.963058, -37.813629

        coordinates belong to this grid if
        longitude > min latitude
        longitude <= max latitude A1 A2 Situation

        latitude >= min latitude
        latitude < max latitude
        '''

        latitude = coordinates[0]
        longitude = coordinates[1]
        if (latitude >= self.min_latitude and latitude < self.max_latitude) and (longitude > self.min_longitude and longitude <= self.max_longitude):
            return True
        return False


class twitter_data:
    def __init__(self, json_data):
        self._id = json_data['_id']
        self.id = json_data['id']
        self.text = json_data['text']
        self.geo = None
        self.coordinates = None
        self.hashtags = []
        self.user_location = json_data['user']['location']
        if 'geo' in json_data and json_data['geo'] is not None:
            self.geo = json_data['geo']
            self.coordinates = json_data['geo']['coordinates']
        for item in json_data['entities']['hashtags']:
            self.hashtags.append(('#' + item['text']))


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

        return twitter_data_list
