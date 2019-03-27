# @Author: hanxunhuang
# @Date:   2019-03-16T20:27:08+11:00
# @Last modified by:   hanxunhuang
# @Last modified time: 2019-03-27T18:34:43+11:00
import json
import ijson
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
    def __init__(self, json_data=None):
        self.json_data = json_data
        if self.json_data is not None:
            self.process_json_data()

    def process_json_data(self):
        self.type = self.json_data['type']
        self.id = self.json_data["properties"]['id']
        # X for Longitude, Y for Latitude
        self.min_longitude = self.json_data['properties']['xmin']
        self.max_longitude = self.json_data['properties']['xmax']
        self.min_latitude = self.json_data['properties']['ymin']
        self.max_latitude = self.json_data['properties']['ymax']
        self.geometry_type = self.json_data['geometry']['type']
        self.geometry_coordinates = self.json_data['geometry']['coordinates'][0]

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
    def __init__(self, json_data=None):
        self.json_data = json_data
        self._id = None
        self.id = None
        self.text = None
        self.geo = None
        self.coordinates = None
        self.hashtags = []
        self.user_location = None
        if self.json_data is not None:
            self.process_json_data()

    def process_json_data(self):
        self._id = self.json_data['_id']
        self.id = self.json_data['id']
        self.text = self.json_data['text']
        self.geo = None
        self.coordinates = None
        self.hashtags = []
        self.user_location = self.json_data['user']['location']
        if 'geo' in self.json_data and self.json_data['geo'] is not None:
            self.geo = self.json_data['geo']
            self.coordinates = self.json_data['geo']['coordinates']
        for item in self.json_data['entities']['hashtags']:
            self.hashtags.append(('#' + item['text']))
        return


class util:
    def load_twitter_with_ijson(file_path='data/tinyTwitter.json'):
        twitter_data_dict = {}
        parser = ijson.parse(open(file_path, 'r'))
        current_twitter_data_item = None
        for prefix, event, value in parser:
            # print(prefix, event, value)
            if prefix == 'rows.item.doc._id':
                current_twitter_data_item = twitter_data()
                current_twitter_data_item._id = value
                twitter_data_dict[current_twitter_data_item._id] = current_twitter_data_item
            elif prefix == 'rows.item.doc.text':
                twitter_data_dict[current_twitter_data_item._id].text = value
            elif prefix == 'rows.item.doc.entities.hashtags.item.text':
                twitter_data_dict[current_twitter_data_item._id].hashtags.append(('#' + value))
            elif prefix == 'rows.item.doc.geo.coordinates.item':
                if twitter_data_dict[current_twitter_data_item._id].coordinates is None:
                    twitter_data_dict[current_twitter_data_item._id].coordinates = []
                twitter_data_dict[current_twitter_data_item._id].coordinates.append(value)
        return twitter_data_dict

    def load_grid_with_ijson(file_path='data/melbGrid.json'):
        grid_dict = {}
        parser = ijson.parse(open(file_path, 'r'))
        current_grid_data_item = None
        for prefix, event, value in parser:
            if prefix == 'features.item.properties.id':
                current_grid_data_item = grid_data()
                current_grid_data_item.id = value
                grid_dict[current_grid_data_item.id] = current_grid_data_item
            elif prefix == 'features.item.properties.xmin':
                grid_dict[current_grid_data_item.id].min_longitude = value
            elif prefix == 'features.item.properties.xmax':
                grid_dict[current_grid_data_item.id].max_longitude = value
            elif prefix == 'features.item.properties.ymin':
                grid_dict[current_grid_data_item.id].min_latitude = value
            elif prefix == 'features.item.properties.ymax':
                grid_dict[current_grid_data_item.id].max_latitude = value
            elif prefix == 'features.item.geometry.type':
                grid_dict[current_grid_data_item.id].geometry_type = value
            elif prefix == 'features.item.geometry.coordinates':
                grid_dict[current_grid_data_item.id].geometry_coordinates = value
        return grid_dict

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
