# @Author: hanxunhuang
# @Date:   2019-03-16T20:27:08+11:00
# @Last modified by:   hanxunhuang
# @Last modified time: 2019-04-14T15:58:39+10:00

# class search_result:   stores the search result
# class grid_data:       parse and stores the grid json data
# class twitter_data:    parse and stores the twitter json data
# class util:            handle the load data and search helper function

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

        top = 0
        last = None
        for item in self.cnt.most_common():
            self.top_5_string = self.top_5_string + str(item) + ' '
            _, count = item

            if count != last:
                top = top + 1
                last = count
            if top >= 5:
                break

        if self.top_5_string == '':
            self.top_5_string = None

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
        if (latitude >= self.min_latitude and latitude <= self.max_latitude) and (longitude >= self.min_longitude and longitude <= self.max_longitude):
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
            self.hashtags.append(('#' + item['text'].lower()))
        return


class util:
    def search(grid_data_list, twitter_data_list, logger, rs_dict):
        # Double Check the grid ID matches!
        if len(rs_dict) != len(grid_data_list):
            raise('Incosistent number of grids!')

        for data in twitter_data_list:
            data = util.process_twitter_json(data)
            if data is None:
                continue

            if data.coordinates is not None:
                for grid_data in grid_data_list:
                    if grid_data.check_if_coordinates_in_grid(data.coordinates):
                        # Make Sure The data is in the grid
                        logger.debug('Grid %s, Longtitude Range is [%f, %f], Latitude Range is [%f, %f]' % (grid_data.id, grid_data.min_longitude, grid_data.max_longitude, grid_data.min_latitude, grid_data.max_latitude))
                        logger.debug(data.coordinates)
                        logger.debug(data.user_location)
                        rs_dict[grid_data.id].increment_num_of_post()
                        rs_dict[grid_data.id].add_hash_tags(data.hashtags)
                        break

        return rs_dict

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

    def process_twitter_json(line):
        current_twitter_data_item = None
        if line.startswith('{"id'):
            if line.endswith('},\n'):
                data = json.loads(line[:-2])
            else:
                data = json.loads(line[:-1])
            current_twitter_data_item = twitter_data()
            current_twitter_data_item.id = data['id']
            if 'geo' in data['doc'] and data['doc']['geo'] is not None and 'coordinates' in data['doc']['geo']:
                current_twitter_data_item.coordinates = data['doc']['geo']['coordinates']
            elif 'coordinates' in data['doc'] and data['doc']['coordinates'] is not None and 'coordinates' in data['doc']['coordinates']:
                current_twitter_data_item.coordinates[0] = data['doc']['coordinates']['coordinates'][1]
                current_twitter_data_item.coordinates[1] = data['doc']['coordinates']['coordinates'][0]
            elif 'value' in data and data['value'] is not None and type(data['value']) == type({}):
                print(data['value'])
            current_twitter_data_item.text = data['doc']['text'].lower()
            tokens = current_twitter_data_item.text.split()
            hash_tags_dict = set()
            for item in tokens:
                if item.startswith('#'):
                    hash_tags_dict.add(item)
            current_twitter_data_item.hashtags = list(hash_tags_dict)

        return current_twitter_data_item

    def load_twitter_data(file_path='data/tinyTwitter.json'):
        data_list = []
        with open(file_path) as f:
            for line in f:
                object = util.process_twitter_json(line)
                if object is not None:
                    data_list.append(object)

        return data_list

    def load_twitter_data_and_search(file_path, grid_data_list, logger, rs_dict):
        with open(file_path) as f:
            count = 0
            for line in f:
                data = util.process_twitter_json(line)
                count = count + 1
                if data is not None and data.coordinates is not None:
                    for grid_data in grid_data_list:
                        if grid_data.check_if_coordinates_in_grid(data.coordinates):
                            # Make Sure The data is in the grid
                            logger.debug('Grid %s, Longtitude Range is [%f, %f], Latitude Range is [%f, %f]' % (grid_data.id, grid_data.min_longitude, grid_data.max_longitude, grid_data.min_latitude, grid_data.max_latitude))
                            logger.debug(data.coordinates)
                            logger.debug(data.user_location)
                            rs_dict[grid_data.id].increment_num_of_post()
                            rs_dict[grid_data.id].add_hash_tags(data.hashtags)
                            break
            logger.info('Processed %d lines of Data entries' % (count))
        return rs_dict

    def test_io(file_path, logger):
        with open(file_path) as f:
            count = 0
            for line in f:
                count = count + 1
            logger.info('Processed %d lines of Data entries' % (count))
        return 
