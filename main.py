# @Author: Hanxun Huang <hanxunh>
# @Date:   2019-03-16T20:48:22+11:00
# @Email:  hanxunh@student.unimelb.edu.au
# @Last modified by:   hanxunhuang
# @Last modified time: 2019-03-17T23:49:47+11:00

import argparse
from util import util, grid_data, twitter_data


# Args Handling
parser = argparse.ArgumentParser(description='COMP90024 Project1')
parser.add_argument('--grid_file_path', type=str, default='data/melbGrid.json')
parser.add_argument('--twitter_data_file_path', type=str, default='data/tinyTwitter_pretty.json')
args = parser.parse_args()


def main():
    grid_data_list = util.load_grid(args.grid_file_path)
    twitter_data_list = util.load_twitter_data(args.twitter_data_file_path)
    return


if __name__ == '__main__':
    main()
