# @Author: Hanxun Huang <hanxunh>
# @Date:   2019-03-16T20:48:22+11:00
# @Email:  hanxunh@student.unimelb.edu.au
# @Last modified by:   hanxunhuang
# @Last modified time: 2019-03-20T17:51:07+11:00

import argparse
import logging
import datetime
from mpi4py import MPI
from util import util, search_result


# Args Handling
parser = argparse.ArgumentParser(description='COMP90024 Project1')
parser.add_argument('--grid_file_path', type=str, default='data/melbGrid.json')
parser.add_argument('--twitter_data_file_path', type=str, default='data/tinyTwitter_pretty.json')
parser.add_argument('--data_pre_processing', default=True)
args = parser.parse_args()


def search(grid_data_list, twitter_data_list, logger):
    rs_dict = {}
    for item in grid_data_list:
        rs = search_result()
        rs.id = item.id
        rs_dict[item.id] = rs

    # Double Check the grid ID matches!
    if len(rs_dict) != len(grid_data_list):
        raise('Incosistent number of grids!')

    for twitter_data in twitter_data_list:
        if twitter_data.coordinates is not None:
            for grid_data in grid_data_list:
                if grid_data.check_if_coordinates_in_grid(twitter_data.coordinates):
                    # Make Sure The data is in the grid
                    logger.debug('Grid %s, Longtitude Range is [%f, %f], Latitude Range is [%f, %f]' % (grid_data.id, grid_data.min_longitude, grid_data.max_longitude, grid_data.min_latitude, grid_data.max_latitude))
                    logger.debug(twitter_data.coordinates)
                    logger.debug(twitter_data.user_location)
                    rs_dict[grid_data.id].increment_num_of_post()
                    rs_dict[grid_data.id].add_hash_tags(twitter_data.hashtags)

    return rs_dict


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()  # new: gives number of ranks in comm

    # Setup Logger
    extra = {'Process_ID': rank}
    logging.basicConfig(level=logging.INFO, format='%(asctime)s-Process-%(Process_ID)s-%(levelname)s-%(message)s')
    logger = logging.getLogger(__name__)
    logger = logging.LoggerAdapter(logger, extra)

    # Root Process handle IO
    if rank == 0:
        start = datetime.datetime.now()
        grid_data_list = util.load_grid(args.grid_file_path)
        twitter_data_list = util.load_twitter_data(args.twitter_data_file_path)
        # grid_data_dict = util.load_grid_with_ijson(args.grid_file_path)
        end = datetime.datetime.now()
        logger.info('Total Number of Cores: %d' % (size))
        logger.info('Total of %d Twitter Data entries before scattering' % (len(twitter_data_list)))
        logger.info('IO takes: %s ' % (str((end-start))))
        chunks = [[] for _ in range(size)]
        for i, chunk in enumerate(twitter_data_list):
            chunks[i % size].append(chunk)
    else:
        chunks = None
        twitter_data_list = None
        grid_data_list = None

    # Concurrent Handling
    grid_data_list = comm.bcast(grid_data_list, root=0)
    twitter_data_list = comm.scatter(chunks, root=0)

    logger.info('Handling %d Twitter Data entries, %d grids' % (len(twitter_data_list), len(grid_data_list)))

    start = datetime.datetime.now()
    rs_dict = search(grid_data_list=grid_data_list, twitter_data_list=twitter_data_list, logger=logger)
    end = datetime.datetime.now()
    logger.info('Search takes: %s ' % (str((end-start))))

    # Root Process handle the gathering
    data = comm.gather(twitter_data_list, root=0)
    result_list = comm.gather(rs_dict, root=0)
    final_result = {}
    for item in grid_data_list:
        rs = search_result()
        rs.id = item.id
        final_result[item.id] = rs

    if rank == 0:
        twitter_data_list = []
        for item in data:
            twitter_data_list = twitter_data_list + item
        logger.info('Total of %d Twitter Data entries after gathering' % (len(twitter_data_list)))
        for result_dict_form_rank in result_list:
            for grid_id in result_dict_form_rank:
                final_result[grid_id].add_hash_tags(result_dict_form_rank[grid_id].hash_tags)
                final_result[grid_id].add_num_of_post(result_dict_form_rank[grid_id].num_of_post)

        # Process The Result
        for grid_id in final_result:
            final_result[grid_id].process_result()

        print(('=' * 30) + ' Final Result ' + ('=' * 30))

        # Print total number of Twitter posts
        print(('=' * 30) + ' total number of Twitter posts' + ('=' * 30))
        for grid_id in final_result:
            print('%s: %d posts' % (final_result[grid_id].id, final_result[grid_id].num_of_post))

        # Print top5 hashtags number of Twitter posts
        print(('=' * 30) + ' Top 5 Hashtags' + ('=' * 30))
        for grid_id in final_result:
            if len(final_result[grid_id].top_5_string) > 0:
                print('%s: (%s) ' % (final_result[grid_id].id, final_result[grid_id].top_5_string))
            else:
                print('%s: None ' % (final_result[grid_id].id))

    return


if __name__ == '__main__':
    main()
