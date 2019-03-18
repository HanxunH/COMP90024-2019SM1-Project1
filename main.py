# @Author: Hanxun Huang <hanxunh>
# @Date:   2019-03-16T20:48:22+11:00
# @Email:  hanxunh@student.unimelb.edu.au
# @Last modified by:   hanxunhuang
# @Last modified time: 2019-03-18T18:45:29+11:00

import argparse
import logging
from mpi4py import MPI
from util import util, search_result


# Args Handling
parser = argparse.ArgumentParser(description='COMP90024 Project1')
parser.add_argument('--grid_file_path', type=str, default='data/melbGrid.json')
parser.add_argument('--twitter_data_file_path', type=str, default='data/tinyTwitter_pretty.json')
args = parser.parse_args()


def search(grid_data_list, twitter_data_list):
    # TODO: Search the batch of data and return result_data list

    return


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()  # new: gives number of ranks in comm

    # Setup Logger
    extra = {'Process_ID': rank}
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s-Process-%(Process_ID)s-%(levelname)s-%(message)s')
    logger = logging.getLogger(__name__)
    logger = logging.LoggerAdapter(logger, extra)

    # Root Process handle IO
    if rank == 0:
        grid_data_list = util.load_grid(args.grid_file_path)
        twitter_data_list = util.load_twitter_data(args.twitter_data_file_path)
        logger.info('Total Number of Cores: %d' % (size))
        logger.info('Total of %d Twitter Data entries before scattering' % (len(twitter_data_list)))

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

    # TODO: Search Function
    rs_dict = {}
    for item in grid_data_list:
        rs = search_result()
        rs.id = item.id
        rs_dict[item.id] = rs

    # Root Process handle the gathering
    data = comm.gather(twitter_data_list, root=0)
    result = comm.gather(rs_dict, root=0)

    if rank == 0:
        twitter_data_list = []
        for item in data:
            twitter_data_list = twitter_data_list + item
        logger.info('Total of %d Twitter Data entries after gathering' % (len(twitter_data_list)))
        # TODO: Handle gathering the search result

    return


if __name__ == '__main__':
    main()
