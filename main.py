# @Author: Hanxun Huang <hanxunh>
# @Date:   2019-03-16T20:48:22+11:00
# @Email:  hanxunh@student.unimelb.edu.au
# @Last modified by:   hanxunhuang
# @Last modified time: 2019-04-07T21:46:40+10:00

# ArgumentParser: Check help argument for the specifications
#
# program_timmer_start()
# program_timmer_end()
# Helper functions that set the timmer of the entire proragm
#
# main()
# Process the load data and search base on the number of cores
#
# print_final_result()
# Process and Print The Final Result

import argparse
import logging
import datetime
from mpi4py import MPI
from util import util, search_result


# Args Handling
parser = argparse.ArgumentParser(description='COMP90024 Project1')
parser.add_argument('--grid_file_path', type=str, default='data/melbGrid.json', help='Path to the melbGrid json File')
parser.add_argument('--twitter_data_file_path', type=str, default='data/tinyTwitter.json', help='Path to the twitter data json File')
parser.add_argument('--batch_size', type=int, default=10, help='Number of data in batch for subprocesses to handle')
parser.add_argument('--debug', default=False, help='Set to True if running in Debug Mode')

args = parser.parse_args()

start = None
end = None
logger = None


def program_timmer_start():
    global start, logger
    start = datetime.datetime.now()
    logger.info('Program Starts at: %s' % (str(start)))


def program_timmer_end():
    global start, logger
    end = datetime.datetime.now()
    logger.info('Program Ends at: %s' % (str(end)))
    logger.info('Program Total execution time: %s ' % (str((end-start))))


def main():
    global logger, start, end
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()  # new: gives number of ranks in comm

    # Setup Logger
    extra = {'Process_ID': rank}
    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s-Process-%(Process_ID)s-%(levelname)s-%(message)s')
    logger = logging.getLogger(__name__)
    logger = logging.LoggerAdapter(logger, extra)

    # Boardcast the gird data
    grid_data_list = util.load_grid(args.grid_file_path)
    grid_data_list = comm.bcast(grid_data_list, root=0)

    # Initialze the result dictionary
    rs_dict = {}
    for item in grid_data_list:
        rs = search_result()
        rs.id = item.id
        rs_dict[item.id] = rs

    if size == 1:
        program_timmer_start()
        twitter_data_list = util.load_twitter_data(args.twitter_data_file_path)
        logger.info('Processed %d lines of Data entries' % (len(twitter_data_list)))
        rs_dict = util.search(grid_data_list=grid_data_list, twitter_data_list=twitter_data_list, logger=logger, rs_dict=rs_dict)
        print_final_result(rs_dict)
        program_timmer_end()
    else:
        next_target = 1
        # Root Process handle IO
        if rank == 0:
            program_timmer_start()
            # Process the twitter data and send to other process by batch
            data_list = []
            total_count = 0
            with open(args.twitter_data_file_path) as f:
                for line in f:
                    data_list.append(line)
                    total_count = total_count + 1
                    if len(data_list) >= args.batch_size:
                        comm.send(data_list, dest=next_target, tag=11)
                        next_target = next_target + 1
                        if next_target == size:
                            next_target = 1
                        data_list = []
                if len(data_list) > 0:
                    comm.send(data_list, dest=next_target, tag=11)
                for index in range(1, size):
                    comm.send([], dest=index, tag=11)
            end = datetime.datetime.now()
            logger.info('Total Number of Cores: %d' % (size))
            logger.info('Sent total of %d lines of Data entries' % (total_count))
            logger.info('IO takes: %s ' % (str((end-start))))
            rs_dict = None
        else:
            # Other Child Process Handles the search
            total_count = 0
            while True:
                data = comm.recv(source=0, tag=11)
                logger.debug('Processing of %d lines of data' % (len(data)))
                rs_dict = util.search(grid_data_list=grid_data_list, twitter_data_list=data, logger=logger, rs_dict=rs_dict, process_data=True)
                if len(data) == 0:
                    break
                total_count = total_count + len(data)
            logger.info('Recv Total of %d lines of data' % (total_count))

        # Gather the result
        result_list = comm.gather(rs_dict, root=0)
        if rank == 0:
            # Merge the results into singal dintionary
            print_final_result(result_list, process_gather=True)
            program_timmer_end()
    return


def print_final_result(result_list, process_gather=False):
    if process_gather:
        final_result = {}
        for result_dict_form_rank in result_list:
            if result_dict_form_rank is None:
                continue
            for grid_id in result_dict_form_rank:
                if grid_id not in final_result:
                    rs = search_result()
                    rs.id = grid_id
                    final_result[grid_id] = rs
                final_result[grid_id].add_hash_tags(result_dict_form_rank[grid_id].hash_tags)
                final_result[grid_id].add_num_of_post(result_dict_form_rank[grid_id].num_of_post)
    else:
        final_result = result_list
    # Process The Result
    for grid_id in final_result:
        final_result[grid_id].process_result()
    final_result = [v for v in final_result.values()]
    final_result = sorted(final_result, key=lambda x: x.num_of_post, reverse=True)
    # Print total number of Twitter posts
    print(('=' * 30) + ' total number of Twitter posts' + ('=' * 30))
    for item in final_result:
        if item.top_5_string is not None:
            disply = '{:1s}: {:6d} posts - {:10s}'.format(item.id, item.num_of_post, item.top_5_string)
        else:
            disply = '{:1s}: {:6d} posts - None'.format(item.id, item.num_of_post)
        print(disply)


if __name__ == '__main__':
    main()
