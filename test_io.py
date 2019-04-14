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

    program_timmer_start()
    util.test_io(args.twitter_data_file_path, logger)
    end = datetime.datetime.now()
    logger.info('IO takes: %s ' % (str((end-start))))

if __name__ == '__main__':
    main()
