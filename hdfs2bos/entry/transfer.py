"""
# @file transfer.py
# @Synopsis  program entry
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-11-23
"""
import sys
import argparse

sys.path.append('..')
from conf.env_config import EnvConfig
from conf.init_logger import initLogger
from bll.transferor import Transferor
from datetime import datetime
import logging

if __name__ == '__main__':
    initLogger()
    logger = logging.getLogger(EnvConfig.GENERAL_LOG_NAME)
    start_time = datetime.now()

    parser = argparse.ArgumentParser(description='Transfer data from HDFS to BOS')
    parser.add_argument('bucket_name', help='destination BOS bucket name')

    subparsers = parser.add_subparsers(title='mode selection', description=('make single transfer '
            'by specifying src and dst as args in command line mode or make multiple transfers by '
            'specifying input file in file mode'), help='choose mode',
            dest='command')

    commandline_parser = subparsers.add_parser('line', help='command line mode')
    commandline_parser.add_argument('src', help='source hdfs path')
    commandline_parser.add_argument('dst', help=('destination bos path. Note that this is the '
        'destination path itself, not its parent path(approximately like '
        '"mv hdfs_path/* bos_path/, except that the bos_path would be created if not exist).'))

    file_parser = subparsers.add_parser('file', help='input file mode')
    file_parser.add_argument('file', help=('input file, contains lines of hdfs_path\\tbos_path. '
        'Note that the bos_path is the destination path itself, not the parent path of it('
        'approximately like "mv hdfs_path/* bos_path/", except that bos_path would be created if '
        'not exist).'))

    args = parser.parse_args()
    bucket_name = args.bucket_name
    if args.command == 'line':
        transferor = Transferor(bucket_name)
        hdfs_path = args.src
        bos_path = args.dst
        logger.debug('start transferring')
        transferor.tranfer(hdfs_path, bos_path)
    else:
        try:
            input_obj = open(args.file)
            src_dst_list = map(lambda l: l.strip().split(), input_obj)
            input_obj.close()
        except Exception as e:
            logger.critical('failed to read input file')
            exit(1)
        logger.debug('start transferring')
        for src, dst in src_dst_list:
            transferor = Transferor(bucket_name)
            transferor.tranfer(src, dst)

    end_time = datetime.now()
    time_span = end_time - start_time
    minutes = time_span.total_seconds() / 60
    logger.debug('transfer spent {0} minutes'.format(minutes))
