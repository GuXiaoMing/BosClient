"""
# @file upload.py
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
from bll.bos_client import MyBosClient
from datetime import datetime
import logging

if __name__ == '__main__':
    initLogger()
    logger = logging.getLogger(EnvConfig.GENERAL_LOG_NAME)
    start_time = datetime.now()

    parser = argparse.ArgumentParser(description=
            'BOS Client, transfer data between local disk and BOS')
    parser.add_argument('bucket_name', help='destination BOS bucket name')

    subparsers = parser.add_subparsers(title='mode selection', description=('upload local file to '
            'BOS or download BOS file to local disc'), help='choose mode',
            dest='command')

    put_parser = subparsers.add_parser('put', help='put local file/dir to BOS')
    put_parser.add_argument('src', help='source local path')
    put_parser.add_argument('dst', help=('destination bos path. Note that this is the '
        'destination path itself, not its parent path(approximately like '
        '"mv local_path/* bos_path/, except that the bos_path would be created if not exist).'))

    get_parser = subparsers.add_parser('get', help='get file/dir from BOS')
    get_parser.add_argument('src', help='source BOS prefix')
    get_parser.add_argument('dst', help=('destination local path. Note that this is the '
        'destination path itself, not its parent path(approximately like '
        '"mv bos_path/* local_path/, except that the local_path would be created if not exist).'))

    args = parser.parse_args()
    bucket_name = args.bucket_name
    bos_client = MyBosClient(bucket_name)
    if args.command == 'put':
        local_path = args.src
        bos_path = args.dst
        logger.debug('start uploading')
        bos_client.put(local_path, bos_path)
    elif args.command == 'get':
        local_path = args.dst
        bos_path = args.src
        logger.debug('start downloading')
        bos_client.get(bos_path, local_path)

    end_time = datetime.now()
    time_span = end_time - start_time
    minutes = time_span.total_seconds() / 60
    logger.debug('Operation spent {0} minutes'.format(minutes))
