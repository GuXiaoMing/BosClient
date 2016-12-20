"""
# @file transferor.py
# @Synopsis  transfer manager
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-11-23
"""
import os
import shutil
import logging
import threading
import Queue
import time
from datetime import datetime

from conf.env_config import EnvConfig
from dao.hdfs import HDFSClient
from dao.hdfs import HDFSError
from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.services.bos.bos_client import BosClient

general_logger = logging.getLogger(EnvConfig.GENERAL_LOG_NAME)
success_logger = logging.getLogger(EnvConfig.SUCCESS_LOG_NAME)
failure_logger = logging.getLogger(EnvConfig.FAILURE_LOG_NAME)

class Transferor(object):
    """
    # @Synopsis  transfer controller
    """
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.transfer_info_list = []
        self.transfer_file_cnt = 0
        self.thread_lock = threading.Lock()
        self.cacheQueue = Queue.Queue()
        self.total_size = 0
        self.processed_size = 0
        self.failure_cnt = 0
        self.download_completed = False

    def tranfer(self, hdfs_path, bos_path):
        """
        # @Synopsis  start a recursive HDFS to BOS transfer, including one download
        # thread and one upload thread. The data is first download to local cache
        # directory then uploaded
        #
        # @Args hdfs_path source hdfs path
        # @Args bos_path destination bos path
        #
        # @Returns   None
        """
        self.transfer_info_list = self.getTransferList(hdfs_path, bos_path)
        self.transfer_file_cnt = len(self.transfer_info_list)
        self.total_size = sum(map(lambda x: x['size'], self.transfer_info_list))
        general_logger.info('start to transfer {0} --> {1}, file_cnt = {2}, total_size = {3:.3f}G'\
                .format(hdfs_path, bos_path, self.transfer_file_cnt,
                    float(self.total_size) / 1024 / 1024 / 1024))
        self.start_time = datetime.now()
        download_thread = DownloadThread(self)
        download_thread.daemon = True
        upload_thread = UploadThread(self)
        upload_thread.daemon = True
        download_thread.start()
        upload_thread.start()
        while threading.active_count() > 1:
            time.sleep(1)

        general_logger.info(('finished transfering {} --> {}, failure_cnt = {}/{}, '
            'see in failure log').format(hdfs_path, bos_path, self.failure_cnt,
                self.transfer_file_cnt))

    def getTransferList(self, hdfs_path, bos_path):
        """
        # @Synopsis  generate all transfer info list, and initialize local
        # cache directory structure to match the hdfs structure
        #
        # @Args hdfs_path
        # @Args bos_path
        #
        # @Returns   transfer info list, each element contains the
        # infomation of the transfer of a single file, including source
        # hdfs file, local cache path, destination bos path and file size
        """
        hdfs_client = HDFSClient(EnvConfig.HADOOP_CLIENT_PATH, EnvConfig.HDFS_LOG_NAME)
        hdfs_objects = hdfs_client.lsr(hdfs_path)
        hdfs_files = filter(lambda x: x['type'] == 'f', hdfs_objects)
        hdfs_file_paths = map(lambda x: x['path'], hdfs_files)
        cache_base_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, 'cache')
        def cache_path_mapper(hdfs_path):
            """
            # @Synopsis  map source hdfs to corresponding dst bos path
            # @Args hdfs_path
            # @Returns   dst bos path
            """
            return os.path.join(cache_base_path, hdfs_path.strip('/'))
        local_cache_paths = map(cache_path_mapper, hdfs_file_paths)

        # initiate local cache path structure
        try:
            shutil.rmtree(cache_base_path)
        except OSError as e:
            pass
        os.mkdir(cache_base_path)
        hdfs_dirs = filter(lambda x: x['type'] == 'd', hdfs_objects)
        hdfs_dir_paths = map(lambda x: x['path'], hdfs_dirs)
        local_dir_paths = map(cache_path_mapper, hdfs_dir_paths)
        local_dir_paths += map(cache_path_mapper, [hdfs_path])
        for dir_path in local_dir_paths:
            try:
                os.makedirs(dir_path)
            except OSError as e:
                print e.message

        relative_paths = map(lambda x: os.path.relpath(x, hdfs_path), hdfs_file_paths)
        bos_paths = map(lambda x: os.path.join(bos_path, x), relative_paths)
        size_list = map(lambda x: x['size'], hdfs_files)
        transfer_info_list = zip(hdfs_file_paths, local_cache_paths, bos_paths, size_list)
        def info_mapper(x):
            """
            # @Synopsis  map tuple info dict
            # @Args x
            # @Returns   info dict
            """
            return dict({
                'hdfs_path': x[0],
                'local_path': x[1],
                'bos_path': x[2],
                'size': x[3]
                })
        transfer_info_list = map(info_mapper, transfer_info_list)
        return transfer_info_list


class DownloadThread(threading.Thread):
    """
    # @Synopsis  download thread, download hdfs file to local cache
    """
    def __init__(self, transferor):
        threading.Thread.__init__(self)
        self.transferor = transferor

    def run(self):
        """
        # @Synopsis  run thread
        # @Returns   None
        """
        hdfs_client = HDFSClient(EnvConfig.HADOOP_CLIENT_PATH, EnvConfig.HDFS_LOG_NAME)
        general_logger.debug('start downloading thread')
        for index, transfer in enumerate(self.transferor.transfer_info_list):
            try:
                hdfs_client.get(transfer['hdfs_path'], transfer['local_path'])
                general_logger.debug('succeeded to download {}/{}: {} --> {}'.format(
                    index + 1, self.transferor.transfer_file_cnt,
                    transfer['hdfs_path'], transfer['local_path']))

                self.transferor.thread_lock.acquire()
                self.transferor.cacheQueue.put(index)
                self.transferor.thread_lock.release()
            except HDFSError as e:
                general_logger.warning('failed to download {}/{}: {} --> {}, message: {}'.format(
                    index + 1, self.transferor.transfer_file_cnt,
                    transfer['hdfs_path'], transfer['local_path'], e.message))
                failure_logger.debug(('{} --> {} --> {} on stage Download, '
                    'message: {}').format(transfer['hdfs_path'], transfer['local_path'],
                        transfer['bos_path'], e.message))

                self.transferor.thread_lock.acquire()
                self.transferor.processed_size += transfer['size']
                self.transferor.failure_cnt += 1
                self.transferor.thread_lock.release()

        self.transferor.download_completed = True
        general_logger.debug('end downloading thread')


class UploadThread(threading.Thread):
    """
    # @Synopsis  upload thread, upload file from local cache to BOS, then delete
    # local cache
    """
    def __init__(self, transferor):
        threading.Thread.__init__(self)
        self.transferor = transferor
        self.bucket_name = self.transferor.bucket_name
        self.initBosClient()

    def initBosClient(self):
        """
        # @Synopsis  initiate BOS client
        # @Returns   None
        """
        bos_host = EnvConfig.BOS_HOST
        access_key_id = EnvConfig.ACCESS_KEY_ID
        secret_access_key = EnvConfig.SECRET_ACCEESS_KEY
        config = BceClientConfiguration(credentials=BceCredentials(access_key_id,
            secret_access_key), endpoint = bos_host)
        config.connection_timeout_in_mills = EnvConfig.BOS_TIMEOUT
        config.recv_buf_size = EnvConfig.BOS_RECV_BUF_SIZE
        config.send_buf_size = EnvConfig.BOS_SEND_BUF_SIZE
        self.bos_client = BosClient(config)

    def run(self):
        """
        # @Synopsis  run thread
        # @Returns   None
        """
        general_logger.debug("start uploading thread")
        while True:
            self.transferor.thread_lock.acquire()
            if not self.transferor.cacheQueue.empty():
                queue_top_index = self.transferor.cacheQueue.get()
                self.transferor.thread_lock.release()

                transfer = self.transferor.transfer_info_list[queue_top_index]
                try:
                    self.bos_client.put_object_from_file(self.bucket_name,
                            transfer['bos_path'], transfer['local_path'])

                    general_logger.debug('succeeded to upload {}/{}: {} --> {}'.format(
                        queue_top_index + 1, self.transferor.transfer_file_cnt,
                        transfer['local_path'], transfer['bos_path']))
                    success_logger.debug('{0} --> {1}'.format(transfer['hdfs_path'],
                        transfer['bos_path']))
                except Exception as e:
                    general_logger.warning('failed to upload {}/{}: {} --> {}, message: {}'.format(
                        queue_top_index + 1, self.transferor.transfer_file_cnt,
                        transfer['local_path'], transfer['bos_path'], e.message))
                    failure_logger.debug(('{} --> {} --> {} on stage Upload, '
                        'message: {}').format(transfer['hdfs_path'], transfer['local_path'],
                            transfer['bos_path'], e.message))
                    self.transferor.thread_lock.acquire()
                    self.transferor.failure_cnt += 1
                    self.transferor.thread_lock.release()

                cur_time = datetime.now()
                time_elapsed = cur_time - self.transferor.start_time
                hour_elapsed = float(time_elapsed.total_seconds()) / 3600
                self.transferor.thread_lock.acquire()
                self.transferor.processed_size += transfer['size']
                processed_size = self.transferor.processed_size
                self.transferor.thread_lock.release()
                estimate_remain_hours = float(self.transferor.total_size -
                        processed_size) / processed_size * hour_elapsed

                general_logger.info(('processed file_cnt {}/{}={:.1f}%, '
                    'file_size {}/{}={:.1f}%, elapsed {:.2f} hours, '
                    'estimate to finish in {:.2f} hours')\
                            .format(queue_top_index + 1, self.transferor.transfer_file_cnt,
                        float(queue_top_index + 1) / self.transferor.transfer_file_cnt * 100,
                        processed_size, self.transferor.total_size,
                        float(processed_size) / self.transferor.total_size * 100,
                        hour_elapsed, estimate_remain_hours))
                os.remove(transfer['local_path'])

            else:
                self.transferor.thread_lock.release()
                if self.transferor.download_completed:
                    return 0
                else:
                    time.sleep(1)
        general_logger.debug("end uploading thread")

if __name__ == '__main__':
    pass
