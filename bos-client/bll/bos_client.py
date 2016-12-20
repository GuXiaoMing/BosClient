"""
# @Synopsis  my bos client
"""
import os
import logging

from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.services.bos.bos_client import BosClient

from conf.env_config import EnvConfig
from bll.transferor import Uploader
from bll.transferor import Downloader

class MyBosClient(object):
    """
    # @Synopsis  should inherent BosClient by instinct, however, BosClient
    # prohibited inheritance by relying on self.__module__
    """
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.bos_client = self.initBosClient()

    def initBosClient(self):
        """
        # @Synopsis  init baidu bos client
        # @Returns   initiated bos client
        """
        bos_host = EnvConfig.BOS_HOST
        access_key_id = EnvConfig.ACCESS_KEY_ID
        secret_access_key = EnvConfig.SECRET_ACCEESS_KEY
        config = BceClientConfiguration(credentials=BceCredentials(access_key_id,
            secret_access_key), endpoint = bos_host)
        config.connection_timeout_in_mills = EnvConfig.BOS_TIMEOUT
        config.recv_buf_size = EnvConfig.BOS_RECV_BUF_SIZE
        config.send_buf_size = EnvConfig.BOS_SEND_BUF_SIZE
        return BosClient(config)

    def put(self, local_path, bos_path):
        """
        # @Synopsis  put local file to bos
        # @Args local_path
        # @Args bos_path
        # @Returns   None
        """
        uploader = Uploader(self)
        uploader.transfer(local_path, bos_path)

    def get(self, bos_path, local_path):
        """
        # @Synopsis  get bos file to local
        # @Args bos_path
        # @Args local_path
        # @Returns   None
        """
        downloader = Downloader(self)
        downloader.transfer(bos_path, local_path)

    def lsr(self, path):
        """
        # @Synopsis  list all object in bos with the given prefix
        # @Args path
        # @Returns   list of object keys
        """
        MAX_KEY_CNT = 1000
        file_list = []
        marker = None
        while True:
            response = self.bos_client.list_objects(self.bucket_name,
                    prefix=path, marker=marker)
            keys = map(lambda x: x.key.encode('utf8'), response.contents)
            file_list += keys
            if len(keys) < MAX_KEY_CNT:
                break
            marker = keys[-1]
        return file_list

    def put_object_from_file(self, src_file, dst_file):
        """
        # @Synopsis  single file put method
        # @Args src_file
        # @Args dst_file
        # @Returns   None
        """
        self.bos_client.put_object_from_file(self.bucket_name, dst_file, src_file)

    def get_object_to_file(self, src_file, dst_file):
        """
        # @Synopsis  single file get method
        # @Args src_file
        # @Args dst_file
        # @Returns   None
        """
        self.bos_client.get_object_to_file(self.bucket_name, src_file, dst_file)
