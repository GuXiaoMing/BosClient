"""
##
# @file env_config.py
# @Synopsis  config environment
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-07
"""
import os
import ConfigParser


class EnvConfig(object):
    """
    # @Synopsis  config environment
    """
    DEBUG = True
    SMS_RECEIVERS = ['18612861842']
    MAIL_RECEIVERS = ['guming@itv.baidu.com']

    CONF_DIR_PATH = os.path.split(os.path.realpath(__file__))[0]
    PROJECT_PATH = os.path.join(CONF_DIR_PATH, '../')
    CONF_FILE_PATH = os.path.join(CONF_DIR_PATH, 'all.cfg')
    LOCAL_DATA_PATH = os.path.join(PROJECT_PATH, 'data/')

    config = ConfigParser.RawConfigParser()
    config.read(CONF_FILE_PATH)

    HADOOP_CLIENT_PATH = config.get('HDFS', 'client_path')

    BOS_HOST = config.get('BOS', 'host')
    ACCESS_KEY_ID = config.get('BOS', 'access_key_id')
    SECRET_ACCEESS_KEY = config.get('BOS', 'secret_access_key')
    BUCKET_NAME = config.get('BOS', 'bucket_name')
    BOS_TIMEOUT = config.getint('BOS', 'connection_timeout_in_mills')
    BOS_SEND_BUF_SIZE = config.getint('BOS', 'send_buf_size')
    BOS_RECV_BUF_SIZE = config.getint('BOS', 'recv_buf_size')

    GENERAL_LOG_NAME = 'general'
    SUCCESS_LOG_NAME = 'success'
    FAILURE_LOG_NAME = 'failure'
    BOS_LOG_NAME = 'baidubce.services.bos.bosclient'
    HDFS_LOG_NAME = 'hdfs'

    LOG_ROTATE_DAY = config.getint('LOG', 'rotate_day')
    ALARM_RECEIVERS = config.get('LOG', 'alarm_receivers').split(',')

    GENERAL_LOG_FILE_PATH = os.path.join(PROJECT_PATH, 'log', 'general.log')
    SUCCESS_LOG_FILE_PATH = os.path.join(PROJECT_PATH, 'log', 'success.log')
    FAILURE_LOG_FILE_PATH = os.path.join(PROJECT_PATH, 'log', 'failure.log')
    BOS_LOG_FILE_PATH = os.path.join(PROJECT_PATH, 'log', 'bos.log')
    HDFS_LOG_FILE_PATH = os.path.join(PROJECT_PATH, 'log', 'hdfs.log')

if __name__ == '__main__':
    pass
