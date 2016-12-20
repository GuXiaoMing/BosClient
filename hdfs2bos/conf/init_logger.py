"""
# @file init_logger.py
# @Synopsis  init logger
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-19
"""
import sys
import logging
import logging.handlers
from conf.env_config import EnvConfig
from dao.mail import MailClient

class MailHandler(logging.Handler):
    """
    # @Synopsis  customized handler, to email critical log
    """

    def emit(self, record):
        """
        # @Synopsis  override logging.Handler emit method, the action when receive
        # the logging record
        #
        # @Args record
        #
        # @Returns nothing
        """
        msg = self.format(record)
        mail_client = MailClient(EnvConfig.GENERAL_LOG_NAME)
        mail_client.send(EnvConfig.ALARM_RECEIVERS, 'PROGRAM ALARM', msg)


def initLogger():
    """
    # @Synopsis  initialize logger
    # @Returns   None
    """
    general_logger = logging.getLogger(EnvConfig.GENERAL_LOG_NAME)
    general_logger.setLevel(logging.DEBUG)
    file_hdlr = logging.handlers.TimedRotatingFileHandler(
            EnvConfig.GENERAL_LOG_FILE_PATH, when='D', backupCount=EnvConfig.LOG_ROTATE_DAY)
    stdout_hdler = logging.StreamHandler(sys.stdout)
    stdout_hdler.setLevel(logging.INFO)
    email_hdler = MailHandler()
    email_hdler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s',
            "%Y-%m-%d %H:%M:%S")
    file_hdlr.setFormatter(formatter)
    stdout_hdler.setFormatter(formatter)
    email_hdler.setFormatter(formatter)
    general_logger.addHandler(file_hdlr)
    general_logger.addHandler(stdout_hdler)
    general_logger.addHandler(email_hdler)

    bos_logger = logging.getLogger(EnvConfig.BOS_LOG_NAME)
    bos_logger.setLevel(logging.DEBUG)
    fh = logging.handlers.TimedRotatingFileHandler(
            EnvConfig.BOS_LOG_FILE_PATH, when='D', backupCount=EnvConfig.LOG_ROTATE_DAY)
    fh.setFormatter(formatter)
    bos_logger.addHandler(fh)


    hdfs_logger = logging.getLogger(EnvConfig.HDFS_LOG_NAME)
    hdfs_logger.setLevel(logging.DEBUG)
    fh = logging.handlers.TimedRotatingFileHandler(
            EnvConfig.HDFS_LOG_FILE_PATH, when='D', backupCount=EnvConfig.LOG_ROTATE_DAY)
    fh.setFormatter(formatter)
    hdfs_logger.addHandler(fh)

    success_logger = logging.getLogger(EnvConfig.SUCCESS_LOG_NAME)
    success_logger.setLevel(logging.DEBUG)
    fh = logging.handlers.TimedRotatingFileHandler(
            EnvConfig.SUCCESS_LOG_FILE_PATH, when='D', backupCount=EnvConfig.LOG_ROTATE_DAY)
    fh.setFormatter(formatter)
    success_logger.addHandler(fh)

    failure_logger = logging.getLogger(EnvConfig.FAILURE_LOG_NAME)
    failure_logger.setLevel(logging.DEBUG)
    fh = logging.handlers.TimedRotatingFileHandler(
            EnvConfig.FAILURE_LOG_FILE_PATH, when='D', backupCount=EnvConfig.LOG_ROTATE_DAY)
    fh.setFormatter(formatter)
    failure_logger.addHandler(fh)

if __name__ == '__main__':
    InitLogger()
