"""
# @file hdfs.py
# @Synopsis  hadoop hdfs operations
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-06
"""
import commands
import os
import logging

class HDFSError(Exception):
    """
    # @Synopsis  HDFS error class
    """
    def __init__(self, message=''):
        self.message = message

class HDFSClient(object):
    """
    # @Synopsis  hadoop fs client
    """

    def __init__(self, client_path, log_name):
        self.client_path = client_path
        self.conf_path = os.path.join(client_path, 'hadoop/conf')
        self.bin_path = os.path.join(client_path, 'hadoop/bin/hadoop')
        self.logger = logging.getLogger(log_name)

    def lsr(self, hdfs_path):
        """
        # @Synopsis  lsr
        # @Args hdfs_path
        # @Returns   file info list, including type, path and size
        """
        hdfs_cmd = '-lsr {0}'.format(hdfs_path)
        sh_cmd = ' '.join([self.bin_path, 'fs', hdfs_cmd])
        status, output = commands.getstatusoutput(sh_cmd)
        self.logger.debug('Returned {0}: {1}\n{2}'.format(status, sh_cmd, output))
        if status != 0:
            pass
        output_lines = output.split('\n')
        def output_parser(line):
            """
            # @Synopsis  map hadoop fs -lsr output to desired format
            # @Args line
            # @Returns   file info dict
            """
            fields = line.split()
            if fields[0][0] == 'd':
                object_type = 'd'
            else:
                object_type = 'f'
            object_path = fields[7]
            object_size = int(fields[4])
            return dict({
                'type': object_type,
                'path': object_path,
                'size': object_size
                })
        objects = map(output_parser, output_lines)
        return objects

    def mkdir(self, hdfs_path):
        """
        # @Synopsis  mkdir
        """
        cmd = ' '.join(['-mkdir', hdfs_path])
        return self._exec(cmd)

    def get(self, hdfs_path, local_path):
        """
        # @Synopsis  get single file from hdfs
        """
        cmd = ' '.join(['-get', hdfs_path, local_path])
        return self._exec(cmd)

    def getmerge(self, hdfs_path, local_path):
        """
        # @Synopsis  get a director from hdfs and merge into one local file
        """
        cmd = ' '.join(['-getmerge', hdfs_path, local_path])
        return self._exec(cmd)

    def put(self, local_path, hdfs_path):
        """
        # @Synopsis  put a local file to hdfs
        """
        cmd = ' '.join(['-put', local_path, hdfs_path])
        return self._exec(cmd)

    def rmr(self, file_path):
        """
        # @Synopsis   remove a hdfs directory
        """
        cmd = ' '.join(['-rmr', file_path])
        return self._exec(cmd)

    def exists(self, file_path):
        """
        # @Synopsis  test if a hdfs file/directory exists
        """
        cmd=' '.join(['-test -e', file_path])
        return self._exec(cmd)

    def overwrite(self, local_path, hdfs_path):
        """
        # @Synopsis  put a local file to hdfs, if the hdfs path already exists,
        # overwrite it
        """
        if self.exists(hdfs_path):
            self.rmr(hdfs_path)
        succeeded = self.put(local_path, hdfs_path)
        return succeeded

    def _exec(self, hdfs_cmd):
        """
        # @Synopsis  execute hadoop fs command
        #
        # @Args hdfs_cmd
        #
        # @Returns   the status of the execution
        """
        # in spark-1.4 client provided by Baidu, the environment variable
        # 'HADOOP_CONF_DIR' would be altered in the shell runing spark-submit
        # script, which means that environment variable would be inherited by
        # the subprocess called by the python program submitted to spark,
        # specifically, hadoop. This would cause the default hadoop
        # configuration to be overridden by that environment variable, and thus
        # doesn't work.'
        # So what the following code doing is: store the current environment
        # variable, change that to the correct one, run hadoop, and then
        # restore it.
        origin_hadoop_conf_dir = os.environ.get('HADOOP_CONF_DIR', None)
        os.environ['HADOOP_CONF_DIR'] = self.conf_path
        sh_cmd = ' '.join([self.bin_path, 'fs', hdfs_cmd])
        status, output = commands.getstatusoutput(sh_cmd)
        self.logger.debug('Returned {0}: {1}\n{2}'.format(status, sh_cmd, output))
        if status != 0:
            raise HDFSError(output)
        if origin_hadoop_conf_dir is not None:
            os.environ['HADOOP_CONF_DIR'] = origin_hadoop_conf_dir
        return status == 0


if __name__ == '__main__':
    import sys
    sys.path.append('..')
    from conf.init_logger import initLogger
    from conf.env_config import EnvConfig
    initLogger()
    hdfs_client = HdfsClient(EnvConfig.HADOOP_CLIENT_PATH, EnvConfig.LOG_NAME)
    print hdfs_client.lsr('/app/vs/ns-video/guming/yuanye')
