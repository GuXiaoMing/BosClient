"""
# @file transferor.py
# @Synopsis  transfer manager
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-12-06
"""
import os
from datetime import datetime
import logging
from conf.env_config import EnvConfig

general_logger = logging.getLogger(EnvConfig.GENERAL_LOG_NAME)
success_logger = logging.getLogger(EnvConfig.SUCCESS_LOG_NAME)
failure_logger = logging.getLogger(EnvConfig.FAILURE_LOG_NAME)

class Transferor(object):
    """
    # @Synopsis  transfer manager
    """
    def __init__(self):
        pass

    def lsrSrcPath(self, path):
        """
        # @Synopsis  list all files recursively in src path, to be overriden by
        # child classes
        # @Args path
        # @Returns   list of file paths
        """
        pass

    def lsrDstPath(self, path):
        """
        # @Synopsis  list all files recursively in dst path, to be overriden by
        # child classes
        # @Args path
        # @Returns   list of file paths
        """
        pass

    def transferFile(self, src_file, dst_file):
        """
        # @Synopsis  transfer single file, to be overriden
        # @Args src_file
        # @Args dst_file
        # @Returns   None
        """
        pass

    def transfer(self, src_root_path, dst_root_path):
        """
        # @Synopsis  transfer management logic
        # @Args src_root_path
        # @Args dst_root_path
        # @Returns   None
        """
        src_files = self.lsrSrcPath(src_root_path)
        if len(src_files) == 0:
            general_logger.warning('No file to transfer in {}'.format(src_root_path))
            return 1

        dst_exist_files = self.lsrDstPath(dst_root_path)
        if len(dst_exist_files) > 0:
            general_logger.warning('Failed to transfer: there are file(s) in destination {}'\
                    .format(dst_root_path))
            return 1

        def path_mapper(path):
            """
            # @Synopsis  map src path to dst path
            # @Args path
            # @Returns   dst path
            """
            if path == src_root_path:
                return dst_root_path
            relative_path = os.path.relpath(path, src_root_path)
            dst_path = os.path.join(dst_root_path, relative_path)
            return dst_path

        dst_files = map(path_mapper, src_files)
        transfer_list = zip(src_files, dst_files)
        transfer_file_cnt = len(transfer_list)
        failure_cnt = 0

        general_logger.info('start to transfer {0} --> {1}, file_cnt = {2}'\
                .format(src_root_path, dst_root_path, transfer_file_cnt))

        start_time = datetime.now()
        for index, transfer_info in enumerate(transfer_list):
            try:
                self.transferFile(transfer_info[0], transfer_info[1])
                general_logger.debug('succeeded to transfer {}/{}: {} --> {}'.format(
                    index + 1, transfer_file_cnt, transfer_info[0], transfer_info[1]))
                success_logger.debug('{0} --> {1}'.format(transfer_info[0], transfer_info[1]))
            except Exception as e:
                general_logger.warning('failed to transfer {}/{}: {} --> {}, message: {}'.format(
                    index + 1, transfer_file_cnt, transfer_info[0], transfer_info[1], e.message))
                failure_logger.debug(('{} --> {}, message: {}').format(transfer_info[0],
                    transfer_info[1], e.message))
                failure_cnt += 1

            cur_time = datetime.now()
            time_elapsed = cur_time - start_time
            hour_elapsed = float(time_elapsed.total_seconds()) / 3600
            estimate_remain_hours = float(transfer_file_cnt -
                    index - 1) / (index + 1) * hour_elapsed

            general_logger.info(('processed file_cnt {}/{}={:.1f}%, elapsed {:.1f} hours, '
                'estimate to finish in {:.1f} hours')\
                    .format(index + 1, transfer_file_cnt,
                    float(index + 1) / transfer_file_cnt * 100,
                    hour_elapsed, estimate_remain_hours))

        general_logger.info(('finished transfering {} --> {}, failure_cnt = {}/{}, '
            'see in failure log if failure_cnt > 0').format(src_root_path, dst_root_path,
                failure_cnt, transfer_file_cnt))


def lsrLocalFiles(path):
    """
    # @Synopsis  list recursive all file under a local path
    # @Args path
    # @Returns   list of file paths
    """
    file_list = []
    if os.path.isfile(path):
        file_list.append(path)
    else:
        for root, dirs, files in os.walk(path):
            for name in files:
                file_path = os.path.join(root, name)
                file_list.append(file_path)
    return file_list


class Uploader(Transferor):
    """
    # @Synopsis  uploader
    """
    def __init__(self, my_bos_client):
        self.my_bos_client = my_bos_client

    def lsrSrcPath(self, path):
        """
        # @Synopsis  list recursively src path, override the father's method
        # @Args path
        # @Returns   list of file paths
        """
        return lsrLocalFiles(path)

    def lsrDstPath(self, path):
        """
        # @Synopsis  list recursively dst path, override the father's method
        # @Args path
        # @Returns   list of file paths
        """
        return self.my_bos_client.lsr(path)

    def transferFile(self, src_file, dst_file):
        """
        # @Synopsis  transfer a single file, override the father's method
        # @Args src_file
        # @Args dst_file
        # @Returns   None
        """
        self.my_bos_client.put_object_from_file(src_file, dst_file)


class Downloader(Transferor):
    """
    # @Synopsis  downloader
    """
    def __init__(self, my_bos_client):
        self.my_bos_client = my_bos_client

    def lsrSrcPath(self, path):
        """
        # @Synopsis  list recursively src path, override the father's method
        # @Args path
        # @Returns   list of file paths
        """
        return self.my_bos_client.lsr(path)

    def lsrDstPath(self, path):
        """
        # @Synopsis  list recursively dst path, override the father's method
        # @Args path
        # @Returns   list of file paths
        """
        return lsrLocalFiles(path)

    def transferFile(self, src_file, dst_file):
        """
        # @Synopsis  transfer a single file, override the father's method
        # @Args src_file
        # @Args dst_file
        # @Returns   None
        """
        father_path = os.path.split(dst_file)[0]
        try:
            os.makedirs(father_path)
        except OSError as e:
            pass
        self.my_bos_client.get_object_to_file(src_file, dst_file)
