import subprocess
import os

from twisted.python.filepath import FilePath


class Lun(object):

    device_path = ''
    lun = ''

    def __init__():
        return

    @staticmethod
    def rescan_scsi():
        with open(os.devnull, 'w') as shutup:
            subprocess.call(['fdisk', '-l'], stdout=shutup, stderr=shutup)

    # Returns a string representing the block device path based
    # on a provided lun slot
    @staticmethod
    def get_device_path_for_lun(lun):
        """
        Returns a FilePath representing the path of the device
        with the sepcified LUN. TODO: Is it valid to predict the
        path based on the LUN?
        return FilePath: The FilePath representing the attached disk
        """
        Lun.rescan_scsi()
        if lun > 31:
            raise Exception('valid lun parameter is 0 - 31, inclusive')
        base = '/dev/sd'

        # luns go 0-31
        ascii_base = ord('c')

        return FilePath(base + chr(ascii_base + lun), False)
