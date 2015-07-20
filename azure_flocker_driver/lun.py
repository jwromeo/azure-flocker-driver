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

    @staticmethod
    def compute_next_remote_lun(azure_service_client, service_name, role_name):
        vm_info = azure_service_client.get_role(
            service_name,
            service_name,
            role_name)

        vm_info.data_virtual_hard_disks = \
            sorted(vm_info.data_virtual_hard_disks, key=lambda obj:
                   obj.lun)
        lun = 0
        for i in range(0, len(vm_info.data_virtual_hard_disks)):
            next_lun = vm_info.data_virtual_hard_disks[i].lun

            if next_lun - i >= 1:
                lun = next_lun - 1
                break

            if i == len(vm_info.data_virtual_hard_disks) - 1:
                lun = next_lun + 1
                break

        return lun

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
