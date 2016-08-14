from azure.mgmt.compute.models import DataDisk
from azure.mgmt.compute.models import VirtualHardDisk
from bitmath import GiB
from vhd import Vhd
import uuid
import time


class AzureAsynchronousTimeout(Exception):

    def __init__(self):
        pass


class AzureInsufficientLuns(Exception):

    def __init__(self):
        pass


class AzureElementNotFound(Exception):

    def __init__(self):
        pass


class AzureVMSizeNotSupported(Exception):

    def __init__(self):
        pass


class AzureOperationNotAllowed(Exception):

    def __init__(self):
        pass


class DiskManager(object):

    # Resource provider constants
    STORAGE_RESOURCE_PROVIDER_NAME = "Microsoft.Storage"
    STORAGE_RESORUCE_PROVIDER_VERSION = "2016-01-01"
    LUN0_RESERVED_VHD_NAME_SUFFIX = "lun0_reserved"

    def __init__(self,
                 resource_client,
                 compute_client,
                 storage_client,
                 disk_container_name,
                 group_name,
                 location,
                 async_timeout=600):
        self._resource_client = resource_client
        self._compute_client = compute_client
        self._resource_group = group_name
        self._location = location
        self._storage_client = storage_client
        self._disk_container = disk_container_name
        self._async_timeout = async_timeout

        # ensure the container exists.
        self._storage_client.create_container(disk_container_name)

    def _str_array_to_lower(self, str_arry):
        array = []
        for s in str_arry:
            array.append(s.lower().replace(' ', ''))
        return array

    def _get_max_luns_for_vm_size(self, vm_size):
        max_luns = 0
        vmSizes = self._compute_client.virtual_machine_sizes.list(
            self._location)
        for vm in vmSizes:
            if vm.name == vm_size:
                max_luns = vm.max_data_disk_count
                break
        return max_luns

    def _is_lun_0_empty(self, diskInfo):
        lun0Empty = True
        for disk in diskInfo:
            if disk.lun == 0:
                lun0Empty = False
                break
        return lun0Empty

    def _compute_next_lun(self, total_luns, data_disks):
        nextLun = -1
        usedLuns = []
        for i in range(0, len(data_disks)):
            usedLuns.append(data_disks[i].lun)
        for lun in range(1, total_luns):
            if lun not in usedLuns:
                nextLun = lun
                break
        if nextLun == -1:
            raise AzureInsufficientLuns()
        return nextLun

    def _attach_disk(self, vm_name, vhd_name, vhd_size_in_gibs, lun):
        self._attach_or_detach_disk(vm_name, vhd_name, vhd_size_in_gibs, lun)

        timeout_count = 0
        while self.is_disk_attached(vm_name, vhd_name) is False:
            time.sleep(1)
            timeout_count += 1
            if timeout_count > self._async_timeout:
                raise AzureAsynchronousTimeout()

    def attach_disk(self, vm_name, vhd_name, vhd_size_in_gibs):
        # get VM information
        vm = self.get_vm(vm_name)
        vm_size = vm.hardware_profile.vm_size
        vm_luns = self._get_max_luns_for_vm_size(vm_size)

        # first check and see if we need to add a special place holder
        # on lun-0
        if self._is_lun_0_empty(vm.storage_profile.data_disks):
            lun0_disk_name = vm_name + "-" + self.LUN0_RESERVED_VHD_NAME_SUFFIX
            print("Need to attach reserved disk named '%s' to lun 0" %
                  lun0_disk_name)
            self.create_disk(lun0_disk_name, 1)
            self._attach_disk(vm_name, lun0_disk_name, 1, 0)
            vm = self.get_vm(vm_name)

        lun = self._compute_next_lun(vm_luns, vm.storage_profile.data_disks)
        self._attach_disk(vm_name, vhd_name, vhd_size_in_gibs, lun)
        return

    def detach_disk(self, vm_name, vhd_name, allow_lun0_detach=False):
        self._attach_or_detach_disk(vm_name, vhd_name, 0,
                                    0, True, allow_lun0_detach)
        timeout_count = 0
        while self.is_disk_attached(vm_name, vhd_name) is True:
            time.sleep(1)
            timeout_count += 1

            if timeout_count > self._async_timeout:
                raise AzureAsynchronousTimeout()

        return

    def list_disks(self):
        # will list a max of 5000 blobs, but there really shouldn't
        # be that many
        disks = self._storage_client.list_blobs(self._disk_container)
        return_disks = []
        for disk in disks:
            disk.name = disk.name.replace('.vhd', '')
            return_disks.append(disk)
        return return_disks

    def destroy_disk(self, disk_name):
        self._storage_client.delete_blob(self._disk_container,
                                         disk_name + '.vhd')
        return

    def create_disk(self, disk_name, size_in_gibs):
        size_in_bytes = int(GiB(size_in_gibs).to_Byte().value)
        link = Vhd.create_blank_vhd(self._storage_client,
                                    self._disk_container,
                                    disk_name + '.vhd',
                                    size_in_bytes)
        return link

    def is_disk_attached(self, vm_name, disk_name):
        disks = self.list_attached_disks(vm_name)
        return disk_name in [d.name for d in disks]

    def _is_disk_successfully_attached(self, vm_name, disk_name):
        vm = self.get_vm(vm_name=vm_name, expand="instanceView")

        for disk_instance in vm.instance_view.disks:
            if disk_instance.name == disk_name:
                return disk_instance.statuses[0].code == \
                        "ProvisioningState/succeeded"

        return False

    def list_attached_disks(self, vm_name):
        vm = self.get_vm(vm_name=vm_name, expand="instanceView")

        disks_in_model = vm.storage_profile.data_disks
        disk_names = [d.name for d in disks_in_model]

        # If there's a disk in the instance view which is not in the model.
        # This disk is stuck.  Add it to our list since we need to know
        # about stuck disks
        for disk_instance in vm.instance_view.disks:
            if disk_instance.name in disk_names is None:
                disk = DataDisk(lun=-1,
                                name=disk_instance.name)
                disks_in_model.append(disk)

        return disks_in_model

    def get_vm(self, vm_name, expand=None):
        return self._compute_client.virtual_machines.get(
            resource_group_name=self._resource_group,
            vm_name=vm_name,
            expand=expand)

    def _update_vm(self, vm_name, vm):
        # To ensure the VM update will be a async update even if the
        # VM did not change we force a change to the VM by setting a
        # tag in every PUT request with a UUID
        if vm.tags is not None:
            vm.tags['updateId'] = str(uuid.uuid4())
        else:
            vm.tags = {'updateId': str(uuid.uuid4())}

        return self._compute_client.virtual_machines.create_or_update(
            self._resource_group,
            vm_name,
            vm)

    def _attach_or_detach_disk(self,
                               vm_name,
                               vhd_name,
                               vhd_size_in_gibs,
                               lun,
                               detach=False,
                               allow_lun_0_detach=False,
                               is_from_retry=False):
        vmcompute = self.get_vm(vm_name)

        if (not detach):
            vhd_url = self._storage_client.make_blob_url(self._disk_container,
                                                         vhd_name + ".vhd")
            print("Attach disk name %s lun %s uri %s" %
                  (vhd_name, lun, vhd_url))
            disk = DataDisk(lun=lun,
                            name=vhd_name,
                            vhd=VirtualHardDisk(vhd_url),
                            caching="None",
                            create_option="attach",
                            disk_size_gb=vhd_size_in_gibs)
            vmcompute.storage_profile.data_disks.append(disk)
        else:
            for i in range(len(vmcompute.storage_profile.data_disks)):
                disk = vmcompute.storage_profile.data_disks[i]
                if disk.name == vhd_name:
                    if disk.lun == 0 and not allow_lun_0_detach:
                        # lun-0 is special, throw an exception if attempting
                        # to detach that disk.
                        raise AzureOperationNotAllowed()

                    print("Detach disk name %s lun %s uri %s" %
                          (disk.name, disk.lun, disk.vhd.uri))
                    del vmcompute.storage_profile.data_disks[i]
                    break

        result = self._update_vm(vm_name, vmcompute)
        start = time.time()
        while True:
            time.sleep(2)
            waited_sec = int(abs(time.time() - start))
            if waited_sec > self._async_timeout:
                raise AzureAsynchronousTimeout()

            if not result.done():
                continue

            updated = self.get_vm(vm_name)

            print("Waited for %s s provisioningState is %s" %
                  (waited_sec, updated.provisioning_state))

            if updated.provisioning_state == "Succeeded":
                print("Operation finshed")
                break

            if updated.provisioning_state == "Failed":
                print("Provisioning ended up in failed state.")

                # Recovery from failed disk atatch-detach operation.
                # For Attach Disk: Detach The Disk then Try To Attach Again
                # For Detach Disk: Call update again, which always sets a tag,
                #                  which forces the service to retry.

                # is_from_retry is checked so we are not stuck in a loop
                # calling ourself
                if not is_from_retry and not detach:
                    print("Detach disk %s after failure, then try attach again"
                          % vhd_name)

                    self._attach_or_detach_disk(vm_name,
                                                vhd_name,
                                                vhd_size_in_gibs,
                                                lun,
                                                detach=True,
                                                allow_lun_0_detach=True,
                                                is_from_retry=True)

                print("Retry disk action for disk %s" % vhd_name)
                result = self._update_vm(vm_name, vmcompute)
