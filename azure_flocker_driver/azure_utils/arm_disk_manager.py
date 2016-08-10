from azure.mgmt.resource.resources.models import GenericResource
from azure.mgmt.compute.models import DataDisk
from azure.mgmt.compute.models import VirtualHardDisk
from bitmath import GiB
from vhd import Vhd
import uuid
import time
import socket


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
    COMPUTE_RESOURCE_PROVIDER_NAME = "Microsoft.Compute"
    COMPUTE_RESOURCE_PROVIDER_VERSION = "2016-03-30"
    VIRTUAL_MACHINES = "virtualMachines"
    STORAGE_RESOURCE_PROVIDER_NAME = "Microsoft.Storage"
    STORAGE_RESORUCE_PROVIDER_VERSION = "2016-01-01"
    LUN0_RESERVED_VHD_NAME = "lun0_reserved_" + \
                             str(socket.gethostname()).replace('.', '_')

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
            if disk['lun'] == 0:
                lun0Empty = False
                break
        return lun0Empty

    def _compute_next_lun(self, total_luns, data_disks):
        nextLun = -1
        usedLuns = []
        for i in range(0, len(data_disks)):
            usedLuns.append(data_disks[i]['lun'])
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
        vm_resource = self.get_vm(vm_name)
        vm_properties = vm_resource.properties
        vm_size = vm_resource.properties['hardwareProfile']['vmSize']
        vm_luns = self._get_max_luns_for_vm_size(vm_size)

        # first check and see if we need to add a special place holder
        # on lun-0
        if self._is_lun_0_empty(vm_properties['storageProfile']['dataDisks']):
            print("Need to attach reserved disk to lun 0")
            self.create_disk(self.LUN0_RESERVED_VHD_NAME, 1)
            self._attach_disk(vm_name, self.LUN0_RESERVED_VHD_NAME, 1, 0)
            vm_resource = self.get_vm(vm_name)
            vm_properties = vm_resource.properties

        lun = self._compute_next_lun(vm_luns,
                                     vm_properties['storageProfile']
                                                  ['dataDisks'])
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
        found = False
        for disk in disks:
            if disk['name'] == disk_name:
                found = True
                break
        return found

    def list_attached_disks(self, vm_name):
        # TODO:  For detection of stuck disks, merge in the disk names
        # from Instance View
        vm = self.get_vm(vm_name)
        properties = vm.properties
        return properties['storageProfile']['dataDisks']

    def get_vm(self, vm_name):
        resource_result = self._resource_client.resources.get(
            self._resource_group,
            resource_provider_namespace=self.COMPUTE_RESOURCE_PROVIDER_NAME,
            parent_resource_path="",
            resource_type=self.VIRTUAL_MACHINES,
            resource_name=vm_name,
            api_version=self.COMPUTE_RESOURCE_PROVIDER_VERSION)
        return resource_result

    def _create_or_update_with_tag(self, resource, resource_name):
        # To ensure the the Microsoft.Compute resource provider will do
        # goal-seeking even if the state of the VM did not change we will
        # update a tag in every PUT requeut with a UUID
        resource.tags = {'updateId': str(uuid.uuid4())}

        # TODO: Now calls raise exceptions, catching those would be good
        result = self._resource_client.resources.create_or_update(
            self._resource_group,
            resource_provider_namespace=self.COMPUTE_RESOURCE_PROVIDER_NAME,
            parent_resource_path="",
            resource_type=self.VIRTUAL_MACHINES,
            resource_name=resource_name,
            api_version=self.COMPUTE_RESOURCE_PROVIDER_VERSION,
            parameters=GenericResource(location=resource.location,
                                       properties=resource.properties,
                                       tags=resource.tags))
        return result

    def _create_or_update_and_wait_for_success(self, resource, vm_name):
        result = self._create_or_update_with_tag(resource, vm_name)

        # We need to wait on provisioning State to Success
        success = False
        timeout_count = 0

        namespace = self.COMPUTE_RESOURCE_PROVIDER_NAME
        while success is False:
            time.sleep(1)
            timeout_count += 1
            result = self._resource_client.resources.get(
                self._resource_group,
                resource_provider_namespace=namespace,
                parent_resource_path="",
                resource_type=self.VIRTUAL_MACHINES,
                resource_name=vm_name,
                api_version=self.COMPUTE_RESOURCE_PROVIDER_VERSION)
            properties = result.properties

            print("waited for %s s provisioningState is %s" %
                  (timeout_count, properties["provisioningState"]))

            if (properties['provisioningState'] == "Failed"):
                # Something went wrong, let's try PUT again
                result = self._create_or_update_with_tag(resource, vm_name)
                properties = result.properties

            # Wait for Success
            if (properties['provisioningState'] == "Succeeded"):
                success = True

            if timeout_count > self._async_timeout:
                # TODO: Add details to exception
                raise AzureAsynchronousTimeout()

        return result

    def _attach_or_detach_disk(self,
                               vm_name,
                               vhd_name,
                               vhd_size_in_gibs,
                               lun,
                               detach=False,
                               allow_lun_0_detach=False):
        vmcompute = self._compute_client.virtual_machines.get(
            self._resource_group,
            vm_name)
        if (not detach):
            vhd_url = self._storage_client.make_blob_url(self._disk_container,
                                                         vhd_name + ".vhd")
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
                    del vmcompute.storage_profile.data_disks[i]
                    break
        result = self._compute_client.virtual_machines.create_or_update(
            self._resource_group,
            vm_name,
            vmcompute)
        # result.wait()
        timeout_count = 0
        while True:
            done = result.done()
            if done:
                vmcompute = self._compute_client.virtual_machines.get(
                    self._resource_group,
                    vm_name)
                if vmcompute.provisioning_state != "Succeeded":
                    print("Provisioning ended up in failed state. "
                          "Adding updateId tag.")
                    if vmcompute.tags is not None:
                        vmcompute.tags['updateId'] = str(uuid.uuid4())
                    else:
                        vmcompute.tags = {'updateId': str(uuid.uuid4())}
                    result = \
                        self._compute_client.virtual_machines.create_or_update(
                            self._resource_group,
                            vm_name,
                            vmcompute)
                else:
                    print("Operation finished.")
                    break
            time.sleep(1)
            timeout_count += 1
            if timeout_count > self._async_timeout:
                raise AzureAsynchronousTimeout()
            print("Waiting " + str(timeout_count) + " seconds for operation.")
