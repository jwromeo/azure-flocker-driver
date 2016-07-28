from azure.mgmt.resource.resources.models import GenericResource
from bitmath import GiB
from vhd import Vhd
import uuid
import time


class AzureAsynchronousTimeout(Exception):

    def __init__(self):
        pass


class DiskManager(object):

    # Resource provider constants
    COMPUTE_RESOURCE_PROVIDER_NAME = "Microsoft.Compute"

    COMPUTE_RESOURCE_PROVIDER_VERSION = "2016-03-30"

    VIRTUAL_MACHINES = "virtualMachines"

    STORAGE_RESOURCE_PROVIDER_NAME = "Microsoft.Storage"

    STORAGE_RESORUCE_PROVIDER_VERSION = "2016-01-01"

    def __init__(self,
                 resource_client,
                 storage_client,
                 disk_container_name,
                 group_name,
                 location,
                 async_timeout=600):
        self._resource_client = resource_client
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

    def compute_next_lun(self, data_disks):
        lun = 0
        for i in range(0, len(data_disks)):
            next_lun = data_disks[i]['lun']

            if next_lun - i >= 1:
                lun = next_lun - 1
                break

            if i == len(data_disks) - 1:
                lun = next_lun + 1
                break
        return lun

    def attach_disk(self, vm_name, vhd_name, vhd_size_in_gibs):
        self._attach_or_detach_disk(vm_name, vhd_name, vhd_size_in_gibs)

        timeout_count = 0
        while self.is_disk_attached(vm_name, vhd_name) is False:
            time.sleep(1)
            timeout_count += 1

            if timeout_count > self._async_timeout:
                raise AzureAsynchronousTimeout()

        return

    def detach_disk(self, vm_name, vhd_name):
        self._attach_or_detach_disk(vm_name, vhd_name, 0, True)

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
        for disk in disks:
            disk.name = disk.name.replace('.vhd', '')
        return disks

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
                               detach=False):
        # Check current VM State first.  If it is bad we wait to do a
        # no-op update first to try and fix it
        resource = self.get_vm(vm_name)
        properties = resource.properties

        if (properties['provisioningState'] == "Failed"):
            self._create_or_update_and_wait(resource, vm_name)
            resource = self.get_vm(vm_name)
            properties = resource.properties

        if (not detach):
            # attach specified disk
            properties['storageProfile']['dataDisks'].append({
                "lun": self.compute_next_lun(
                    properties['storageProfile']['dataDisks']),
                "name": vhd_name,
                "createOption": "Attach",
                "vhd": {
                    "uri": self._storage_client.make_blob_url(
                        self._disk_container,
                        vhd_name + ".vhd")
                },
                "caching": "None"
            })

        else:      # detach specified disk
            for i in range(len(properties['storageProfile']['dataDisks'])):
                d = properties['storageProfile']['dataDisks'][i]
                if d['name'] == vhd_name:
                    del properties['storageProfile']['dataDisks'][i]
                    break

        resource.properties = properties
        self._create_or_update_and_wait_for_success(resource, vm_name)
