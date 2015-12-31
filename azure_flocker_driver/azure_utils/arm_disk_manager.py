from azure.mgmt.resource import ResourceGroup
from azure.mgmt.resource import ResourceListParameters
from azure.mgmt.resource import ResourceIdentity
from azure.mgmt.resource import GenericResource
from bitmath import Byte, GiB
from vhd import Vhd
import json
import os
import uuid
import time

class AzureAsynchronousTimeout(Exception):

    def __init__(self):
        pass

class DiskManager(object):

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
      array.append(s.lower().replace(' ',''))

    return array
  def _get_supported_api_versions(self, location, resource_namespace, resource_type):
    vm_provider = self._resource_client.providers.get(resource_namespace)

    for t in vm_provider.provider.resource_types:
      if resource_type == t.name and \
        location in self._str_array_to_lower(t.locations):
        return t.api_versions
    
    return None

  def compute_next_lun(self, data_disks):
    lun = 0
    for i in range(0, len(data_disks)):
        print data_disks[i]
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
    # will list a max of 5000 blobs, but there really shouldn't be that many
    disks = self._storage_client.list_blobs(self._disk_container);

    for i in range(len(disks)):
      disk = disks[i]
      disk.name = disk.name.replace('.vhd', '')

    return disks

  def destroy_disk(self, disk_name):
    self._storage_client.delete_blob(self._disk_container, disk_name + '.vhd')
    return

  def create_disk(self, disk_name, size_in_gibs):
    link = Vhd.create_blank_vhd(self._storage_client, 
      self._disk_container,
      disk_name + '.vhd',
      int(GiB(size_in_gibs).to_Byte().value))
    return link

  def is_disk_attached(self, vm_name, disk_name):
    disks = self.list_attached_disks(vm_name)
    found = False
    for i in range(len(disks)):
      disk = disks[i]
      if disk['name'] == disk_name:
          found = True
          break
    
    if (found):
      return  found

    # check if blob lease is active, this means the disk is
    # actually still attached, and may be detaching
    props = self._storage_client.get_blob_properties(self._disk_container, disk_name + '.vhd')
    return props['x-ms-lease-status'] == 'locked'

  def list_attached_disks(self, vm_name):
    parameters = ResourceListParameters(
      resource_group_name=self._resource_group,
      resource_type='Microsoft.Compute/virtualMachines')
    result = self._resource_client.resources.list(parameters)

    api_versions = self._get_supported_api_versions(result.resources[0].location, 'Microsoft.Compute', 'virtualMachines')

    if len(api_versions) == 0:
      raise Exception('No API Version supported for Microsoft.Compute/virtualMachines in location ' + result.resources[0].location)
    
    identity = ResourceIdentity(
      resource_name=result.resources[0].name, 
      resource_type='virtualMachines',
      api_version=api_versions[0],
      resource_namespace='Microsoft.Compute')
    resource_result = self._resource_client.resources.get(self._resource_group, identity)
    resource = resource_result.resource
    properties = json.loads(resource.properties)

    return properties['storageProfile']['dataDisks']

  def _attach_or_detach_disk(self, vm_name, vhd_name, vhd_size_in_gibs, detach=False):
    vhd_link = 'https://' + self._storage_client.account_name + '.'

    if 'STORAGE_HOST_NAME' in os.environ:
      vhd_link += os.environ['STORAGE_HOST_NAME']
    else:
      vhd_link += 'blob.core.windows.net'

    vhd_link += '/' + self._disk_container + '/' + vhd_name + '.vhd'
    parameters = ResourceListParameters(
      resource_group_name=self._resource_group,
      resource_type='Microsoft.Compute/virtualMachines')
    result = self._resource_client.resources.list(parameters)

    api_versions = self._get_supported_api_versions(result.resources[0].location, 'Microsoft.Compute', 'virtualMachines')

    if len(api_versions) == 0:
      raise Exception('No API Version supported for Microsoft.Compute/virtualMachines in location ' + result.resources[0].location)
    
    identity = ResourceIdentity(
      resource_name=result.resources[0].name, 
      resource_type='virtualMachines',
      api_version=api_versions[0],
      resource_namespace='Microsoft.Compute')
    resource_result = self._resource_client.resources.get(self._resource_group, identity)
    resource = resource_result.resource
    properties = json.loads(resource.properties)
    if (not detach):
      # attach specified disk
      properties['storageProfile']['dataDisks'].append({
                      "lun": self.compute_next_lun(properties['storageProfile']['dataDisks']),
                      "name": vhd_name,
                      "createOption": "Attach",
                      "vhd": {
                          "uri": vhd_link
                      },
                      "caching": "None"
                  })
      # Azure engineering recommended a workaround
      # to bad machine state by updating the tag of the VM with the disk
      resource.tags = { 'dummy': str(uuid.uuid4()) }
    else:
      # detach specified disk
      for i in range(len(properties['storageProfile']['dataDisks'])):
        d = properties['storageProfile']['dataDisks'][i]
        if d['name'] == vhd_name:
          del properties['storageProfile']['dataDisks'][i]
	  break;

    resource.properties = json.dumps(properties)
    self._resource_client.resources.create_or_update(self._resource_group, identity,
      GenericResource(location=resource.location,
                      properties=resource.properties,
                      tags=resource.tags))



