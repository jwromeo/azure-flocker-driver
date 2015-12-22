from azure.mgmt.resource import ResourceGroup
from azure.mgmt.resource import ResourceListParameters
from azure.mgmt.resource import ResourceIdentity
from azure.mgmt.resource import GenericResource
import json
class DiskManager(object):

  def __init__(self, resource_client, group_name, location):
    self._resource_client = resource_client
    self._resource_group = group_name
    self._location = location


  def _str_array_to_lower(self, str_arry):
    array = []
    for s in str_arry:
      array.append(s.lower().replace(' ',''))

    return array
  def _get_supported_api_versions(self, location, resource_namespace, resource_type):
    vm_provider = self._resource_client.providers.get(resource_namespace)

    print dir(vm_provider.provider.resource_types)

    for t in vm_provider.provider.resource_types:
      print t.name + '==' + resource_type
      print self._str_array_to_lower(t.locations)
      print t.locations
      if resource_type == t.name and \
        location in self._str_array_to_lower(t.locations):
        return t.api_versions
    
    return None

  def compute_next_lun(self, data_disks):
      lun = 0
      for i in range(0, len(data_disks)):
          next_lun = data_disks[i].lun

          if next_lun - i >= 1:
              lun = next_lun - 1
              break

          if i == len(data_disks) - 1:
              lun = next_lun + 1
              break

      return lun

  def attach_disk(self, vm_name, vhd_name, vhd_link, vhd_size_in_gibs):
    return self._attach_or_detach_disk(vm_name, vhd_name, vhd_link, vhd_size_in_gibs)

  def detach_disk(self, vm_name, vhd_name, vhd_link, vhd_size_in_gibs):
    return self._attach_or_detach_disk(vm_name, vhd_name, vhd_link, vhd_size_in_gibs, True)

  def list_disks(self, vm_name):
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

  def _attach_or_detach_disk(self, vm_name, vhd_name, vhd_link, vhd_size_in_gibs, detach=False):
    parameters = ResourceListParameters(
      resource_group_name=self._resource_group,
      resource_type='Microsoft.Compute/virtualMachines')
    result = self._resource_client.resources.list(parameters)\

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
                      "caching": "None",
                      "diskSizeGB": vhd_size_in_gibs
                  })
    else:
      # detach specified disk
      target_disk
      for i in range(len(properties['storageProfile']['dataDisks'])):
        d = properties['storageProfile']['dataDisks'][i]
        if d.name == vhd_name:
          del properties['storageProfile']['dataDisks'][i]
    resource.properties = json.dumps(properties)
    self._resource_client.resources.create_or_update(self._resource_group, identity,
      GenericResource(location=resource.location,
                      properties=resource.properties))



