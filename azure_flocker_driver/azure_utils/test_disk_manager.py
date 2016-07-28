from arm_disk_manager import DiskManager
from twisted.trial import unittest
from eliot import Logger
from azure.storage.blob import PageBlobService
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource.resources import ResourceManagementClient
import os
import yaml

azure_config = None
_logger = Logger()
config_file_path = os.environ.get('AZURE_CONFIG_FILE')

if config_file_path is not None:
    config_file = open(config_file_path)
    config = yaml.load(config_file.read())
    azure_config = config['azure_settings']


class DiskCreateTestCase(unittest.TestCase):

    def setUp(self):
        creds = ServicePrincipalCredentials(
            client_id=azure_config['client_id'],
            secret=azure_config['client_secret'],
            tenant=azure_config['tenant_id'])
        self._resource_client = ResourceManagementClient(
            creds,
            azure_config['subscription_id'])
        self._page_blob_service = PageBlobService(
            account_name=azure_config['storage_account_name'],
            account_key=azure_config['storage_account_key'])
        self._manager = DiskManager(
            self._resource_client,
            self._page_blob_service,
            azure_config['storage_account_container'],
            azure_config['group_name'],
            azure_config['location'])

    def test_create_blank_vhd(self):
        link = self._manager.create_disk(azure_config['test_vhd_name'], 2)
        self.assertEqual(link,
                         'https://' +
                         self._page_blob_service.account_name +
                         '.blob.core.windows.net/' +
                         azure_config['storage_account_container'] +
                         '/' + azure_config['test_vhd_name'] + '.vhd')

        disks = self._manager.list_disks()
        found = False
        for disk in disks:
            if disk.name == azure_config['test_vhd_name']:
                found = True
                break
        self.assertEqual(found, True,
                         'Expected disk: ' +
                         azure_config['test_vhd_name'] +
                         ' to be listed in DiskManager.list_disks')

        self._manager.destroy_disk(azure_config['test_vhd_name'])
        disks = self._manager.list_disks()
        found = False
        for disk in disks:
            if disk.name == azure_config['test_vhd_name']:
                found = True
                break

        self.assertEqual(found, False, 'Expected disk: '
                         + azure_config['test_vhd_name'] +
                         ' not to be listed in DiskManager.list_disks')

    def test_attach_blank_vhd(self):
        node_name = azure_config['test_vm_name']
        vhd_name = azure_config['test_vhd_name']

        self._manager.create_disk(azure_config['test_vhd_name'], 2)
        disks = self._manager.list_disks()
        found = False
        for disk in disks:
            if disk.name == azure_config['test_vhd_name']:
                found = True
                break
        self.assertEqual(found, True,
                         'Expected disk: ' +
                         azure_config['test_vhd_name'] +
                         ' to be listed in DiskManager.list_disks')

        self._manager.attach_disk(node_name, vhd_name, 2)
        attached = self._manager.is_disk_attached(node_name, vhd_name)
        self.assertEqual(attached, True,
                         'Expected disk: ' +
                         azure_config['test_vhd_name'] +
                         ' should be attached to vm: ' + node_name)

        self._manager.detach_disk(node_name, vhd_name)
        attached = self._manager.is_disk_attached(node_name, vhd_name)
        self.assertEqual(attached, False,
                         'Expected disk: ' +
                         azure_config['test_vhd_name'] +
                         ' should not be attached to vm: ' + node_name)

        self._manager.destroy_disk(vhd_name)
        disks = self._manager.list_disks()
        found = False
        for disk in disks:
            if disk.name == vhd_name:
                found = True
                break
        self.assertEqual(found, False, 'Expected disk: ' +
                         azure_config['test_vhd_name'] +
                         ' should not to be listed in '
                         'DiskManager.list_disks')
