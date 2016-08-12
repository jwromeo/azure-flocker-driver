from arm_disk_manager import DiskManager
from arm_disk_manager import AzureOperationNotAllowed
from twisted.trial import unittest
from eliot import Logger
from azure.storage.blob import PageBlobService
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
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
        self._compute_client = ComputeManagementClient(
            creds,
            azure_config['subscription_id'])
        self._page_blob_service = PageBlobService(
            account_name=azure_config['storage_account_name'],
            account_key=azure_config['storage_account_key'])
        self._manager = DiskManager(
            self._resource_client,
            self._compute_client,
            self._page_blob_service,
            azure_config['storage_account_container'],
            azure_config['group_name'],
            azure_config['location'])

    def _has_lun0_disk(self, node_name):
        vm_disks = self._manager.list_attached_disks(node_name)
        for disk in vm_disks:
            if disk.lun == 0:
                return True
        return False

    def list_disks(self):
        return self._manager.list_disks()

    def _create_disk(self, vhd_name, vhd_size_in_gibs):
        print("creating disk " + vhd_name + ", size " + str(vhd_size_in_gibs))
        link = self._manager.create_disk(vhd_name, vhd_size_in_gibs)
        disks = self.list_disks()
        found = False
        for disk in disks:
            if disk.name == vhd_name:
                found = True
                break
        self.assertEqual(found, True,
                         'Expected disk: ' + vhd_name +
                         ' to be listed in DiskManager.list_disks')
        return link

    def _attach_disk(self, node_name, vhd_name, vhd_size_in_gibs):
        print("attaching disk " + vhd_name + ", node " + node_name)
        self._manager.attach_disk(node_name, vhd_name, vhd_size_in_gibs)
        attached = self._manager.is_disk_attached(node_name, vhd_name)
        self.assertEqual(attached, True,
                         'Expected disk: ' + vhd_name +
                         ' should be attached to vm: ' + node_name)

    def _check_for_lun_0_disk(self, node_name, vhd_name):
        print("checking for lun 0 on " + node_name)
        lun0Disk = None
        vm_disks = self._manager.list_attached_disks(node_name)
        for disk in vm_disks:
            if disk.lun == 0:
                lun0Disk = disk.name.replace('.vhd', '')
                break
        self.assertNotEqual(lun0Disk, None,
                            'After an attach of any disk, lun0 should '
                            'have a disk attached to vm: ' + node_name)
        self.assertNotEqual(lun0Disk, vhd_name,
                            'Lun-0 disk is special and should not be '
                            'the same as the test_vhd ' + vhd_name)
        return lun0Disk

    def _try_to_detach_disk_on_lun_0(self, node_name, lun0_disk):
        print("attempted lun 0 detach on " + node_name)
        exceptionCaught = False
        try:
            self._manager.detach_disk(node_name, lun0_disk)
        except AzureOperationNotAllowed:
            exceptionCaught = True
        self.assertEqual(exceptionCaught, True,
                         'Detaching lun-0 should result in an '
                         'exception.  vm: ' + node_name + ', '
                         'lun-0 disk: ' + lun0_disk)

    def _detach_disk(self, node_name, vhd_name, allow_lun0=False):
        print("detach disk " + vhd_name + ", node " + node_name)
        self._manager.detach_disk(node_name, vhd_name, allow_lun0)
        attached = self._manager.is_disk_attached(node_name, vhd_name)
        self.assertEqual(attached, False,
                         'Expected disk: ' +
                         azure_config['test_vhd_name'] +
                         ' should not be attached to vm: ' + node_name)

    def _destroy_disk(self, vhd_name):
        print("destroy disk " + vhd_name)
        self._manager.destroy_disk(vhd_name)
        disks = self._list_disks()
        found = False
        for disk in disks:
            if disk.name == vhd_name:
                found = True
                break
        self.assertEqual(found, False, 'Expected disk: ' + vhd_name +
                         ' should not to be listed in '
                         'DiskManager.list_disks')

    def test_delete_lun_0(self):
        node_name = azure_config['test_vm_name']
        vhd_name = azure_config['test_vhd_name']

        # does the VM have an existing disk attached to Lun 0?
        # if so, we will not remove it at end of the test
        lun0_exists = self._has_lun0_disk(node_name)

        # create the test vhd
        self._create_disk(vhd_name, 2)

        # attach it
        self._attach_disk(node_name, vhd_name, 2)

        # make sure there is a disk on lun 0
        lun0_disk = self._check_for_lun_0_disk(node_name, vhd_name)

        # attempt to delete the disk on lun 0
        self._try_to_detach_disk_on_lun_0(node_name, lun0_disk)

        # detach the test vhd
        self._detach_disk(node_name, vhd_name)

        # delete the test_vhd
        self._destroy_disk(vhd_name)

        # to return VM to proper state, if lun 0 was not present
        # at start of test, then clean up to original state
        if not lun0_exists:
            self._detach_disk(node_name, lun0_disk, True)
            self._destroy_disk(lun0_disk)

    def test_create_blank_vhd(self):
        # create a blank vhd and verify the link
        link = self._create_disk(azure_config['test_vhd_name'], 2)
        self.assertEqual(link,
                         'https://' +
                         self._page_blob_service.account_name +
                         '.blob.core.windows.net/' +
                         azure_config['storage_account_container'] +
                         '/' + azure_config['test_vhd_name'] + '.vhd')

        # delete the test vhd
        self._destroy_disk(azure_config['test_vhd_name'])

