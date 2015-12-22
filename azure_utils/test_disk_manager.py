from arm_disk_manager import DiskManager
from auth_token import AuthToken
from twisted.trial import unittest
from eliot import Message, Logger
from bitmath import Byte, GiB
from twisted.trial.unittest import SkipTest
from azure.storage.blob import BlobService
from azure.mgmt.common import SubscriptionCloudCredentials
from azure.mgmt.resource import ResourceManagementClient
from vhd import Vhd
import os
import yaml
import socket

_logger = Logger()
azure_config = None
config_file_path = os.environ.get('AZURE_CONFIG_FILE')

if config_file_path is not None:
    config_file = open(config_file_path)
    config = yaml.load(config_file.read())
    azure_config = config['azure_settings']

class DiskCreateTestCase(unittest.TestCase):
    def test_create_blank_vhd(self):
        auth_token = AuthToken.get_token_from_client_credentials(
            azure_config['subscription_id'],
            azure_config['tenant_id'],
            azure_config['client_id'],
            azure_config['client_secret'])
        creds = SubscriptionCloudCredentials(azure_config['subscription_id'], auth_token)
        self._resource_client = ResourceManagementClient(creds)
        self._azure_storage_client = BlobService(
            azure_config['storage_account_name'],
            azure_config['storage_account_key'])
        link = Vhd.create_blank_vhd(self._azure_storage_client, 
            azure_config['storage_account_container'],
            'sometestblob.vhd',
            int(GiB(2).to_Byte().value))
        self.assertEqual(link , 'https://sedouardubuntujb.blob.core.windows.net/flocker/sometestblob.vhd')
        self._azure_storage_client.delete_container(azure_config['storage_account_container'])

    def test_attach_blank_vhd(self):
        auth_token = AuthToken.get_token_from_client_credentials(
            azure_config['subscription_id'],
            azure_config['tenant_id'],
            azure_config['client_id'],
            azure_config['client_secret'])
        creds = SubscriptionCloudCredentials(azure_config['subscription_id'], auth_token)
        self._resource_client = ResourceManagementClient(creds)

        manager = DiskManager(self._resource_client, azure_config['group_name'],
            azure_config['location'])
        creds = SubscriptionCloudCredentials(azure_config['subscription_id'], auth_token)
        self._resource_client = ResourceManagementClient(creds)
        self._azure_storage_client = BlobService(
            azure_config['storage_account_name'],
            azure_config['storage_account_key'])

        link = Vhd.create_blank_vhd(self._azure_storage_client, 
            azure_config['storage_account_container'],
            'sometestblob.vhd',
            int(GiB(2).to_Byte().value))
        print "Attempting to attach disk: " + link
        node_name = unicode(socket.gethostname())
        manager.attach_disk(node_name, 'test_vhd', link, 2)

        disks = manager.list_disks(node_name)
        found = false
        for i in range(len(disks)):
            disk = disks[i]
            if disk.name == 'test_vhd':
                found = true
                break

        self.assertEqual(found, true, 'Expected to find an attached disk: ' + 'test_vhd')