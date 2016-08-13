"""
Functional tests for
``flocker.node.agents.blockdevice.AzureStorageBlockDeviceAPI``
"""
import bitmath
import logging
import os
from uuid import uuid4
import yaml

from flocker.node.agents import blockdevice
from flocker.node.agents.test.test_blockdevice import (
    make_iblockdeviceapi_tests)
from twisted.python.components import proxyForInterface
from zope.interface import implementer

from azure_storage_driver import (
    azure_driver_from_configuration
)

MIN_ALLOCATION_SIZE = bitmath.GiB(1).bytes
MIN_ALLOCATION_UNIT = MIN_ALLOCATION_SIZE

LOG = logging.getLogger(__name__)


@implementer(blockdevice.IBlockDeviceAPI)
class TestDriver(proxyForInterface(blockdevice.IBlockDeviceAPI, 'original')):
    """Wrapper around driver class to provide test cleanup."""
    def __init__(self, original):
        self.original = original
        self.volumes = {}

    def _cleanup(self):
        """Clean up testing artifacts."""
        with self.original._client.open_connection() as api:
            for vol in self.volumes.keys():
                # Make sure it has been cleanly removed
                try:
                    self.original.detach_volume(self.volumes[vol])
                except Exception:
                    pass

                try:
                    api.delete_volume(vol)
                except Exception:
                    LOG.exception('Error cleaning up volume.')

    def create_volume(self, dataset_id, size):
        """Track all volume creation."""
        blockdevvol = self.original.create_volume(dataset_id, size)
        self.volumes[u"%s" % dataset_id] = blockdevvol.blockdevice_id
        return blockdevvol


def create_driver(**config):
    return azure_driver_from_configuration(
        client_id=config['client_id'],
        client_secret=config['client_secret'],
        tenant_id=config['tenant_id'],
        subscription_id=config['subscription_id'],
        storage_account_name=config['storage_account_name'],
        storage_account_key=config['storage_account_key'],
        storage_account_container=config['storage_account_container'],
        group_name=config['group_name'],
        location=config['location'],
        debug=config['debug'])


def api_factory(test_case):
    """Create a test instance of the block driver.

    :param test_case: The specific test case instance.
    :return: A test configured driver instance.
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-7s [%(threadName)-19s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        filename='../driver.log')
    test_config_path = os.environ.get(
        'FLOCKER_CONFIG',
        '../example.azure_agent.yml')
    if not os.path.exists(test_config_path):
        raise Exception('Functional test configuration not found.')

    with open(test_config_path) as config_file:
        config = yaml.load(config_file.read())

    config = config.get('dataset', {})
    test_driver = TestDriver(
        create_driver(
            **config))
    test_case.addCleanup(test_driver._cleanup)
    return test_driver


class AzureStorageBlockDeviceAPIInterfaceTests(
    make_iblockdeviceapi_tests(
        blockdevice_api_factory=(
            lambda test_case: api_factory(test_case)
        ),
        minimum_allocatable_size=MIN_ALLOCATION_SIZE,
        device_allocation_unit=MIN_ALLOCATION_UNIT,
        unknown_blockdevice_id_factory=lambda test: unicode(uuid4()))):
    pass
