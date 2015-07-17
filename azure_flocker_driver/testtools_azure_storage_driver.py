# Copyright Hybrid Logic Ltd. and EMC Corporation.
# See LICENSE file for details.

"""
Azure Test helpers for ``flocker.node.agents``.
"""

import os
import yaml
import time
import sys

from zope.interface.verify import verifyObject
from zope.interface import implementer

from twisted.trial.unittest import SynchronousTestCase, SkipTest
from twisted.python.components import proxyForInterface

from .azure_storage_driver import azure_driver_from_configuration

azure_config = None
config_file_path = os.environ.get('AZURE_CONFIG_FILE')

if config_file_path is not None:
    config_file = open(config_file_path)
    config = yaml.load(config_file.read())
    azure_config = config['azure_settings']


def azure_test_driver_from_yaml(test_case):
    """
    Create a ``azure.scaleio.ScaleIO`` using credentials from a
    test_azure_storage.yaml (TODO move these to config file)
    :returns: An instance of ``scaleiopy.scaleio.ScaleIO`` authenticated
    """
    
    if azure_config == None:
        raise SkipTest(
            'Supply the path to a test config file '
            'using the AZURE_CONFIG_FILE environment variable. '
            'See: '
            'https://docs.clusterhq.com/en/latest/gettinginvolved/acceptance-testing.html '  # noqa
            'for details of the expected format.'
        )

    driver = azure_driver_from_configuration(
            service_name=azure_config['service_name'],
            subscription_id=azure_config['subscription_id'],
            storage_account_name=azure_config['storage_account_name'],
            certificate_data_path=azure_config['management_certificate_path'],
            storage_account_key=azure_config['storage_account_key'],
            disk_container_name=azure_config['disk_container_name']
        )

    test_case.addCleanup(driver.detach_delete_all_disks)
    return driver;
# def tidy_scaleio_client_for_test(test_case):
#     """
#     Return a ``scaleiopy.scaleio.ScaleIO`` whose ScaleIO API is a
#     wrapped by a ``TidyScaleIOVolumeManager`` and register a ``test_case``
#     cleanup callback to remove any volumes that are created during each test.
#     """
#     client, pd, sp = scaleio_client_from_environment()
#     client = TidyScaleIOVolumeManager(client)
#     test_case.addCleanup(client._cleanup)
#     return client, pd, sp
