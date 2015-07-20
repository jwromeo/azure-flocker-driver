# Copyright Hybrid Logic Ltd. and EMC Corporation.
# See LICENSE file for details.

"""
Azure Test helpers for ``flocker.node.agents``.
"""

import os
import yaml

from twisted.trial.unittest import SkipTest

from .azure_storage_driver import azure_driver_from_configuration

azure_config = None
config_file_path = os.environ.get('AZURE_CONFIG_FILE')

if config_file_path is not None:
    config_file = open(config_file_path)
    config = yaml.load(config_file.read())
    azure_config = config['azure_settings']


def azure_test_driver_from_yaml(test_case):
    """
    Create a ``azure.Azure.Azure`` using credentials from a
    test_azure_storage.yaml (TODO move these to config file)
    :returns: An instance of ``Azurepy.Azure.Azure`` authenticated
    """

    if azure_config is None:
        raise SkipTest(
            'Supply the path to a test config file '
            'using the AZURE_CONFIG_FILE environment variable. '
            'See: '
            'https://docs.clusterhq.com/en/latest/gettinginvolved/acceptance-testing.html '  # noqa
            'for details of the expected format.'
        )

    driver = azure_driver_from_configuration(azure_config)

    test_case.addCleanup(lambda: driver.detach_delete_all_disks)
    return driver
# def tidy_Azure_client_for_test(test_case):
#     """
#     Return a ``Azurepy.Azure.Azure`` whose Azure API is a
#     wrapped by a ``TidyAzureVolumeManager`` and register a ``test_case``
#     cleanup callback to remove any volumes that are created during each test.
#     """
#     client, pd, sp = Azure_client_from_environment()
#     client = TidyAzureVolumeManager(client)
#     test_case.addCleanup(client._cleanup)
#     return client, pd, sp
