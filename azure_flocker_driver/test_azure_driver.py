# Copyright Hybrid Logic Ltd. and EMC Corporation.
# See LICENSE file for details.

"""
Functional tests for
``flocker.node.agents.blockdevice.EMCScaleIOBlockDeviceAPI``
using a real Scaleio cluster.
Ideally emc drivers should be seperate like cinder driver,
we may change thay in the future.
"""

from uuid import uuid4, UUID

from bitmath import Byte, GiB

from twisted.trial.unittest import SynchronousTestCase

from flocker.testtools import skip_except
from twisted.internet import reactor

from azure_storage_driver import azure_driver_from_configuration
from .testtools_azure_storage_driver import azure_driver_from_yaml

from flocker.node.agents.test.test_blockdevice import (
    make_iblockdeviceasyncapi_tests, make_iblockdeviceapi_tests
)


def azureblockdeviceasyncapi_for_test(test_case):
    """
    Create a ``EMCScaleIOBlockDeviceAPI`` instance for use in tests.
    :returns: A ``EMCCinderBlockDeviceAPI`` instance
    """


    return azure_test_driver_from_yaml(test_case)

def azure_factory():
    return make_iblockdeviceasyncapi_tests(azureblockdeviceasyncapi_for_test)

@skip_except(
    supported_tests=[
        'test_interface',
        'test_list_volume_empty',
        'test_listed_volume_attributes',
        'test_created_is_listed',
        'test_created_volume_attributes',
        'test_destroy_unknown_volume',
        'test_destroy_volume',
        'test_destroy_destroyed_volume',
        'test_attach_unknown_volume',
        'test_attach_attached_volume',
        'test_attach_elsewhere_attached_volume',
        'test_attach_unattached_volume',
        'test_attached_volume_listed',
        'test_attach_volume_validate_size',
        'test_list_attached_and_unattached',
        'test_multiple_volumes_attached_to_host',
        'test_detach_unknown_volume',
        'test_detach_detached_volume',
        'test_detach_volume',
        'test_reattach_detached_volume',
        'test_attach_destroyed_volume',
        'test_get_device_path_unknown_volume',
        'test_get_device_path_unattached_volume',
        'test_get_device_path_device',
        'test_get_device_path_device_repeatable_results',
        'test_device_size',
        'test_compute_instance_id_nonempty',
        'test_compute_instance_id_unicode'
    ]
)

class AzureStorageBlockDeviceAPIInterfaceTests(
    make_iblockdeviceapi_tests(
        blockdevice_api_factory=(
                lambda test_case: azureblockdeviceasyncapi_for_test(test_case)
            ),
        minimum_allocatable_size=int(GiB(1).to_Byte().value),
        device_allocation_unit=int(GiB(1).to_Byte().value),
        unknown_blockdevice_id_factory=lambda test: unicode(uuid4())
    )

):
    """
    something
    """
