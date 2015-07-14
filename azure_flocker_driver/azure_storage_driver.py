import time
import sys
from uuid import UUID
import logging
import requests
import json
import socket
import re
import os
import subprocess

from bitmath import Byte, GiB, MiB, KiB

from azure.servicemanagement import ServiceManagementService

from eliot import Message, Logger
from zope.interface import implementer, Interface
from twisted.internet import defer, task, reactor
from twisted.python.filepath import FilePath

from flocker.node.agents.blockdevice import AlreadyAttachedVolume, \
    IBlockDeviceAPI, BlockDeviceVolume, UnknownVolume, UnattachedVolume

# Eliot is transitioning away from the "Logger instances all over the place"
# approach.  And it's hard to put Logger instances on PRecord subclasses which
# we have a lot of.  So just use this global logger for now.

_logger = Logger()

# Azure's allocation granularity is 1GB

ALLOCATION_GRANULARITY = 1


class Lun(object):

    device_path = ''
    lun = ''

    def __init__():
        return

    @staticmethod
    def compute_next_lun():

        # force the latest scsci info

        with open(os.devnull, 'w') as shutup:
            subprocess.call(['sudo', 'fdisk', '-l'], stdout=shutup,
                            stderr=shutup)
        disk_info_string = subprocess.check_output('lsscsi')
        parts = disk_info_string.strip('\n').split('\n')

        lun = -1
        count = 0
        for i in range(0, len(parts)):

            line = parts[i]
            segments = re.split(':|]|\[', line)

            print 'computed nextlun:'

            if int(segments[1]) != 5:
                continue

            next_lun = int(segments[4])

            if next_lun - count >= 1:
                lun = next_lun - 1
                break

            if i == len(parts) - 1:
                lun = next_lun + 1
                break
            count += 1
            lun = next_lun

        return lun

    @staticmethod
    def get_attached_luns_list():

        # force the latest scsci info

        with open(os.devnull, 'w') as shutup:
            subprocess.call(['sudo', 'fdisk', '-l'], stdout=shutup,
                            stderr=shutup)
        disk_info_string = subprocess.check_output('lsscsi')
        parts = disk_info_string.strip('\n').split('\n')
        lun = -1
        lun_list = []

        if len(parts) <= 2:
            return lun_list

        for i in range(2, len(parts)):
            lun = Lun()
            line = parts[i]
            segments = re.split(':|]| ', line)
            lun.lun = int(segments[3])
            lun.device_path = segments(segments[len(segments) - 1])

            lun_list.append(lun)

        return lun_list

    # Returns a string representing the block device path based
    # on a provided lun slot

    @staticmethod
    def get_device_path_for_lun(lun):

        if lun > 32:
            raise Exception('valid lun parameter is 0 - 32, inclusive')
        base = '/dev/sd'

        # luns go 0-32

        ascii_base = ord('c')

        return '/dev/sd' + chr(ascii_base + lun)


class UnsupportedVolumeSize(Exception):

    """
    The volume size is not supported
    Needs to be 8GB allocation granularity
    :param unicode dataset_id: The volume dataset_id
    """

    def __init__(self, dataset_id):
        if not isinstance(dataset_id, UUID):
            raise TypeError('Unexpected dataset_id type. Expected unicode. Got {!r}.'.format(dataset_id))
        Exception.__init__(self, dataset_id)
        self.dataset_id = dataset_id


class AzureStorageBlockDeviceAPI(object):

    """
    An ``IBlockDeviceAsyncAPI`` which uses Azure Storage Backed Block Devices
    Current Support: Azure SMS
    """

    def __init__(
        self,
        azure_client,
        service_name,
        deployment_name,
        storage_account_name,
        ):
        """
        :param ServiceManagement azure_client: an instance of the azure 
        serivce managment api client.
        :param String service_name: The name of the service. For SMS api deployments
        this is the cloud service name
        :param 
            names of ScaleIO volumes to identify cluster
        :returns: A ``BlockDeviceVolume``.
        """

        self._instance_id = self.compute_instance_id()
        self._azure_client = azure_client
        self._service_name = service_name
        self._storage_account_name = storage_account_name

    def allocation_unit(self):
        """
        1GiB is the minimum allocation unit for azure disks
        return int: 1 GiB
        """

        return int(GiB(1).to_Byte().value)

    def compute_instance_id(self):
        """
        ScaleIO Stored a UUID in the SDC kernel module.
        """

        # Node host names should be unique within a vnet

        return unicode(socket.gethostname())

    def _wait_for_async(self, request_id, timeout):
        count = 0
        result = self._azure_client.get_operation_status(request_id)
        while result.status == 'InProgress':
            count = count + 1
            if count > timeout:
                print 'Timed out waiting for async operation to complete.'
                return
            time.sleep(5)
            print '.',
            sys.stdout.flush()
            result = self._azure_client.get_operation_status(request_id)
            if result.error:
                print result.error.code
                print vars(result.error)
        print result.status + ' in ' + str(count * 5) + 's'

    def _gibytes_to_bytes(self, size):

        return int(GiB(size).to_Byte().value)

    def _blockdevicevolume_from_azure_volume(self, disk,
            attached_to_name=None):

        label = None

        if disk.__class__.__name__ == 'DataVirtualHardDisk':

            # this is returned by a callt o get_data_disk

            label = disk.disk_label
        else:
            label = disk.label
            if disk.attached_to != None:
                attached_to_name = disk.attached_to.role_name

                return BlockDeviceVolume(blockdevice_id=unicode(self._blockdevice_id_for_disk_label(label)),
                        size=self._gibytes_to_bytes(disk.logical_disk_size_in_gb),
                        attached_to=attached_to_name,
                        dataset_id=self._blockdevice_id_for_disk_label(label))  # disk labels are formatted as flocker-<data_set_id>

    def _compute_next_remote_lun(self, role_name):
        vm_info = self._azure_client.get_role(self._service_name,
                self._service_name, role_name)
        vm_info.data_virtual_hard_disks = \
            sorted(vm_info.data_virtual_hard_disks, key=lambda obj: \
                   obj.lun)
        lun = 0
        for i in range(0, len(vm_info.data_virtual_hard_disks)):
            next_lun = vm_info.data_virtual_hard_disks[i].lun

            if next_lun - i >= 1:
                lun = next_lun - 1
                break

            if i == len(vm_info.data_virtual_hard_disks) - 1:
                lun = next_lun + 1
                break

        return lun

    def create_volume(self, dataset_id, size):
        """
        Create a new volume.
        :param UUID dataset_id: The Flocker dataset ID of the dataset on this
            volume.
        :param int size: The size of the new volume in bytes.
        :returns: A ``Deferred`` that fires with a ``BlockDeviceVolume`` when
            the volume has been created.
        """

        lun = Lun.compute_next_lun()

        size_in_gb = Byte(size).to_GiB().value

        if size_in_gb % 1 != 0:
            raise UnsupportedVolumeSize(dataset_id)

        # print 'creating disk for dataset: '
        # print dataset_id
        # print 'service_name/deployment_name:' + self._service_name
        # print 'role_name' + self.compute_instance_id()
        # print 'lun:' + str(lun)
        # print 'disk_label:' \
        #     + self._disk_label_for_blockdevice_id(dataset_id)
        # print 'media_link:' + 'https://' + self._storage_account_name \
        #     + '.blob.core.windows.net/flocker/' \
        #     + self.compute_instance_id() + '-' \
        #     + self._disk_label_for_blockdevice_id(dataset_id)
        # print 'size:' + str(size_in_gb)

        # azure api only allows us to create a data disk when we are
        # attaching. so to work-around we have to attach to this node
        # and then detach

        request = self._azure_client.add_data_disk(
            service_name=self._service_name,
            deployment_name=self._service_name,
            role_name=self.compute_instance_id(),
            lun=lun,
            media_link='https://' + self._storage_account_name
                + '.blob.core.windows.net/flocker/'
                + self.compute_instance_id() + '-'
                + self._disk_label_for_blockdevice_id(dataset_id),
            disk_label=self._disk_label_for_blockdevice_id(dataset_id),
            logical_disk_size_in_gb='{0:.0f}'.format(size_in_gb),
            )

        self._wait_for_async(request.request_id, 5000)

        # now, awkwardly detach this disk from this node since the calle
        # didn't actually ask us to attach this disk anywhere

        disk = self._azure_client.get_data_disk(self._service_name,
                self._service_name, self.compute_instance_id(), lun)

        volume = self._blockdevicevolume_from_azure_volume(disk)

        self.detach_volume(volume.blockdevice_id)
        return volume

    def _disk_label_for_blockdevice_id(self, blockdevice_id):

        # need to mark flocker disks to differentiate from other
        # disks in subscription disk repository

        label = 'flocker-' + str(blockdevice_id)
        return label

    def _blockdevice_id_for_disk_label(self, disk_label):
        return UUID(disk_label.replace('flocker-', ''))

    def destroy_volume(self, blockdevice_id):
        """
        Destroy an existing volume.
        :param unicode blockdevice_id: The unique identifier for the volume to
            destroy.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :return: ``None``
        """

        (target_disk, role_name, lun) = \
            self._get_disk_vmname_lun(blockdevice_id)

        if target_disk == None:
            return UnknownVolume(blockdevice_id)

        if target_disk.attached_to != None:
            request = \
                self.delete_data_disk(service_name=self._service_name,
                    deployment_name=self.service_name,
                    role_name=role_name, lun=lun, delete_vhd=True)
        else:
            request = self._azure_client.delete_disk(target_disk.name,
                    True)
            self._wait_for_async(request.request_id, 5000)

    def attach_volume(self, blockdevice_id, attach_to):
        """
        Attach ``blockdevice_id`` to ``host``.
        :param unicode blockdevice_id: The unique identifier for the block
            device being attached.
        :param unicode attach_to: An identifier like the one returned by the
            ``compute_instance_id`` method indicating the node to which to
            attach the volume.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises AlreadyAttachedVolume: If the supplied ``blockdevice_id`` is
            already attached.
        :returns: A ``BlockDeviceVolume`` with a ``host`` attribute set to
            ``host``.
        """

        print 'attchinb volume for blockdevice:'
        print blockdevice_id
        (target_disk, role_name, lun) = \
            self._get_disk_vmname_lun(blockdevice_id)
        print 'role name:'
        print role_name
        print 'lun:'
        print lun
        if lun != None:
            raise AlreadyAttachedVolume(blockdevice_id)

        print 'attaching disk:'
        print target_disk.name
        remote_lun = self._compute_next_remote_lun(str(attach_to))
        print 'to: ' + str(attach_to) + 'at lun:' + str(remote_lun)

        request = \
            self._azure_client.add_data_disk(service_name=self._service_name,
                deployment_name=self._service_name,
                role_name=str(attach_to), lun=remote_lun,
                disk_name=target_disk.name)

        self._wait_for_async(request.request_id, 5000)
        return self._blockdevicevolume_from_azure_volume(target_disk,
                attach_to)

    def detach_volume(self, blockdevice_id):
        """
        Detach ``blockdevice_id`` from whatever host it is attached to.
        :param unicode blockdevice_id: The unique identifier for the block
            device being detached.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to anything.
        :returns: ``None``
        """

        (target_disk, role_name, lun) = \
            self._get_disk_vmname_lun(blockdevice_id)

        if lun == None:
            raise UnattachedVolume(blockdevice_id)

        # contrary to function name it doesn't delete by default, just detachs

        request = \
            self._azure_client.delete_data_disk(service_name=self._service_name,
                deployment_name=self._service_name,
                role_name=role_name, lun=lun)

        self._wait_for_async(request.request_id, 5000)

        timeout = 25
        timeout_count = 0
        print 'waiting for azure to repot disk as detached...'
        while role_name != None or lun != None:
            (target_disk, role_name, lun) = \
                self._get_disk_vmname_lun(blockdevice_id)
            time.sleep(1)
            timeout_count += 1

    def get_device_path(self, blockdevice_id):
        """
        Return the device path that has been allocated to the block device on
        the host to which it is currently attached.
        :param unicode blockdevice_id: The unique identifier for the block
            device.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to a host.
        :returns: A ``FilePath`` for the device.
        """

        (target_disk, role_name, lun) = \
            self._get_disk_vmname_lun(blockdevice_id)

        if lun == None:
            raise UnattachedVolume(blockdevice_id)

        return Lun.get_device_path_for_lun(lun)

    def list_volumes(self):
        """
        List all the block devices available via the back end API.
        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """

        disks = self._azure_client.list_disks()
        disk_list = []
        for d in disks:

            # flocker disk labels are formatted as 'flocker-DATASET_ID_GUID'

            if not 'flocker-' in d.label:
                continue
            disk_list.append(self._blockdevicevolume_from_azure_volume(d))

        return disk_list

    def _get_disk_vmname_lun(self, blockdevice_id):
        target_disk = None
        target_lun = None
        role_name = None
        disk_list = self._azure_client.list_disks()

        for d in disk_list:

            if not 'flocker-' in d.label:
                continue

            if str(self._blockdevice_id_for_disk_label(d.label)) \
                == str(blockdevice_id):

                target_disk = d
                break

        if target_disk == None:
            raise UnknownVolume(blockdevice_id)

        vm_info = None

        if hasattr(target_disk.attached_to, 'role_name'):
            vm_info = self._azure_client.get_role(self._service_name,
                    self._service_name,
                    target_disk.attached_to.role_name)

            for d in vm_info.data_virtual_hard_disks:

                # azure api has two similar but different disk object types, one which
                # names disk fields like 'disk_name' and others will simply just be 'name'

                if d.disk_name == target_disk.name:
                    target_lun = d.lun
                    break

            role_name = target_disk.attached_to.role_name

        return (target_disk, role_name, target_lun)


def azure_driver_from_configuration(
    service_name,
    subscription_id,
    storage_account_name,
    certificate_data_path,
    debug=None,
    ):
    """
    Returns Flocker Azure BlockDeviceAPI from plugin config yml.
        :param uuid cluster_id: The UUID of the cluster
        :param string username: The username for ScaleIO Driver,
            this will be used to login and enable requests to
            be made to the underlying ScaleIO BlockDeviceAPI
        :param string password: The username for ScaleIO Driver,
            this will be used to login and enable requests to be
            made to the underlying ScaleIO BlockDeviceAPI
        :param unicode mdm_ip: The Main MDM IP address. ScaleIO
            Driver will communicate with the ScaleIO Gateway
            Node to issue REST API commands.
        :param integer port: MDM Gateway The port
        :param string protection_domain: The protection domain
            for this driver instance
        :param string storage_pool: The storage pool used for
            this driver instance
        :param FilePath certificate: An optional certificate
            to be used for optional authentication methods.
            The presence of this certificate will change verify
            to True inside the requests.
        :param boolean ssl: use SSL?
        :param boolean debug: verbosity
    """

    # todo return azure storage driver api

    if not os.path.isfile(certificate_data_path):
        raise IOError('Certificate ' + certificate_data_path
                      + ' does not exist')
    sms = ServiceManagementService(subscription_id,
                                   certificate_data_path)
    return AzureStorageBlockDeviceAPI(sms, service_name, service_name,
            storage_account_name)



            