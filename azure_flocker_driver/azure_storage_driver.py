import time
from uuid import UUID
import logging
import requests
import json
import socket
import re

from subprocess import check_output

from bitmath import Byte, GiB, MiB, KiB

from azure.servicemanagement import ServiceManagement

from eliot import Message, Logger
from zope.interface import implementer, Interface
from twisted.internet import defer, task, reactor
from twisted.python.filepath import FilePath

from flocker.node.agents.blockdevice import (
    AlreadyAttachedVolume, IBlockDeviceAsyncAPI,
    BlockDeviceVolume, UnknownVolume, UnattachedVolume
)

# Eliot is transitioning away from the "Logger instances all over the place"
# approach.  And it's hard to put Logger instances on PRecord subclasses which
# we have a lot of.  So just use this global logger for now.
_logger = Logger()


# Azure's allocation granularity is 1GB
ALLOCATION_GRANULARITY = 1


@implementer(IBlockDeviceAsyncAPI)
class AzureStorageBlockDeviceAPI(object):
    """
    An ``IBlockDeviceAsyncAPI`` which uses Azure Storage Backed Block Devices
    Current Support: Azure SMS
    """

    def __init__(self, azure_client, cluster_id, service_name, storage_account_name):
        """
        :param ScaleIO sio_client: An instance of ScaleIO requests
            client.
        :param UUID cluster_id: An ID that will be included in the
            names of ScaleIO volumes to identify cluster
        :returns: A ``BlockDeviceVolume``.
        """
        self._cluster_id = cluster_id
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
        return socket.gethostname()

    def _compute_next_lun():
        # force the latest scsci info
        subprocess.call('sudo fdisk -l')
        disk_info_string = subprocess.check_output('lsscsi')
        parts = disk_info_string.strip('\n').split('\n')
        lun = -1

        if len(parts) <= 2:
            return 0

        for i in range(2, len(parts)):

            line = parts[i]
            next_lun = int(re.split(':|]', line)[3]);

            if nex_lun - i > 1
                lun = next_lun - 1
                break 
        
            if i == len(parts) - 1:
                lun = next_lun + 1
                break 
        
            lun = next_lun
        
        return lun

    def _get_attached_luns_list():
        # force the latest scsci info
        subprocess.call('sudo fdisk -l')
        disk_info_string = subprocess.check_output('lsscsi')
        parts = disk_info_string.strip('\n').split('\n')
        lun = -1
        lun_list = []

        if len(parts) <= 2:
            return lun_list

        for i in range(2, len(parts)):

            line = parts[i]
            next_lun = int(re.split(':|]', line)[3]);

            lun_list.append(next_lun)

        
        return lun_list

    def _watch_request_deferred(request_id, timeout):
            
            def _wait_for_async(*args):
                request_id = args[0]
                timeout = args[1]
                deferred = args[2]
                count = 0
                result = self._azure_client.get_operation_status(request_id)

                while result.status == 'InProgress':
                    count = count + 1
                    if count > timeout:
                        return deferred.errback('Timed out waiting for async operation to complete.')
                    time.sleep(.1)
                    print('.'),
                    sys.stdout.flush()
                    result = self._azure_client.get_operation_status(request_id)
                    if result.error:
                        print(result.error.code)
                        print(vars(result.error))
                        return deferred.errback(result)
                print result.status + ' in ' + str(count*5) + 's'
                deferred.callback(result)

            deferred = defer.Deferred()
            reactor.callInThread(_wait_for_async, request_id, timeout, deferred)

            return deferred

    def _get_deferred_for_sync(sync_func):
        print 'calling sync wrapper!'

        def run_sync_func(sync_func, deferred):
            try:
                result = sync_func()
                deferred.callback(result)
            except Exception as inst:
                deferred.errback(inst)

        deferred = defer.Deferred()
        reactor.callInThread(run_sync_func, sync_func, deferred)

        return deferred

    def _get_deferred_for_sync_kwargs(sync_func, kwargs):
        print 'calling keword wrapper!'

        def run_sync_func(sync_func, deferred, kwargs):
            try:
                result = sync_func(**kwargs)
                deferred.callback(result)
            except Exception as inst:
                deferred.errback(inst)

        deferred = defer.Deferred()
        reactor.callInThread(run_sync_func, sync_func, deferred, kwargs)

        return deferred

    def _get_deferred_for_sync_args(sync_func, args):

        print 'calling args wrapper!'
        def run_sync_func(sync_func, deferred, args):
            try:
                result = sync_func(*args)
                deferred.callback(result)
            except Exception as inst:
                deferred.errback(inst)

        deferred = defer.Deferred()
        reactor.callInThread(run_sync_func, sync_func, deferred, args)

        return deferred

    def _gibytes_to_bytes(size):

        return int(GiB(size).to_Byte().value)


    def _blockdevicevolume_from_azure_volume(disk_info, dataset_id):

        return BlockDeviceVolume(
            blockdevice_id=disk_info.disk_label,
            size=self.__gibytes_to_bytes(logical_disk_size_in_gb),
            attached_to=self.compute_instance_id()
            dataset_id=dataset_id
            )

    # azure api forces you to attach upon creation
    @check_login
    def create_volume(self, dataset_id, size):
        """
        Create a new volume.
        :param UUID dataset_id: The Flocker dataset ID of the dataset on this
            volume.
        :param int size: The size of the new volume in bytes.
        :returns: A ``Deferred`` that fires with a ``BlockDeviceVolume`` when
            the volume has been created.
        """


        lun = _compute_next_lun()

        def _watch_request(result):
            print 'watching:' + result.request_id
            return _watch_request_deferred(result.request_id, 5000)

        def _azure_volume_info(result):
            args = (self._service_name, self._service_name, self.compute_instance_id(), lun)
            return self._get_deferred_for_sync(self._azure_client.get_data_disk, args)

        def _create_block_device(result):
            return self._blockdevicevolume_from_azure_volume(result, dataset_id)

        def _failed(result):
            print 'error'
            print result

        deferred = self._get_deferred_for_sync_kwargs(self._azure_client.add_data_disk, {'service_name':'sedouard-dokku',
            'deployment_name':'sedouard-dokku',
            'role_name':'sedouard-dokku',
            'lun':1,
            'disk_label': self._disk_label_for_blockdevice_id(dataset_id),
            'disk_name': self._disk_label_for_blockdevice_id(dataset_id),
            'media_link':'https://'+ _storage_account_name + '.blob.core.windows.net/flocker/' + self.compute_instance_id() + '-' + dataset_id,
            'logical_disk_size_in_gb':12,
            'disk_name': })

        deferred.addCallback(_watch_request)
        deferred.addCallback(_azure_volume_info)
        deferred.addCallback(_create_block_device)
        deferred.addErrback(_failed)

    def _disk_label_for_blockdevice_id(blockdevice_id):
        # need to mark flocker disks to differentiate from other
        # disks in subscription disk repository
        return 'flocker-' + blockdevice_id

    @check_login
    def destroy_volume(self, blockdevice_id):
        """
        Destroy an existing volume.
        :param unicode blockdevice_id: The unique identifier for the volume to
            destroy.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :return: ``None``
        """

        def _disk_not_found(result):
            if result.status == 'ResourceNotFound':
                raise UnknownVolume(blockdevice_id)

            raise Exception(result)

        def _delete_disk(result):
            self._get_deferred_for_sync_args(self.azure_client.delete_disk, self._disk_label_for_blockdevice_id(blockdevice_id), True)

        deferred = self._get_deferred_for_sync_args(self.azure_client.get_disk, self._disk_label_for_blockdevice_id(blockdevice_id))
        deferred.addCallback(_delete_disk)
        deferred.addErrback(_disk_not_found)
        
        return deferred

    @check_login
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
        def _disk_not_found(result):
            if result.status == 'ResourceNotFound':
                raise UnknownVolume(blockdevice_id)

            raise Exception(result)

        def _attach_disk(disk):
            if disk.attached_to != None:
                raise AlreadyAttachedVolume(blockdevice_id)

            return self._get_deferred_for_sync_kwargs(self.azure_client.add_data_disk, service_name=self._service_name,
                deployment_name=self._service_name,
                role_name=attach_to,
                lun=self._compute_next_lun(),
                disk_label=self._disk_label_for_blockdevice_id(blockdevice_id),
                media_link='https://'+ _storage_account_name + '.blob.core.windows.net/flocker/' + attach_to + '-' + blockdevice_id)

        def _watch_request(request):
            return self._watch_request_deferred(request.request_id, 5000)

        def _create_disk(result):
            return self._blockdevicevolume_from_azure_volume(result, dataset_id)

        deferred = self._get_deferred_for_sync_args(self.azure_client.get_disk, self._disk_label_for_blockdevice_id(blockdevice_id))
        deferred.addCallback(_attach_disk)
        deferred.addCallback(_watch_request)
        deferred.addErrback(_disk_not_found)

        return deferred


    @check_login
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
        current_lun = 0
        count = 0
        lun_list = self._get_attached_luns_list()
        current_lun = lun_list[0]
  
        if len(lun_list) == 0:
            Message.new(Error="Could Not Detach Volume "
                        + str(blockdevice_id)
                        + "is unattached").write(_logger)
            raise UnattachedVolume(blockdevice_id)

        def _check_if_exists(result):
            if result.status == 'ResourceNotFound':
                raise UnknownVolume(blockdevice_id)

        def _get_data_disk():
            deferred = self._get_deferred_for_sync_kwargs(self._azure_client.get_data_disk,
            service_name=self._service_name,
            deployment_name=self._service_name,
            role_name=self.compute_instance_id(),
            lun=lun)

            return deferred

        def _return(result):
            # todo do we need to swallow the result so that we
            # conform to the interface returning nothing?
            return

        
 
        def _check_if_correct_disk(disk):

            count++

            if disk.disk_label === self._disk_label_for_blockdevice_id(blockdevice_id):
                deferred = self._get_deferred_for_sync_kwargs(self.delete_data_disk,
                    service_name=self._service_name,
                    deployment_name=self.service_name,
                    role_name=self.compute_instance_id(),
                    lun=current_lun)

                deferred.addCallback(_return)

                return deferred

            if count >= len(lun_list)
                raise UnattachedVolume(blockdevice_id)

            # this isn't the right disk, try the next attached one
            current_lun = lun_list[count]

            deferred = self._get_deferred_for_sync_kwargs(self._azure_client.get_data_disk,
                service_name=self._service_name,
                deployment_name=self._service_name,
                role_name=self.compute_instance_id(),
                lun=current_lun)

            deferred.addCallback(_check_if_correct_disk)

            return deferred

        deferred = self._get_deferred_for_sync_args(self.azure_client.get_disk, self._disk_label_for_blockdevice_id(blockdevice_id))
        deferred.addErrback(_check_if_exists)
        deferred.addCallback(_get_data_disk)
        deferred.addCallback(_check_if_correct_disk)

        return deferred

    @check_login
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
        lun_list = self._get_attached_luns_list

        def _check_if_exists(result):
            if result.status == 'ResourceNotFound':
                raise UnknownVolume(blockdevice_id)

        def _get_disk():
            if len(lun_list) === 0
                return UnattachedVolume

            args = (self._service_name, self._service_name, self.compute_instance_id(), lun)
            deferred = self._get_deferred_for_sync_args(self._azure_client.get_data_disk, args)
            return deferred

        def _get_device_path():
            self._get_attached_luns_list

        deferred = self._get_deferred_for_sync(self.get_disk, self._disk_label_for_blockdevice_id(blockdevice_id))
        deferred.addErrback(_check_if_exists)
        deferred.addCallback(_get_disk)
        deferred.addCallback(_)
        deferred.addCallback(_get_device_path)

    @check_login
    def list_volumes(self):
        """
        List all the block devices available via the back end API.
        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """
        
        def _build_volume_list(disks):
            disk_list = []
            for d in disks:
                disk_list.append(self._blockdevicevolume_from_azure_volume(d))

            return disk_list

        deferred = self._get_deferred_for_sync(self._azure_client.list_disks)
        deferred.addCallback(_build_volume_list)

        return deferred

def azure_driver_from_configuration(service_name, subscription_id, certificate_data, debug):
    """
    Returns Flocker ScaleIO BlockDeviceAPI from plugin config yml.
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