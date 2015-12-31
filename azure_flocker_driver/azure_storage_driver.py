import time
from uuid import UUID
import socket
import os
import sys
from bitmath import Byte, GiB
from azure.servicemanagement import ServiceManagementService
from azure.storage import BlobService
from azure_utils import DiskManager
from eliot import Message, to_file
from zope.interface import implementer

from lun import Lun
from vhd import Vhd
from azure_utils import 
from flocker.node.agents.blockdevice import AlreadyAttachedVolume, \
    IBlockDeviceAPI, BlockDeviceVolume, UnknownVolume, UnattachedVolume


# Logging Helpers
def log_info(message):

    Message.new(Info=message).write()


def log_error(message):

    Message.new(Error=message).write()


class UnsupportedVolumeSize(Exception):
    """
    The volume size is not supported
    Needs to be 1GB allocation granularity
    :param unicode dataset_id: The volume dataset_id
    """

    def __init__(self, dataset_id):
        if not isinstance(dataset_id, UUID):
            raise TypeError(
                'Unexpected dataset_id type. '
                + 'Expected unicode. Got {!r}.'.format(
                    dataset_id))
        Exception.__init__(self, dataset_id)
        self.dataset_id = dataset_id


class AsynchronousTimeout(Exception):

    def __init__(self):
        pass


@implementer(IBlockDeviceAPI)
class AzureStorageBlockDeviceAPI(object):
    """
    An ``IBlockDeviceAsyncAPI`` which uses Azure Storage Backed Block Devices
    Current Support: Azure SMS API
    """

    def __init__(self, **azure_config):
        """
        :param ServiceManagement azure_client: an instance of the azure
        serivce managment api client.
        :param String service_name: The name of the cloud service
        :param
            names of Azure volumes to identify cluster
        :returns: A ``BlockDeviceVolume``.
        """
        self._instance_id = self.compute_instance_id()
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
        self._manager = DiskManager(self._resource_client, 
            self._azure_storage_client,
            azure_config['storage_account_container'],
            azure_config['group_name'],
            azure_config['location'])
        self._storage_account_name = azure_config['storage_account_name']
        self._disk_container_name = azure_config['disk_container_name']

        if azure_config['debug']:
            to_file(sys.stdout)

    def allocation_unit(self):
        """
        1GiB is the minimum allocation unit for azure disks
        return int: 1 GiB
        """

        return int(GiB(1).to_Byte().value)

    def compute_instance_id(self):
        """
        Azure Stored a UUID in the SDC kernel module.
        """

        # Node host names should be unique within a vnet

        return unicode(socket.gethostname())

    def create_volume(self, dataset_id, size):
        """
        Create a new volume.
        :param UUID dataset_id: The Flocker dataset ID of the dataset on this
            volume.
        :param int size: The size of the new volume in bytes.
        :returns: A ``Deferred`` that fires with a ``BlockDeviceVolume`` when
            the volume has been created.
        """

        size_in_gb = Byte(size).to_GiB().value

        if size_in_gb % 1 != 0:
            raise UnsupportedVolumeSize(dataset_id)

        self._manager.create_disk(dataset_id, size_in_gb)

        return BlockDeviceVolume(
            blockdevice_id=str(dataset_id),
            size=size,
            attached_to=None,
            dataset_id=dataset_id)

    def destroy_volume(self, blockdevice_id):
        """
        Destroy an existing volume.
        :param unicode blockdevice_id: The unique identifier for the volume to
            destroy.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :return: ``None``
        """
        log_info('Destorying block device: ' + str(blockdevice_id))
        (target_disk, role_name, lun) = \
            self._get_disk_vmname_lun(blockdevice_id)

        if target_disk is None:

            raise UnknownVolume(blockdevice_id)

        self._manager.destroy_disk(self, str(blockdevice_id))

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

        log_info('Attempting to attach ' + str(blockdevice_id)
                 + ' to ' + str(attach_to))

        self._manager.attach_disk(attach_to, blockdevice_id)
        disk_size = self._attach_disk(blockdevice_id, target_disk, attach_to)

        self._wait_for_attach(blockdevice_id)

        log_info('disk attached')

        return self._blockdevicevolume_from_azure_volume(blockdevice_id,
                                                         disk_size,
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

        if target_disk is None:
            raise UnknownVolume(blockdevice_id)

        if lun is None:
            raise UnattachedVolume(blockdevice_id)

        # contrary to function name it doesn't delete by default, just detachs

        request = \
            self._azure_service_client.delete_data_disk(
                service_name=self._service_name,
                deployment_name=self._service_name,
                role_name=role_name, lun=lun)

        self._wait_for_async(request.request_id, 5000)

        self._wait_for_detach(blockdevice_id)

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

        (target_disk_or_blob, role_name, lun) = \
            self._get_disk_vmname_lun(blockdevice_id)

        if target_disk_or_blob is None:
            raise UnknownVolume(blockdevice_id)

        if lun is None:
            raise UnattachedVolume(blockdevice_id)

        return Lun.get_device_path_for_lun(lun)

    def list_volumes(self):
        """
        List all the block devices available via the back end API.
        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """
        media_url_prefix = 'https://' + self._storage_account_name \
            + '.blob.core.windows.net/' + self._disk_container_name
        disks = self._azure_service_client.list_disks()
        disk_list = []
        all_blobs = self._get_flocker_blobs()
        for d in disks:

            if media_url_prefix not in d.media_link or \
                    'flocker-' not in d.label:
                    continue

            role_name = None

            if d.attached_to is not None \
                    and d.attached_to.role_name is not None:

                    role_name = d.attached_to.role_name

            disk_list.append(self._blockdevicevolume_from_azure_volume(
                d.label, self._gibytes_to_bytes(d.logical_disk_size_in_gb),
                role_name))

            if d.label in all_blobs:
                del all_blobs[d.label]

        for key in all_blobs:
            # include unregistered 'disk' blobs
            disk_list.append(self._blockdevicevolume_from_azure_volume(
                all_blobs[key].name,
                all_blobs[key].properties.content_length,
                None))

        return disk_list

    def _attach_disk(
            self,
            blockdevice_id,
            target_disk,
            attach_to):

        """
        Attaches disk to specified VM
        :param string blockdevice_id: The identifier of the disk
        :param DataVirtualHardDisk/Blob target_disk: The Blob
               or Disk to be attached
        :returns int: The size of the attached disk
        """

        lun = Lun.compute_next_lun(
            self._azure_service_client,
            self._service_name,
            str(attach_to))
        common_params = {
            'service_name': self._service_name,
            'deployment_name': self._service_name,
            'role_name': attach_to,
            'lun': lun
        }
        disk_size = None

        if target_disk.__class__.__name__ == 'Blob':
            # exclude 512 byte footer
            disk_size = target_disk.properties.content_length

            common_params['source_media_link'] = \
                'https://' + self._storage_account_name \
                + '.blob.core.windows.net/' + self._disk_container_name \
                + '/' + blockdevice_id

            common_params['disk_label'] = blockdevice_id

        else:

            disk_size = self._gibytes_to_bytes(
                target_disk.logical_disk_size_in_gb)

            common_params['disk_name'] = target_disk.name

        request = self._azure_service_client.add_data_disk(**common_params)
        self._wait_for_async(request.request_id, 5000)

        return disk_size

    def _create_volume_blob(self, size, dataset_id):
        # Create a new page blob as a blank disk
        self._azure_storage_client.put_blob(
            container_name=self._disk_container_name,
            blob_name=self._disk_label_for_dataset_id(dataset_id),
            blob=None,
            x_ms_blob_type='PageBlob',
            x_ms_blob_content_type='application/octet-stream',
            x_ms_blob_content_length=size)

        # for disk to be a valid vhd it requires a vhd footer
        # on the last 512 bytes
        vhd_footer = Vhd.generate_vhd_footer(size)

        self._azure_storage_client.put_page(
            container_name=self._disk_container_name,
            blob_name=self._disk_label_for_dataset_id(dataset_id),
            page=vhd_footer,
            x_ms_page_write='update',
            x_ms_range='bytes=' + str((size - 512)) + '-' + str(size - 1))

    def _disk_label_for_dataset_id(self, dataset_id):
        """
        Returns a disk label for a given Dataset ID
        :param unicode dataset_id: The identifier of the dataset
        :returns string: A string representing the disk label
        """

        label = 'flocker-' + str(dataset_id)
        return label

    def _dataset_id_for_disk_label(self, disk_label):
        """
        Returns a UUID representing the Dataset ID for the given disk
        label
        :param string disk_label: The disk label
        :returns UUID: The UUID of the dataset
        """
        return UUID(disk_label.replace('flocker-', ''))

    def _get_disk_vmname_lun(self, blockdevice_id):
        target_disk = None
        target_lun = None
        role_name = None
        disk_list = self._azure_service_client.list_disks()

        for d in disk_list:

            if 'flocker-' not in d.label:
                continue
            if d.label == str(blockdevice_id):
                target_disk = d
                break

        if target_disk is None:
            # check for unregisterd disk
            blobs = self._get_flocker_blobs()
            blob = None

            if str(blockdevice_id) in blobs:
                blob = blobs[str(blockdevice_id)]

            return blob, None, None

        vm_info = None

        if hasattr(target_disk.attached_to, 'role_name'):
            vm_info = self._azure_service_client.get_role(
                self._service_name, self._service_name,
                target_disk.attached_to.role_name)

            for d in vm_info.data_virtual_hard_disks:
                if d.disk_name == target_disk.name:
                    target_lun = d.lun
                    break

            role_name = target_disk.attached_to.role_name

        return (target_disk, role_name, target_lun)

    def _get_flocker_blobs(self):
        all_blobs = {}

        blobs = self._azure_storage_client.list_blobs(
            self._disk_container_name,
            prefix='flocker-')

        for b in blobs:
            # todo - this could be big!
            all_blobs[b.name] = b

        return all_blobs

    def _wait_for_detach(self, blockdevice_id):
        role_name = ''
        lun = -1

        timeout_count = 0

        log_info('waiting for azure to ' + 'report disk as detached...')

        while role_name is not None or lun is not None:
            (target_disk, role_name, lun) = \
                self._get_disk_vmname_lun(blockdevice_id)
            time.sleep(1)
            timeout_count += 1

            if timeout_count > 5000:
                raise AsynchronousTimeout()

        log_info('Disk Detached')

    def _wait_for_attach(self, blockdevice_id):
        timeout_count = 0
        lun = None

        log_info('waiting for azure to report disk as attached...')

        while lun is None:
            (target_disk, role_name, lun) = \
                self._get_disk_vmname_lun(blockdevice_id)
            time.sleep(.001)
            timeout_count += 1

            if timeout_count > 5000:
                raise AsynchronousTimeout()

    def _wait_for_async(self, request_id, timeout):
        count = 0
        result = self._azure_service_client.get_operation_status(request_id)
        while result.status == 'InProgress':
            count = count + 1
            if count > timeout:
                log_error('Timed out waiting for async operation to complete.')
                raise AsynchronousTimeout()
            time.sleep(.001)
            log_info('.')
            result = self._azure_service_client.get_operation_status(
                request_id)
            if result.error:
                log_error(result.error.code)
                log_error(str(result.error.message))

        log_error(result.status + ' in ' + str(count * 5) + 's')

    def _gibytes_to_bytes(self, size):

        return int(GiB(size).to_Byte().value)

    def _blockdevicevolume_from_azure_volume(self, label, size,
                                             attached_to_name):

        # azure will report the disk size excluding the 512 byte footer
        # however flocker expects the exact value it requested for disk size
        # so offset the reported size to flocker by 512 bytes
        return BlockDeviceVolume(
            blockdevice_id=unicode(label),
            size=int(size),
            attached_to=attached_to_name,
            dataset_id=self._dataset_id_for_disk_label(label)
        )  # disk labels are formatted as flocker-<data_set_id>


def azure_driver_from_configuration(config):
    """
    Returns Flocker Azure BlockDeviceAPI from plugin config yml.
        :param dictonary config: The Dictonary representing
            the data from the configuration yaml
    """

    # todo return azure storage driver api

    if not os.path.isfile(config['management_certificate_path']):
        raise IOError(
            'Certificate ' +
            config['management_certificate_path'] + ' does not exist')

    return AzureStorageBlockDeviceAPI(**config)
