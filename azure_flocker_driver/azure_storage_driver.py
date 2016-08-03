from uuid import UUID
import socket
import sys
from bitmath import Byte, GiB
from azure_utils.arm_disk_manager import DiskManager
from eliot import Message, to_file
from zope.interface import implementer

from azure.storage.blob import PageBlobService
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient

from lun import Lun
from flocker.node.agents.blockdevice import IBlockDeviceAPI, \
    BlockDeviceVolume, UnknownVolume, UnattachedVolume


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
        self._azure_storage_client = PageBlobService(
            account_name=azure_config['storage_account_name'],
            account_key=azure_config['storage_account_key'])
        self._manager = DiskManager(self._resource_client,
                                    self._compute_client,
                                    self._azure_storage_client,
                                    azure_config['storage_account_container'],
                                    azure_config['group_name'],
                                    azure_config['location'])
        self._storage_account_name = azure_config['storage_account_name']
        self._disk_container_name = azure_config['storage_account_container']
        self._resource_group = azure_config['group_name']

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

        disk_label = self._disk_label_for_dataset_id(dataset_id)
        self._manager.create_disk(disk_label, size_in_gb)

        return BlockDeviceVolume(
            blockdevice_id=dataset_id,
            size=size,
            attached_to=None,
            dataset_id=UUID(dataset_id))

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
        disks = self._manager.list_disks()
        target_disk = None
        for disk in disks:
            if disk.name == self._disk_label_for_dataset_id(blockdevice_id):
                target_disk = disk
                break

        if target_disk is None:
            raise UnknownVolume(blockdevice_id)

        self._manager.destroy_disk(target_disk.name)

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

        # Make sure disk is present.  Also, need the disk size is needed.
        disks = self._manager.list_disks()
        target_disk = None
        for disk in disks:
            if disk.name == self._disk_label_for_dataset_id(blockdevice_id):
                target_disk = disk
                break
        if target_disk is None:
            raise UnknownVolume(blockdevice_id)

        self._manager.attach_disk(
            str(attach_to),
            target_disk.name,
            int(GiB(bytes=target_disk.properties.content_length)))

        log_info('disk attached')

        return self._blockdevicevolume_from_azure_volume(
            blockdevice_id,
            target_disk.properties.content_length,
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

        (target_disk, vm_name, lun) = \
            self._get_disk_vmname_lun(blockdevice_id)

        if target_disk is None:
            raise UnknownVolume(blockdevice_id)

        if lun is None:
            raise UnattachedVolume(blockdevice_id)

        self._manager.detach_disk(vm_name, target_disk)

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

        (target_disk, vm_name, lun) = \
            self._get_disk_vmname_lun(blockdevice_id)

        if target_disk is None:
            raise UnknownVolume(blockdevice_id)

        if lun is None:
            raise UnattachedVolume(blockdevice_id)

        return Lun.get_device_path_for_lun(lun)

    def _get_details_for_disks(self, disks_in):
        """
        Give a list of disks, returns a ''list'' of ''BlockDeviceVolume''s
        """
        disk_info = []
        disks = dict((d.name, d) for d in disks_in)

        # first handle disks attached to vms
        vms = self._compute_client.virtual_machines.list(self._resource_group)
        for vm in vms:
            for data_disk in vm.storage_profile.data_disks:
                if 'flocker-' in data_disk.name:
                    disk_name = data_disk.name.replace('.vhd','')
                    if disk_name in disks:
                        disk_info.append(
                            self._blockdevicevolume_from_azure_volume(
                                disk_name,
                                self._gibytes_to_bytes(data_disk.disk_size_gb),
                                vm.name))
                        del disks[disk_name]
                    else:
                        # We have a data disk mounted that isn't in the known
                        # list of blobs.
                        log_info(
                            "Disk attached, but not known in container: " +
                            str(self._dataset_id_for_disk_label(disk_name)))

        # each remaining disk should be added as not attached
        for disk in disks:
            if 'flocker-' in disk:
                disk_info.append(self._blockdevicevolume_from_azure_volume(
                                 disk.replace('.vhd', ''),
                                 disks[disk].properties.content_length,
                                 None))

        return disk_info

    def list_volumes(self):
        """
        List all the block devices available via the back end API.
        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """
        disks = self._manager.list_disks()
        disk_list = self._get_details_for_disks(disks)
        return disk_list

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
        vm_name = None

        disk_list = self._manager.list_disks()
        for disk in disk_list:
            if 'flocker-' not in disk.name:
                continue
            if disk.name == self._disk_label_for_dataset_id(blockdevice_id):
                target_disk = disk
                break

        vm_info = None
        vm_disk_info = None
        vms = self._compute_client.virtual_machines.list(self._resource_group)
        for vm in vms:
            for disk in vm.storage_profile.data_disks:
                if disk.name == target_disk.name:
                    vm_disk_info = disk
                    vm_info = vm
                    break
            if vm_disk_info is not None:
                break
        if vm_info is not None:
            vm_name = vm_info.name
            target_lun = vm_disk_info.lun

        return (target_disk.name, vm_name, target_lun)

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

    return AzureStorageBlockDeviceAPI(**config)
