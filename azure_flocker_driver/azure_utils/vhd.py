import datetime
import uuid
import os
import time


class AzureOperationFailed(Exception):

    def __init__(self):
            pass


class Vhd(object):

    def __init__():
        return

    @staticmethod
    def create_blank_vhd(azure_storage_client,
                         container_name,
                         name,
                         size_in_bytes):
        # VHD size must be aligned on a megabyte boundary.  The
        # current calling function converts from gigabytes to bytes,
        # but ideally a check should be added.
        #
        # The blob itself, must include a footer which is an additional
        # 512 bytes.  So, the size is increased accordingly to allow
        # for the vhd footer.
        size_in_bytes_with_footer = size_in_bytes + 512

        # Create a new page blob as a blank disk
        azure_storage_client.create_container(container_name)
        azure_storage_client.create_blob(
            container_name=container_name,
            blob_name=name,
            content_length=size_in_bytes_with_footer)

        # for disk to be a valid vhd it requires a vhd footer
        # on the last 512 bytes
        vhd_footer = Vhd.generate_vhd_footer(size_in_bytes)
        azure_storage_client.update_page(
            container_name=container_name,
            blob_name=name,
            page=vhd_footer,
            start_range=size_in_bytes_with_footer-512,
            end_range=size_in_bytes_with_footer-1)

        # for on-prem and azure china to override via env
        if 'STORAGE_HOST_NAME' in os.environ:
            storage_host_name = os.environ['STORAGE_HOST_NAME']
        else:
            storage_host_name = 'blob.core.windows.net'
        return('https://' + azure_storage_client.account_name +
               '.' + storage_host_name + '/' + container_name +
               '/' + name)

    @staticmethod
    def calculate_geometry(size):
        # this value taken from how Azure generates geometry values for VHDs
        vhd_sector_length = 512

        total_sectors = size / vhd_sector_length
        if total_sectors > 65535 * 16 * 255:
            total_sectors = 65535 * 16 * 255

        if total_sectors > 65535 * 16 * 63:
            sectors_per_track = 255
            heads = 16
            cylinder_times_heads = int(total_sectors / sectors_per_track)
        else:
            sectors_per_track = 17
            cylinder_times_heads = int(total_sectors / sectors_per_track)

            heads = int((cylinder_times_heads + 1023) / 1024)
            if heads < 4:
                heads = 4

            if cylinder_times_heads >= (heads * 1024) or heads > 16:
                sectors_per_track = 31
                heads = 16
                cylinder_times_heads = int(total_sectors / sectors_per_track)

            if cylinder_times_heads >= (heads * 1024):
                sectors_per_track = 63
                heads = 16
                cylinder_times_heads = int(total_sectors / sectors_per_track)
        cylinders = int(cylinder_times_heads / heads)

        return cylinders, heads, sectors_per_track

    @staticmethod
    def generate_vhd_footer(size):
        """
        Generate a binary VHD Footer
        # Fixed VHD Footer Format Specification
        # spec:
        # https://technet.microsoft.com/en-us/virtualization/bb676673.aspx#E3B
        # Field         Size (bytes)
        # Cookie        8
        # Features      4
        # Version       4
        # Data Offset   4
        # TimeStamp     4
        # Creator App   4
        # Creator Ver   4
        # CreatorHostOS 4
        # Original Size 8
        # Current Size  8
        # Disk Geo      4
        # Disk Type     4
        # Checksum      4
        # Unique ID     16
        # Saved State   1
        # Reserved      427
        #

        """
        # TODO Are we taking any unreliable dependencies of the content of
        # the azure VHD footer?
        footer_dict = {}
        # the ascii string 'conectix'
        footer_dict['cookie'] = \
            bytearray([0x63, 0x6f, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x78])
        # no features enabled
        footer_dict['features'] = bytearray([0x00, 0x00, 0x00, 0x02])
        # current file version
        footer_dict['version'] = bytearray([0x00, 0x01, 0x00, 0x00])
        # in the case of a fixed disk, this is set to -1
        footer_dict['data_offset'] = \
            bytearray([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                      0xff])
        # hex representation of seconds since january 1st 2000
        footer_dict['timestamp'] = Vhd._generate_timestamp()
        # ascii code for 'wa' = windowsazure
        footer_dict['creator_app'] = bytearray([0x77, 0x61, 0x00, 0x00])
        # ascii code for version of creator application
        footer_dict['creator_version'] = \
            bytearray([0x00, 0x07, 0x00, 0x00])
        # creator host os. windows or mac, ascii for 'wi2k'
        footer_dict['creator_os'] = \
            bytearray([0x57, 0x69, 0x32, 0x6b])
        footer_dict['original_size'] = \
            bytearray.fromhex(hex(size).replace('0x', '').zfill(16))
        footer_dict['current_size'] = \
            bytearray.fromhex(hex(size).replace('0x', '').zfill(16))
        # given the size, calculate the geometry -- 2 bytes cylinders,
        # 1 byte heads, 1 byte sectors
        (cylinders, heads, sectors) = Vhd.calculate_geometry(size)
        footer_dict['disk_geometry'] = \
            bytearray([((cylinders >> 8) & 0xff), (cylinders & 0xff),
                      (heads & 0xff), (sectors & 0xff)])
        # 0x2 = fixed hard disk
        footer_dict['disk_type'] = bytearray([0x00, 0x00, 0x00, 0x02])
        # a uuid
        footer_dict['unique_id'] = bytearray.fromhex(uuid.uuid4().hex)
        # saved state and reserved
        footer_dict['saved_reserved'] = bytearray(428)

        footer_dict['checksum'] = Vhd._compute_checksum(footer_dict)

        return bytes(Vhd._combine_byte_arrays(footer_dict))

    @staticmethod
    def _generate_timestamp():
        hevVal = hex(long(datetime.datetime.now().strftime("%s")) - 946684800)
        return bytearray.fromhex(hevVal.replace(
            'L', '').replace('0x', '').zfill(8))

    @staticmethod
    def _compute_checksum(vhd_data):

        if 'checksum' in vhd_data:
            del vhd_data['checksum']

        wholeArray = Vhd._combine_byte_arrays(vhd_data)

        total = 0
        for byte in wholeArray:
            total += byte

        # ones compliment
        total = ~total

        def tohex(val, nbits):
            return hex((val + (1 << nbits)) % (1 << nbits))

        return bytearray.fromhex(tohex(total, 32).replace('0x', ''))

    @staticmethod
    def _combine_byte_arrays(vhd_data):
        wholeArray = vhd_data['cookie'] \
            + vhd_data['features'] \
            + vhd_data['version'] \
            + vhd_data['data_offset'] \
            + vhd_data['timestamp'] \
            + vhd_data['creator_app'] \
            + vhd_data['creator_version'] \
            + vhd_data['creator_os'] \
            + vhd_data['original_size'] \
            + vhd_data['current_size'] \
            + vhd_data['disk_geometry'] \
            + vhd_data['disk_type']

        if 'checksum' in vhd_data:
            wholeArray += vhd_data['checksum']

        wholeArray += vhd_data['unique_id'] \
            + vhd_data['saved_reserved']

        return wholeArray
