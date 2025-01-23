from __future__ import annotations

from typing import BinaryIO

from dissect.util.stream import AlignedStream

from dissect.volume.dm.btree import BTree
from dissect.volume.dm.c_dm import SECTOR_SIZE, c_dm
from dissect.volume.exceptions import DMError


class ThinPool:
    """Interact with a device-mapper thin-provision pool."""

    def __init__(self, metadata_fh: BinaryIO, data_fh: BinaryIO):
        self.metadata_fh = metadata_fh
        self.data_fh = data_fh

        self.metadata = Metadata(metadata_fh)

    def open(self, device_id: int, size_hint: int | None = None) -> ThinDevice:
        """Open a thin device on this pool.

        No size information is stored in the pool, so it's recommended to provide a size hint.

        Args:
            device_id: The device ID to open.
            size_hint: Optional size hint for the device to open.
        """
        return ThinDevice(self, device_id, size_hint)


class Metadata:
    """Interact with a device-mapper thin-provision metadata device."""

    def __init__(self, fh: BinaryIO):
        self.fh = fh
        self.sb = c_dm.thin_disk_superblock(fh)
        if self.sb.magic != c_dm.THIN_SUPERBLOCK_MAGIC:
            raise DMError("Invalid magic for thin-pool metadata superblock")

        self.data_block_size = self.sb.data_block_size
        self.metadata_block_size = self.sb.metadata_block_size

        self.data_mapping = BTree(self.fh, self.sb.data_mapping_root, self.metadata_block_size)
        self.device_details = BTree(self.fh, self.sb.device_details_root, self.metadata_block_size)


class ThinDevice(AlignedStream):
    """Implements a readable thin device/volume.

    Args:
        pool: The pool the device belongs to.
        device_id: The device ID of the device.
        size_hint: Optional device size hint.
    """

    def __init__(self, pool: ThinPool, device_id: int, size_hint: int | None = None):
        self.pool = pool
        self.device_id = device_id
        self.block_size = self.pool.metadata.data_block_size * SECTOR_SIZE

        details_buf = self.pool.metadata.device_details.lookup(device_id)
        if not details_buf:
            raise DMError(f"Device ID is not known in pool: {device_id}")
        self.details = c_dm.disk_device_details(details_buf)

        super().__init__(size_hint, self.block_size)

    def _read(self, offset: int, length: int) -> bytes:
        data_fh = self.pool.data_fh
        data_mapping = self.pool.metadata.data_mapping

        block = offset // self.block_size

        result = []
        while length > 0:
            block_info = data_mapping.lookup([self.device_id, block])
            if block_info is None:
                remaining = length
                if self.size is None or (remaining := self.size - offset) > 0:
                    return b"\x00" * min(length, remaining)

                break

            block_time = int.from_bytes(block_info, "little")
            data_block, _ = _unpack_block_time(block_time)

            read_size = min(length, self.block_size)
            data_fh.seek(data_block * self.block_size)
            result.append(data_fh.read(read_size))

            length -= read_size
            offset += read_size
            block += 1

        return b"".join(result)


def _unpack_block_time(block_time: int) -> tuple[int, int]:
    """Unpack a block number and timestamp from a combined block time value.

    Block numbers in device-mapper thin-provisioning store the block number in the upper 40 bits and
    a timestamp in the lower 24 bits.
    """
    block = block_time >> 24
    time = block_time & ((1 << 24) - 1)
    return block, time
