from __future__ import annotations

import io
import operator
import struct
from typing import TYPE_CHECKING, BinaryIO
from uuid import UUID

from dissect.util import ts

from dissect.volume.md.c_md import SECTOR_SIZE, c_md
from dissect.volume.raid.raid import RAID, Configuration, PhysicalDisk, VirtualDisk
from dissect.volume.raid.stream import Level

if TYPE_CHECKING:
    import datetime

    MDPhysicalDiskDescriptor = BinaryIO | "MDPhysicalDisk"


class MD(RAID):
    """Read an MD RAID set of one or multiple devices/file-like objects.

    Use this class to read from a RAID set.

    Args:
        fh: A single file-like object or :class:`MDPhysicalDisk`, or a list of multiple belonging to the same RAID set.
    """

    def __init__(self, fh: list[MDPhysicalDiskDescriptor] | MDPhysicalDiskDescriptor):
        fhs = [fh] if not isinstance(fh, list) else fh
        physical_disks = [MDPhysicalDisk(fh) if not isinstance(fh, MDPhysicalDisk) else fh for fh in fhs]

        config_map = {}
        for disk in physical_disks:
            config_map.setdefault(disk.set_uuid, []).append(disk)

        super().__init__([MDConfiguration(disks) for disks in config_map.values()])


class MDConfiguration(Configuration):
    def __init__(self, physical_disks: list[MDPhysicalDisk]):
        physical_disks = sorted(physical_disks, key=operator.attrgetter("raid_disk"))

        if len({disk.set_uuid for disk in physical_disks}) != 1:
            raise ValueError("Multiple MD sets detected, supply only the disks of a single set")

        virtual_disks = [MDVirtualDisk(physical_disks)]
        super().__init__(physical_disks, virtual_disks)


class MDVirtualDisk(VirtualDisk):
    def __init__(self, physical_disks: list[MDPhysicalDisk]):
        reference_disk = sorted(physical_disks, key=operator.attrgetter("events"), reverse=True)[0]
        disk_map = {disk.raid_disk: (0, disk) for disk in physical_disks if disk.raid_disk is not None}

        if reference_disk.level == Level.LINEAR:
            size = sum(disk.size for _, disk in disk_map.values())
        elif reference_disk.level == Level.RAID0:
            size = 0
            for _, disk in disk_map.values():
                size += disk.size & ~(reference_disk.chunk_size - 1)
        elif reference_disk.level in (Level.RAID1, Level.RAID4, Level.RAID5, Level.RAID6, Level.RAID10):
            size = reference_disk.sb.size * SECTOR_SIZE
        else:
            raise ValueError(
                "Invalid MD RAID configuration: No valid RAID level found for the reference disk, found: %d",
                reference_disk.level,
            )

        super().__init__(
            reference_disk.set_name,
            reference_disk.set_uuid,
            size,
            reference_disk.level,
            reference_disk.layout,
            reference_disk.chunk_size,
            reference_disk.raid_disks,
            disk_map,
        )


class MDPhysicalDisk(PhysicalDisk):
    """Parse metadata from an MD device.

    Supports 0.90 and 1.x metadata.

    Args:
        fh: The file-like object to read metadata from.
    """

    def __init__(self, fh: BinaryIO):
        sb_offset, sb_major, sb_minor = find_super_block(fh)
        if sb_offset is None:
            raise ValueError("File-like object is not an MD device")

        fh.seek(sb_offset * SECTOR_SIZE)
        if sb_major == 1:
            self.sb = c_md.mdp_superblock_1(fh)
        elif sb_major == 0:
            self.sb = c_md.mdp_super_t(fh)
        else:
            raise ValueError(f"Invalid MD version at {sb_offset:#x}: {sb_major}.{sb_minor}")

        if self.sb.major_version == 1:
            self.set_uuid = UUID(bytes_le=self.sb.set_uuid)
            self.set_name = self.sb.set_name.split(b"\x00", 1)[0].decode(errors="surrogateescape")
            self.events = self.sb.events
            self.chunk_sectors = self.sb.chunksize
            self.chunk_size = self.chunk_sectors * SECTOR_SIZE
            self.data_offset = self.sb.data_offset
            self.data_size = self.sb.data_size
            self.dev_number = self.sb.dev_number
            self.device_uuid = UUID(bytes_le=self.sb.device_uuid)

            role = self.sb.dev_roles[self.sb.dev_number]
            if role == c_md.MD_DISK_ROLE_JOURNAL:
                self.raid_disk = 0
            elif role <= c_md.MD_DISK_ROLE_MAX:
                self.raid_disk = role
            else:
                self.raid_disk = None

        else:
            self.set_uuid = UUID(bytes_le=self.sb.set_uuid0 + self.sb.set_uuid1 + self.sb.set_uuid2 + self.sb.set_uuid3)
            self.set_name = None
            self.events = (self.sb.events_hi << 32) | self.sb.events_lo
            self.chunk_size = self.sb.chunk_size
            self.chunk_sectors = self.chunk_size // SECTOR_SIZE
            self.data_offset = 0
            self.data_size = sb_offset
            self.dev_number = self.sb.this_disk.number
            self.device_uuid = None
            self.raid_disk = self.sb.disks[self.dev_number].raid_disk

        self.creation_time = _parse_ts(self.sb.ctime)
        self.update_time = _parse_ts(self.sb.ctime)
        self.level = self.sb.level
        self.layout = self.sb.layout
        self.raid_disks = self.sb.raid_disks
        self.sectors = self.data_size

        super().__init__(fh, self.data_offset * SECTOR_SIZE, self.data_size * SECTOR_SIZE)


def find_super_block(fh: BinaryIO) -> tuple[int, int, int]:
    # Super block can start at a couple of places, depending on version
    # Just try them all until we find one

    size = fh.size if hasattr(fh, "size") else fh.seek(0, io.SEEK_END)
    size //= SECTOR_SIZE

    possible_offsets = [
        # 0.90.0
        (size & ~(c_md.MD_RESERVED_SECTORS - 1)) - c_md.MD_RESERVED_SECTORS,
        # Major version 1
        # 0: At least 8K, but less than 12K, from end of device
        size - 8 * 2,
        # 1: At start of device
        0,
        # 2: 4K from start of device.
        8,
    ]

    for offset in possible_offsets:
        fh.seek(offset * SECTOR_SIZE)

        peek = fh.read(12)
        if len(peek) != 12:
            continue

        magic, major, minor = struct.unpack("<3I", peek)
        if magic == c_md.MD_SB_MAGIC:
            return offset, major, minor

    return None, None, None


def _parse_ts(timestamp: int) -> datetime.datetime:
    """Utility method for parsing MD timestamps.

    Lower 40 bits are seconds, upper 24 are microseconds.
    """
    seconds = timestamp & 0xFFFFFFFFFF
    micro = timestamp >> 40
    return ts.from_unix_us((seconds * 1000000) + micro)
