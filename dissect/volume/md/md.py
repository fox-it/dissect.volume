from __future__ import annotations

import datetime
import io
import operator
import struct
from typing import BinaryIO, Union
from uuid import UUID

from dissect.util import ts

from dissect.volume.md.c_md import SECTOR_SIZE, c_md
from dissect.volume.raid.raid import RAID, Configuration, PhysicalDisk, VirtualDisk
from dissect.volume.raid.stream import Level


class MD(RAID):
    """Read an MD RAID set of one or multiple devices/file-like objects.

    Use this class to read from a RAID set.

    Args:
        fh: A single file-like object or :class:`Device`, or a list of multiple belonging to the same RAID set.
    """

    def __init__(self, fh: Union[list[Union[BinaryIO, Device]], Union[BinaryIO, Device]]):
        fhs = [fh] if not isinstance(fh, list) else fh
        self.devices = [Device(fh) if not isinstance(fh, Device) else fh for fh in fhs]

        config_map = {}
        for dev in self.devices:
            config_map.setdefault(dev.set_uuid, []).append(dev)

        super().__init__([MDConfiguration(devices) for devices in config_map.values()])


class MDConfiguration(Configuration):
    def __init__(self, devices: list[Union[BinaryIO, Device]]):
        devices = [Device(fh) if not isinstance(fh, Device) else fh for fh in devices]

        self.devices = sorted(devices, key=operator.attrgetter("raid_disk"))
        if len({dev.set_uuid for dev in self.devices}) != 1:
            raise ValueError("Multiple MD sets detected, supply only the devices of a single set")

        virtual_disk = MDDisk(self)
        super().__init__(self.devices, [virtual_disk])


class MDDisk(VirtualDisk):
    def __init__(self, configuration: MDConfiguration):
        self.configuration = configuration
        reference_dev = sorted(configuration.devices, key=operator.attrgetter("events"), reverse=True)[0]
        disks = {dev.raid_disk: (0, dev) for dev in self.configuration.devices if dev.raid_disk is not None}

        if reference_dev.level == Level.LINEAR:
            size = sum(disk.size for _, disk in disks.values())
        elif reference_dev.level == Level.RAID0:
            size = 0
            for _, disk in disks.values():
                size += disk.size & ~(reference_dev.chunk_size - 1)
        elif reference_dev.level in (Level.RAID1, Level.RAID4, Level.RAID5, Level.RAID6, Level.RAID10):
            size = reference_dev.sb.size * SECTOR_SIZE

        super().__init__(
            reference_dev.set_name,
            reference_dev.set_uuid,
            size,
            reference_dev.level,
            reference_dev.layout,
            reference_dev.chunk_size,
            reference_dev.raid_disks,
            disks,
        )


class Device(PhysicalDisk):
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

    if hasattr(fh, "size"):
        size = fh.size
    else:
        size = fh.seek(0, io.SEEK_END)
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
