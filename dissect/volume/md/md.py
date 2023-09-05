from __future__ import annotations

import datetime
import io
import operator
import struct
from typing import BinaryIO, Union
from uuid import UUID

from dissect.util import ts
from dissect.util.stream import RangeStream

from dissect.volume.md.c_md import c_md
from dissect.volume.md.stream import (
    RAID0Stream,
    RAID10Stream,
    RAID456Stream,
    create_linear_stream,
)


class MD:
    def __init__(self, fh: Union[list[Union[BinaryIO, Device]], Union[BinaryIO, Device]]):
        self.fh = [fh] if not isinstance(fh, list) else fh
        if not fh:
            raise ValueError("At least one file-like object is required")

        devices = [Device(fh) if not isinstance(fh, Device) else fh for fh in self.fh]
        self.devices = sorted(devices, key=operator.attrgetter("raid_disk"))
        if len({dev.set_uuid for dev in self.devices}) != 1:
            raise ValueError("Multiple MD sets detected, supply only the devices of a single set")

        self.sb = sorted(self.devices, key=operator.attrgetter("events"), reverse=True)[0].sb
        reference_dev = self.devices[0]
        self.uuid = reference_dev.set_uuid
        self.name = reference_dev.set_name
        self.level = reference_dev.level
        self.layout = reference_dev.layout
        self.chunk_size = reference_dev.chunk_size
        self.chunk_sectors = reference_dev.chunk_sectors
        self.raid_disks = self.sb.raid_disks

        self.size = self.open().size

    def open(self) -> BinaryIO:
        if self.level == c_md.LEVEL_LINEAR:
            return create_linear_stream(self)
        elif self.level == 0:
            return RAID0Stream(self)
        elif self.level == 1:
            # Don't really care which mirror to read from, so just open the first device
            return self.devices[0].open()
        elif self.level in (4, 5, 6):
            return RAID456Stream(self)
        elif self.level == 10:
            return RAID10Stream(self)


class Device:
    def __init__(self, fh: BinaryIO):
        self.fh = fh

        sb_offset, sb_major, sb_minor = find_super_block(fh)
        if sb_offset is None:
            raise ValueError("File-like object is not an MD device")

        fh.seek(sb_offset * 512)
        if sb_major == 1:
            self.sb = c_md.mdp_superblock_1(fh)
        elif sb_major == 0:
            self.sb = c_md.mdp_super_t(fh)
        else:
            raise ValueError(f"Invalid MD version at {sb_offset:#x}: {sb_major}.{sb_minor}")

        if self.sb.major_version == 1:
            self.set_uuid = UUID(bytes_le=self.sb.set_uuid)
            self.set_name = self.sb.set_name.decode(errors="surrigateescape")
            self.events = self.sb.events
            self.chunk_sectors = self.sb.chunksize
            self.chunk_size = self.chunk_sectors << 9
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
            self.chunk_sectors = self.chunk_size >> 9
            self.data_offset = 0
            self.data_size = sb_offset
            self.dev_number = self.sb.this_disk.number
            self.device_uuid = None
            self.raid_disk = self.sb.disks[self.dev_number].raid_disk

        self.creation_time = _parse_ts(self.sb.ctime)
        self.update_time = _parse_ts(self.sb.ctime)
        self.level = self.sb.level
        self.layout = self.sb.layout
        self.sectors = self.data_size

    def open(self) -> BinaryIO:
        return RangeStream(
            self.fh,
            self.data_offset * 512,
            self.data_size * 512,
            align=self.chunk_size or io.DEFAULT_BUFFER_SIZE,
        )


def find_super_block(fh: BinaryIO) -> tuple[int, int, int]:
    # Super block can start at a couple of places, depending on version
    # Just try them all until we find one

    if hasattr(fh, "size"):
        size = fh.size
    else:
        size = fh.seek(0, io.SEEK_END)
    size //= 512

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
        fh.seek(offset * 512)

        peek = fh.read(12)
        if len(peek) != 12:
            continue

        magic, major, minor = struct.unpack("<3I", peek)
        if magic == c_md.MD_SB_MAGIC:
            return offset, major, minor

    return None, None, None


def _parse_ts(timestamp: int) -> datetime.datetime:
    seconds = timestamp & 0xFFFFFFFFFF
    micro = timestamp >> 40
    return ts.from_unix_us((seconds * 1000000) + micro)
