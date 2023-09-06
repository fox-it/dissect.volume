from __future__ import annotations

from typing import BinaryIO

from dissect.util.stream import RangeStream

from dissect.volume.raid.stream import (
    Level,
    LinearStream,
    RAID0Stream,
    RAID10Stream,
    RAID456Stream,
)


class RAID:
    def __init__(self, configurations: list[Configuration]):
        self.configurations = configurations


class Configuration:
    def __init__(self, physical_disks: list[PhysicalDisk], virtual_disks: list[VirtualDisk]):
        self.physical_disks = physical_disks
        self.virtual_disks = virtual_disks


class PhysicalDisk:
    def __init__(self, fh: BinaryIO, offset: int, size: int):
        self.fh = fh
        self.offset = offset
        self.size = size

    def open(self) -> BinaryIO:
        """Return a file-like object of the data section of the disk."""
        return RangeStream(self.fh, self.offset, self.size)


class VirtualDisk:
    def __init__(
        self,
        name: str,
        uuid: str,
        size: int,
        level: int,
        layout: int,
        stripe_size: int,
        num_disks: int,
        physical_disks: dict[int, tuple[int, PhysicalDisk]],
    ):
        self.name = name
        self.uuid = uuid
        self.size = size
        self.level = level
        self.layout = layout
        self.stripe_size = stripe_size
        self.num_disks = num_disks
        self.physical_disks = physical_disks

    def open(self) -> BinaryIO:
        """Return a file-like object of the RAID volume in this set."""
        if self.level == Level.LINEAR:
            return LinearStream(self)
        elif self.level == Level.RAID0:
            return RAID0Stream(self)
        elif self.level == Level.RAID1:
            # Don't really care which mirror to read from, so just open the first disk
            return self.physical_disks[0][1].open()
        elif self.level in (Level.RAID4, Level.RAID5, Level.RAID6):
            return RAID456Stream(self)
        elif self.level == Level.RAID10:
            return RAID10Stream(self)
        else:
            raise NotImplementedError(f"Unsupported RAID level: {self.level}")
