from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO

from dissect.volume.disk.schemes import APM, BSD, GPT, MBR
from dissect.volume.exceptions import DiskError

if TYPE_CHECKING:
    from dissect.volume.disk.partition import Partition


class Disk:
    """Generic disk partitioning implementation. The partition scheme is detected automatically.

    Supported partition schemes:
        - MBR (Master Boot Record)
        - GPT (GUID Partition Table)
        - APM (Apple Partition Map)
        - BSD disklabel (contained in another partition or standalone)

    Args:
        fh: File-like object of a disk containing a partition scheme.
        sector_size: Sector size in bytes. GPT will try to detect this automatically, but can be overridden here.
    """

    def __init__(self, fh: BinaryIO, sector_size: int = 512):
        self.fh = fh
        self.sector_size = sector_size
        self.scheme: APM | GPT | MBR | BSD = None
        self.partitions: list[Partition] = []

        errors = []

        # Do a little dance with MBR and GPT first, since we can try to determine the sector size here
        try:
            self.fh.seek(0)
            self.scheme = MBR(self.fh, sector_size=self.sector_size)
        except Exception as e:
            errors.append(str(e))

        if self.scheme and any(p.type == 0xEE for p in self.scheme.partitions):
            # There's a protective MBR, potentially GPT
            # Try to detect sector size until we find a valid GPT header
            for guess in [512, 4096]:
                try:
                    self.fh.seek(0)
                    self.scheme = GPT(self.fh, sector_size=guess)
                    # Winner winner chicken dinner
                    self.sector_size = guess
                    break
                except Exception as e:
                    errors.append(str(e))
            else:
                # No valid GPT found
                raise DiskError("Found GPT type partition, but MBR scheme detected. Maybe exotic sector size?")

        else:
            # It's not MBR or GPT, try the other schemes
            # BSD is usually contained in another scheme's partition, but it can also live standalone.
            # We try to detect BSD as part of another scheme later on, so only try to detect BSD last
            # as standalone.
            for scheme in [APM, BSD]:
                try:
                    self.fh.seek(0)
                    self.scheme = scheme(self.fh, sector_size=self.sector_size)
                    break
                except Exception as e:
                    errors.append(str(e))

        if self.scheme is None:
            raise DiskError("Unable to detect a valid partition scheme:\n- {}".format("\n- ".join(errors)))

        main_scheme = self.scheme
        for partition in main_scheme.partitions:
            # BSD disklabel can also be relative from any other partition
            if partition.type in BSD.TYPES:
                self.scheme = BSD(partition.open(), sector_size=self.sector_size)
                self.partitions.extend(self.scheme.partitions)
            else:
                self.partitions.append(partition)

    @property
    def serial(self) -> int | None:
        if isinstance(self.scheme, MBR):
            return self.scheme.mbr.vol_no
        return None
