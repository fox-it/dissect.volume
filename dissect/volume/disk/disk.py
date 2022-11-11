from typing import BinaryIO, Optional

from dissect.volume.disk.partition import Partition
from dissect.volume.disk.schemes import APM, BSD, GPT, MBR
from dissect.volume.exceptions import DiskError


class Disk:
    def __init__(self, fh: BinaryIO, sector_size: int = 512):
        self.fh = fh
        self.sector_size = sector_size
        self.scheme = None
        self.partitions: list[Partition] = []

        start = fh.tell()
        errors = []

        # The GPT scheme also parses the protective MBR, so it must be tried before MBR.
        # BSD is usually contained in another scheme's partition, but it can also live standalone.
        # We try to detect BSD as part of another scheme later on, so only try to detect BSD last
        # as standalone.
        for scheme in [GPT, MBR, APM, BSD]:
            try:
                fh.seek(start)
                self.scheme = scheme(fh, sector_size=self.sector_size)

                break
            except Exception as e:
                errors.append(str(e))

        if not self.scheme:
            raise DiskError("Unable to detect a valid partition scheme:\n- {}".format("\n- ".join(errors)))

        main_scheme = self.scheme
        for partition in main_scheme.partitions:
            # BSD disklabel can also be relative from any other partition
            if partition.type in BSD.TYPES:
                self.scheme = BSD(partition.open(), sector_size=self.sector_size)
                self.partitions.extend(self.scheme.partitions)
            else:
                self.partitions.append(partition)

        if isinstance(self.scheme, MBR) and any([p.type == 0xEE for p in self.partitions]):
            raise DiskError("Found GPT type partition, but MBR scheme detected. Maybe 4K sector size.")

    @property
    def serial(self) -> Optional[int]:
        if isinstance(self.scheme, MBR):
            return self.scheme.mbr.vol_no
        return None
