from dissect.volume.exceptions import DiskError
from dissect.volume.disk.schemes import APM, GPT, MBR


class Disk:
    def __init__(self, fh, sector_size=512):
        self.fh = fh
        self.sector_size = sector_size
        self.scheme = None

        start = fh.tell()
        errors = []

        # The GPT scheme also parses the protective MBR, so it must be tried before MBR
        for scheme in [GPT, MBR, APM]:
            try:
                fh.seek(start)
                self.scheme = scheme(fh, sector_size=self.sector_size)

                break
            except DiskError as e:
                errors.append(str(e))

        if not self.scheme:
            raise DiskError("Unable to detect a valid partition scheme:\n- {}".format("\n- ".join(errors)))

        self.partitions = self.scheme.partitions
        if isinstance(self.scheme, MBR) and any([p.type == 0xEE for p in self.partitions]):
            raise DiskError("Found GPT type partition, but MBR scheme detected. Maybe 4K sector size.")

    @property
    def serial(self):
        if isinstance(self.scheme, MBR):
            return self.scheme.mbr.vol_no
        return None
