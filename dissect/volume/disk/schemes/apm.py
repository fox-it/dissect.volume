from typing import BinaryIO, Iterator

from dissect import cstruct
from dissect.volume.disk.partition import Partition
from dissect.volume.exceptions import DiskError

apm_def = """
struct partition_entry {
    char    signature[2];
    uint16  reserved_1;
    uint32  partition_count;
    uint32  partition_start;
    uint32  partition_size;
    char    partition_name[32];
    char    partition_type[32];
    uint32  data_start;
    uint32  data_size;
    uint32  partition_status;
    uint32  boot_code_start;
    uint32  boot_code_size;
    uint32  boot_loader_address;
    uint32  reserved_2;
    uint32  boot_code_entry;
    uint32  reserved_3;
    uint32  boot_code_checksum;
    char    processor_type[16];
    char    reserved_4[376];
};
"""

c_apm = cstruct.cstruct()
c_apm.load(apm_def)
c_apm.endian = ">"


class APM:
    """Apple Partition Map."""

    def __init__(self, fh: BinaryIO, sector_size: int = 512):
        self.fh = fh
        self.sector_size = sector_size
        self.apm = c_apm.partition_entry(fh)

        if self.apm.signature == b"ER":
            # We parsed the boot jump, the offset is conveniently in reserved_1 field
            fh.seek(self.apm.reserved_1)
            self.apm = c_apm.partition_entry(fh)

        if self.apm.signature != b"PM":
            raise DiskError(f"Invalid APM signature, expected 'PM', got {self.apm.signature!r}.")

        self._partitions_offset = fh.tell()
        self.partitions: list[Partition] = list(self._partitions())

    def _partitions(self) -> Iterator[Partition]:
        self.fh.seek(self._partitions_offset)
        for i in range(self.apm.partition_count):
            if i == 0:
                p = self.apm
            else:
                p = c_apm.partition_entry(self.fh)

            yield Partition(
                disk=self,
                number=i + 1,  # partitions are 1-indexed
                offset=p.partition_start * self.sector_size,
                size=p.partition_size * self.sector_size,
                vtype=p.partition_type.rstrip(b"\x00").decode(),
                name=p.partition_name.rstrip(b"\x00").decode(),
                raw=p,
            )
