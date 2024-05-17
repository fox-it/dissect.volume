from typing import BinaryIO, Iterator, Optional

from dissect.cstruct import cstruct

from dissect.volume.disk.partition import Partition
from dissect.volume.exceptions import DiskError

mbr_def = """
typedef struct part_s {
    uint8   bootable;       // +0: 0x80/0x00 - bootable/not bootable
    uint8   start_head;     // +1: head (start)
    uint16  start_cyl_sec;  // +2: cyl+sect (start)
    uint8   type;           // +4: type
    uint8   end_head;       // +5: head (end)
    uint16  end_cyl_sec;    // +6: cyl+sec (end)
    uint32  sector_ofs;     // +8: offset in sectors
    uint32  sector_size;    // +12: size in sectors
} part;

typedef struct mbr_s {
    char    bootcode[0x1b8];
    uint32  vol_no;
    uint16  pad1;
    part    part[4];
    uint16  bootsig;
} mbr;
"""

c_mbr = cstruct()
c_mbr.load(mbr_def)


class MBR:
    """Master Boot Record."""

    def __init__(self, fh: BinaryIO, sector_size: int = 512):
        self.fh = fh
        self.sector_size = sector_size
        self.offset = fh.tell()
        self.mbr = c_mbr.mbr_s(fh)

        if self.mbr.bootsig != 0xAA55:
            raise DiskError(f"Invalid MBR boot signature. Expected 0xaa55, got 0x{self.mbr.bootsig:x}.")

        # This sucks but don't have a better way atm
        sig = self.mbr.bootcode[3:11]
        if any(v in sig for v in [b"MSDOS", b"MSWIN", b"NTFS", b"FAT", b"EXFAT", b"-FVE-FS-", b"SYSLINUX"]):
            raise DiskError("Sector is a filesystem VBR, not an MBR")

        if self.mbr.bootcode[18:38] == b"Hit Esc for .altboot" or self.mbr.bootcode[168:174] == b"\r\nQNX ":
            raise DiskError("Sector is a QNX Boot Sector, not an MBR")

        self.partitions: list[Partition] = list(self._partitions(self.mbr, self.offset))

    def _partitions(
        self, mbr: c_mbr.mbr_s, offset: int, num_start: int = 0, ebr_offset: Optional[int] = None
    ) -> Iterator[Partition]:
        for num, partition in enumerate(mbr.part):
            if partition.type == 0x00:
                continue

            part_offset = offset + partition.sector_ofs * self.sector_size

            if partition.type in (0x05, 0x0F, 0x85):  # Extended
                if not ebr_offset:
                    ebr_offset = part_offset
                else:
                    # Chained extended MBRs are relative to the first one
                    part_offset = ebr_offset + partition.sector_ofs * self.sector_size

                self.fh.seek(part_offset)
                e_mbr = c_mbr.mbr_s(self.fh)
                yield from self._partitions(e_mbr, part_offset, num_start + num, ebr_offset=ebr_offset)

                continue

            yield Partition(
                disk=self,
                number=num_start + num + 1,  # partitions are 1-indexed
                offset=part_offset,
                size=partition.sector_size * self.sector_size,
                vtype=partition.type,
                name=None,
                flags=partition.bootable,
                raw=partition,
            )
