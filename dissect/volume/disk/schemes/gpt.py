from typing import BinaryIO, Iterator

from dissect.cstruct import cstruct

from dissect.volume.disk.partition import Partition
from dissect.volume.disk.schemes.mbr import MBR
from dissect.volume.exceptions import DiskError

gpt_def = """
// http://en.wikipedia.org/wiki/GUID_Partition_Table
struct GPT_HEADER {
    char        signature[8];
    uint32      revision;
    uint32      header_size;
    uint32      crc32;
    uint32      reserved;
    uint64      current_lba;
    uint64      backup_lba;
    uint64      first_usable_lba;
    uint64      last_usable_lba;
    char        guid[16];
    uint64      lba_partition_array;
    uint32      partition_table_count;
    uint32      partition_entry_size;
    uint32      partition_table_crc;
    char        _[420];
};

struct GPT_PARTITION {
    char        type_guid[16];
    char        partition_guid[16];
    uint64      first_lba;
    uint64      last_lba;
    uint64      attribute_flags;
    char        name[72];   // UTF16 encoded
};

// 0 (0x00)     16 bytes     Partition type GUID
// 16 (0x10)    16 bytes     Unique partition GUID
// 32 (0x20)    8 bytes      First LBA (little endian)
// 40 (0x28)    8 bytes      Last LBA (inclusive, usually odd)
// 48 (0x30)    8 bytes      Attribute flags (e.g. bit 60 denotes read-only)
// 56 (0x38)    72 bytes     Partition name (36 UTF-16LE code units)
"""

c_gpt = cstruct().load(gpt_def)


class GPT:
    """GUID Partition Table."""

    def __init__(self, fh: BinaryIO, sector_size: int = 512):
        self.fh = fh
        self.sector_size = sector_size

        self.mbr = MBR(fh, sector_size=sector_size)
        gpt_parts = [p for p in self.mbr.partitions if p.type == 0xEE]
        if not gpt_parts:
            raise DiskError("Invalid protective/hybrid MBR, could not find 0xEE GPT partition")

        gpt_part = gpt_parts[0]
        fh.seek(gpt_part.offset)

        self.gpt = c_gpt.GPT_HEADER(fh)
        if self.gpt.signature != b"EFI PART":
            raise DiskError(f"Invalid GPT signature, expected 'EFI PART', got {self.gpt.signature!r}.")

        self.partitions: list[Partition] = list(self._partitions())

    def _partitions(self) -> Iterator[Partition]:
        # First iterate MBR partitions
        # When we find the GPT partition, iterate GPT partitions
        # This should support Hybrid GPT
        seen_gpt = False

        for mbr_part in self.mbr.partitions:
            # There can be multiple protective MBR entries
            if mbr_part.type == 0xEE:
                if seen_gpt:
                    continue

                base_offset = self.gpt.lba_partition_array * self.sector_size
                self.fh.seek(base_offset)

                # Numbering a hybrid GPT is a bit weird, I think it's best to do nothing
                # This way the MBR and the GPT have their own separate partition numbers
                for part_num in range(self.gpt.partition_table_count):
                    self.fh.seek(base_offset + (part_num * self.gpt.partition_entry_size))

                    partition = c_gpt.GPT_PARTITION(self.fh)
                    if partition.first_lba == 0:
                        continue

                    name = (
                        partition.name.decode("utf-16-le", "ignore")
                        .split("\x00")[0]
                        .strip("\x00")
                        .strip("\uffff")  # a non-character in UTF-16
                    )

                    yield Partition(
                        disk=self,
                        number=part_num + 1,  # partitions are 1-indexed
                        offset=partition.first_lba * self.sector_size,
                        size=(partition.last_lba - partition.first_lba) * self.sector_size,
                        vtype=partition.type_guid,
                        name=name,
                        flags=partition.attribute_flags,
                        guid=partition.partition_guid,
                        raw=partition,
                    )

                seen_gpt = True
            else:
                yield mbr_part
