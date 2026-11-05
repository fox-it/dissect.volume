from __future__ import annotations

import io
import struct
from typing import BinaryIO

import pytest

from dissect.volume import disk
from dissect.volume.disk.schemes import MBR


def test_mbr(mbr: BinaryIO) -> None:
    d = disk.Disk(mbr)

    assert isinstance(d.scheme, MBR)
    assert len(d.partitions) == 2
    assert d.scheme.mbr.bootsig == 0xAA55

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 0x100000
    assert d.partitions[0].size == 0x1F400000
    assert d.partitions[0].type == 0x7
    assert repr(d.partitions[0]) == "<Partition number=1 offset=0x100000 size=0x1f400000 type='0x7 (NTFS)' name=None>"

    assert d.partitions[1].number == 2
    assert d.partitions[1].offset == 0x1F500000
    assert d.partitions[1].size == 0xEE0A00000
    assert d.partitions[1].type == 0x7


def test_mbr_invalid() -> None:
    buf = io.BytesIO(b"\x00" * 512)
    with pytest.raises(disk.DiskError):
        MBR(buf)


def _boot_sector(head: bytes, partitions: list[tuple[int, int, int, int]]) -> bytes:
    """A boot sector starting with ``head``, with ``partitions`` (bootable, type, offset, size) at 0x1BE."""
    sector = bytearray(512)
    sector[: len(head)] = head
    for i, (bootable, part_type, offset, size) in enumerate(partitions):
        struct.pack_into("<B3xB3xII", sector, 0x1BE + i * 16, bootable, part_type, offset, size)
    sector[0x1FE:0x200] = b"\x55\xaa"
    return bytes(sector)


def _fat32_boot_sector(oem_id: bytes) -> bytes:
    """A FAT32 boot sector of a 200 MiB volume, with boot code where an MBR has its partition table."""
    bpb = bytearray(b"\xeb\x58\x90" + oem_id + bytes(25))
    # Bytes per sector, sectors per cluster, reserved sectors, number of FATs
    struct.pack_into("<HBHB", bpb, 11, 512, 8, 32, 2)
    bpb[21] = 0xF8  # Media descriptor
    struct.pack_into("<I", bpb, 32, 409600)  # Total sectors
    # The boot code reads as four partition entries, all starting past the end of the volume
    boot_code = [(0x20, 0x0B, 0x6F732D6E, 0x73696420), (0x6E, 0x07, 0x72726F72, 0x0A0D2079)] * 2
    return _boot_sector(bytes(bpb), boot_code)


def test_mbr_fat_boot_sector(fat: BinaryIO) -> None:
    # The first sector of a FAT12, FAT16 and FAT32 volume made by mkfs.fat (dosfstools 4.2, with
    # --invariant), whose OEM ID is "mkfs.fat"
    with pytest.raises(disk.DiskError, match="VBR, not an MBR"):
        MBR(fat)

    fat.seek(0)
    with pytest.raises(disk.DiskError, match="Unable to detect a valid partition scheme"):
        disk.Disk(fat)


@pytest.mark.parametrize("oem_id", [b"FANUC   ", b"        ", b"MSDOS5.0"])
def test_mbr_fat_boot_sector_oem_id(oem_id: bytes) -> None:
    # Boot code that reads as partition entries, as some vendor tools write it, whatever the OEM ID
    buf = io.BytesIO(_fat32_boot_sector(oem_id) + bytes(512))
    with pytest.raises(disk.DiskError, match="VBR, not an MBR"):
        MBR(buf)

    with pytest.raises(disk.DiskError, match="Unable to detect a valid partition scheme"):
        disk.Disk(buf)


def test_mbr_boot_code_with_jump() -> None:
    # Boot code that starts with a jump, as GRUB's does, but without a BIOS parameter block
    buf = io.BytesIO(_boot_sector(b"\xeb\x63\x90", [(0x80, 0x83, 2048, 409600)]))
    m = MBR(buf)

    assert len(m.partitions) == 1
    assert m.partitions[0].offset == 2048 * 512
    assert m.partitions[0].type == 0x83
