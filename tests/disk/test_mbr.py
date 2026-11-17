from __future__ import annotations

import io
from typing import BinaryIO

import pytest

from dissect.volume import disk
from dissect.volume.disk.schemes import MBR
from dissect.volume.disk.schemes.mbr import c_mbr


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


def test_mbr_fat_boot_sector(fat: BinaryIO) -> None:
    # The first sector of a FAT12, FAT16 and FAT32 volume made by mkfs.fat (dosfstools 4.2, with
    # --invariant), whose OEM ID is "mkfs.fat"
    with pytest.raises(disk.DiskError, match="VBR, not an MBR"):
        MBR(fat)

    with pytest.raises(disk.DiskError, match="Unable to detect a valid partition scheme"):
        disk.Disk(fat)


@pytest.mark.parametrize("oem_id", [b"FANUC   ", b"        ", b"MSDOS5.0"])
def test_mbr_fat_boot_sector_oem_id(fat: BinaryIO, oem_id: bytes) -> None:
    # The same boot sectors with another OEM ID, and with boot code whose message runs over where an MBR has its
    # partition table, as in the boot sectors of some vendor tools
    sector = bytearray(fat.read())
    sector[3:11] = oem_id
    message = b"Non-system disk or disk error. Replace the disk, and then press any key to try again.\r\n"
    sector[0x1FE - len(message) : 0x1FE] = message
    buf = io.BytesIO(bytes(sector))
    with pytest.raises(disk.DiskError, match="VBR, not an MBR"):
        MBR(buf)

    with pytest.raises(disk.DiskError, match="Unable to detect a valid partition scheme"):
        disk.Disk(buf)


def test_mbr_boot_code_with_jump() -> None:
    # Boot code that starts with a jump, as GRUB's does, but without a BIOS parameter block
    partition = c_mbr.part(bootable=0x80, type=0x83, sector_ofs=2048, sector_size=409600)
    mbr = c_mbr.mbr(bootcode=b"\xeb\x63\x90", part=[partition] + [c_mbr.part()] * 3, bootsig=0xAA55)
    m = MBR(io.BytesIO(mbr.dumps()))

    assert len(m.partitions) == 1
    assert m.partitions[0].offset == 2048 * 512
    assert m.partitions[0].type == 0x83
