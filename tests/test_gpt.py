import io
from typing import BinaryIO

import pytest

from dissect.volume import disk
from dissect.volume.disk.schemes import GPT


def test_gpt(gpt: BinaryIO) -> None:
    d = disk.Disk(gpt)

    assert isinstance(d.scheme, GPT)
    assert len(d.partitions) == 3
    assert len(d.scheme.partitions) == 3
    assert d.scheme.mbr.mbr.bootsig == 0xAA55

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 20480
    assert d.partitions[0].size == 0xC7FFE00
    assert d.partitions[0].type == b"\x28\x73\x2a\xc1\x1f\xf8\xd2\x11\xba\x4b\x00\xa0\xc9\x3e\xc9\x3b"
    assert d.partitions[0].name == "EFI System Partition"

    assert d.partitions[1].number == 2
    assert d.partitions[1].offset == 0xC900000
    assert d.partitions[1].size == 0x7465FFE00
    assert d.partitions[1].type == b"\xa2\xa0\xd0\xeb\xe5\xb9\x33\x44\x87\xc0\x68\xb6\xb7\x26\x99\xc7"
    assert d.partitions[1].name == ""


def test_gpt_invalid(gpt: BinaryIO) -> None:
    buf = io.BytesIO(gpt.read(512) + 896 * b"\x00")
    with pytest.raises(disk.DiskError):
        GPT(buf)


def test_hybrid_gpt(gpt_hybrid: BinaryIO) -> None:
    d = disk.Disk(gpt_hybrid)

    assert isinstance(d.scheme, GPT)
    assert len(d.partitions) == 5
    assert len(d.scheme.partitions) == 5
    assert d.scheme.mbr.mbr.bootsig == 0xAA55

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 0x100000
    assert d.partitions[0].size == 0x80200
    assert d.partitions[0].type == 0x83
    assert d.partitions[0].name is None

    assert d.partitions[1].number == 2
    assert d.partitions[1].offset == 0x180200
    assert d.partitions[1].size == 0x7BC00
    assert d.partitions[1].type == 0x83
    assert d.partitions[1].name is None

    assert d.partitions[2].number == 1
    assert d.partitions[2].offset == 0x100000
    assert d.partitions[2].size == 0x80000
    assert d.partitions[2].type == b"\xaf\x3d\xc6\x0f\x83\x84\x72\x47\x8e\x79\x3d\x69\xd8\x47\x7d\xe4"
    assert d.partitions[2].name == "Linux filesystem"

    assert d.partitions[3].number == 2
    assert d.partitions[3].offset == 0x4400
    assert d.partitions[3].size == 0xFBA00
    assert d.partitions[3].type == b"\xaf\x3d\xc6\x0f\x83\x84\x72\x47\x8e\x79\x3d\x69\xd8\x47\x7d\xe4"
    assert d.partitions[3].name == "Linux filesystem"

    assert d.partitions[4].number == 3
    assert d.partitions[4].offset == 0x180200
    assert d.partitions[4].size == 0x7BA00
    assert d.partitions[4].type == b"\xaf\x3d\xc6\x0f\x83\x84\x72\x47\x8e\x79\x3d\x69\xd8\x47\x7d\xe4"
    assert d.partitions[4].name == "Linux filesystem"


def test_gpt_4k(gpt_4k: BinaryIO) -> None:
    with pytest.raises(disk.DiskError) as e:
        disk.Disk(gpt_4k)

    assert str(e.value) == "Found GPT type partition, but MBR scheme detected. Maybe 4K sector size."

    gpt_4k.seek(0)
    d = disk.Disk(gpt_4k, sector_size=4096)

    assert isinstance(d.scheme, GPT)
    assert len(d.partitions) == 3

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 0x100000
    assert d.partitions[0].size == 0x100000
    assert d.partitions[0].type == b"\xaf\x3d\xc6\x0f\x83\x84\x72\x47\x8e\x79\x3d\x69\xd8\x47\x7d\xe4"
    assert d.partitions[0].name == "Linux filesystem"

    assert d.partitions[1].number == 2
    assert d.partitions[1].offset == 0x300000
    assert d.partitions[1].size == 0x100000
    assert d.partitions[1].type == b"\xaf\x3d\xc6\x0f\x83\x84\x72\x47\x8e\x79\x3d\x69\xd8\x47\x7d\xe4"
    assert d.partitions[1].name == "Linux filesystem"

    assert d.partitions[2].number == 3
    assert d.partitions[2].offset == 0x500000
    assert d.partitions[2].size == 0xB5A000
    assert d.partitions[2].type == b"\xaf\x3d\xc6\x0f\x83\x84\x72\x47\x8e\x79\x3d\x69\xd8\x47\x7d\xe4"
    assert d.partitions[2].name == "Linux filesystem"


def test_gpt_esxi(gpt_esxi: BinaryIO) -> None:
    d = disk.Disk(gpt_esxi)

    assert isinstance(d.scheme, GPT)
    assert len(d.partitions) == 5

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 0x8000
    assert d.partitions[0].size == 0x63FFE00
    assert d.partitions[0].type == b"\x28\x73\x2a\xc1\x1f\xf8\xd2\x11\xba\x4b\x00\xa0\xc9\x3e\xc9\x3b"
    assert d.partitions[0].name == "BOOT"

    assert d.partitions[1].number == 5
    assert d.partitions[1].offset == 0x6600000
    assert d.partitions[1].size == 0xFFEFFE00
    assert d.partitions[1].type == b"\xa2\xa0\xd0\xeb\xe5\xb9\x33\x44\x87\xc0\x68\xb6\xb7\x26\x99\xc7"
    assert d.partitions[1].name == "BOOTBANK1"

    assert d.partitions[2].number == 6
    assert d.partitions[2].offset == 0x106600000
    assert d.partitions[2].size == 0xFFEFFE00
    assert d.partitions[2].type == b"\xa2\xa0\xd0\xeb\xe5\xb9\x33\x44\x87\xc0\x68\xb6\xb7\x26\x99\xc7"
    assert d.partitions[2].name == "BOOTBANK2"

    assert d.partitions[3].number == 7
    assert d.partitions[3].offset == 0x206600000
    assert d.partitions[3].size == 0x1DF99FFE00
    assert d.partitions[3].type == b"\x39\xea\xb2\x4e\x55\x78\x90\x47\xa7\x9e\xfa\xe4\x95\xe2\x1f\x8d"
    assert d.partitions[3].name == "OSDATA"

    assert d.partitions[4].number == 8
    assert d.partitions[4].offset == 0x2000100000
    assert d.partitions[4].size == 0x37FEFBC00
    assert d.partitions[4].type == b"\x2a\xe0\x31\xaa\x0f\x40\xdb\x11\x95\x90\x00\x0c\x29\x11\xd1\xb8"
    assert d.partitions[4].name == "datastore1"


def test_gpt_esxi_no_name_xff(gpt_no_name_xff: BinaryIO) -> None:
    d = disk.Disk(gpt_no_name_xff)

    assert len(d.partitions) == 1
    assert d.partitions[0].name == ""
