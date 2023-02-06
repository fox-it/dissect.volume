import io

import pytest

from dissect.volume import disk
from dissect.volume.disk.schemes import MBR


def test_mbr(mbr):
    d = disk.Disk(mbr)

    assert isinstance(d.scheme, MBR)
    assert len(d.partitions) == 2
    assert d.scheme.mbr.bootsig == 0xAA55

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 0x100000
    assert d.partitions[0].size == 0x1F400000
    assert d.partitions[0].type == 0x7

    assert d.partitions[1].number == 2
    assert d.partitions[1].offset == 0x1F500000
    assert d.partitions[1].size == 0xEE0A00000
    assert d.partitions[1].type == 0x7


def test_mbr_invalid():
    buf = io.BytesIO(b"\x00" * 512)
    with pytest.raises(disk.DiskError):
        MBR(buf)
