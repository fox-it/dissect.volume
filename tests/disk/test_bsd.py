from __future__ import annotations

from typing import BinaryIO
from uuid import UUID

from dissect.volume import disk
from dissect.volume.disk.schemes import BSD


def test_bsd(bsd: BinaryIO) -> None:
    vs = BSD(bsd)

    assert len(vs.partitions) == 3

    assert vs.partitions[0].number == 1
    assert vs.partitions[0].offset == 0x800
    assert vs.partitions[0].size == 0x200
    assert vs.partitions[0].type == 7
    assert vs.partitions[0].guid is None
    assert vs.partitions[0].open().read() == b"\x01" * 512

    assert vs.partitions[1].number == 2
    assert vs.partitions[1].offset == 0xC00
    assert vs.partitions[1].size == 0x200
    assert vs.partitions[1].type == 1
    assert vs.partitions[1].guid is None
    assert vs.partitions[1].open().read() == b"\x02" * 512

    assert vs.partitions[2].number == 5
    assert vs.partitions[2].offset == 0x1000
    assert vs.partitions[2].size == 0x200
    assert vs.partitions[2].type == 27
    assert vs.partitions[2].guid is None
    assert vs.partitions[2].open().read() == b"\x05" * 512


def test_bsd64(bsd64: BinaryIO) -> None:
    d = disk.Disk(bsd64)

    assert isinstance(d.scheme, BSD)
    assert len(d.partitions) == 4

    # Index 0 is another GPT partition

    assert d.partitions[1].number == 1
    assert d.partitions[1].offset == 0x100000
    assert d.partitions[1].size == 0x1000
    assert d.partitions[1].type == 7
    assert d.partitions[1].guid == UUID("c3ce50a4-61bd-11ed-9048-010c29feac9a")
    assert d.partitions[1].open().read(512) == b"\x01" * 512

    assert d.partitions[2].number == 2
    assert d.partitions[2].offset == 0x101000
    assert d.partitions[2].size == 0x1000
    assert d.partitions[2].type == 7
    assert d.partitions[2].guid == UUID("eebaf645-61bd-11ed-9048-010c29feac9a")
    assert d.partitions[2].open().read(512) == b"\x02" * 512

    assert d.partitions[3].number == 5
    assert d.partitions[3].offset == 0x102000
    assert d.partitions[3].size == 0x1000
    assert d.partitions[3].type == 7
    assert d.partitions[3].guid == UUID("eebaf650-61bd-11ed-9048-010c29feac9a")
    assert d.partitions[3].open().read(512) == b"\x05" * 512
