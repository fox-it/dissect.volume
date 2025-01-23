from __future__ import annotations

import io
from typing import BinaryIO

import pytest

from dissect.volume import disk
from dissect.volume.disk.schemes import APM


def test_apm(apm: BinaryIO) -> None:
    d = disk.Disk(apm)

    assert isinstance(d.scheme, APM)
    assert len(d.partitions) == 3
    assert d.scheme.apm.signature == b"PM"

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 0x200
    assert d.partitions[0].size == 0x7E00
    assert d.partitions[0].type == "Apple_partition_map"
    assert d.partitions[0].name == "Apple"

    assert d.partitions[1].number == 2
    assert d.partitions[1].offset == 0x8000
    assert d.partitions[1].size == 0x752FF6000
    assert d.partitions[1].type == "Windows_NTFS"
    assert d.partitions[1].name == ""

    assert d.partitions[2].number == 3
    assert d.partitions[2].offset == 0x752FFE000
    assert d.partitions[2].size == 0x2000
    assert d.partitions[2].type == "Apple_Free"
    assert d.partitions[2].name == ""


def test_apm_invalid() -> None:
    buf = io.BytesIO(512 * b"\x00")
    with pytest.raises(disk.DiskError):
        APM(buf)
