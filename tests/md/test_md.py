from __future__ import annotations

from io import BytesIO
from typing import BinaryIO

import pytest

from dissect.volume.md.md import MD, find_super_block
from dissect.volume.raid.stream import RAID0Stream


@pytest.mark.parametrize(
    ("fixture", "name", "uuid", "level", "size", "num_test_blocks"),
    [
        pytest.param(
            "md_linear",
            "fedora:linear",
            "3657debc-2cb4-6c49-d01c-76653ae532ac",
            -1,
            0x200000,
            512,
            id="linear",
        ),
        pytest.param(
            "md_raid0",
            "fedora:raid0",
            "e6b48b47-f483-6050-6591-94edd9949c0e",
            0,
            0x500000,
            512,
            id="raid0",
        ),
        pytest.param(
            "md_raid1",
            "fedora:raid1",
            "914306a9-ed9f-19b3-c26b-9d1f5a37ddcf",
            1,
            0x200000,
            512,
            id="raid1",
        ),
        pytest.param(
            "md_raid4",
            "fedora:raid4",
            "0baab6c0-a8b6-6d02-1f24-d25cb6fc685c",
            4,
            0x200000,
            512,
            id="raid4",
        ),
        pytest.param(
            "md_raid5",
            "fedora:raid5",
            "04285eaf-4cf2-4b78-dbf9-550e7430ac94",
            5,
            0x200000,
            512,
            id="raid5",
        ),
        pytest.param(
            "md_raid6",
            "fedora:raid6",
            "bc6b1414-2b5d-0dbc-a9fe-6961381d366c",
            6,
            0x200000,
            512,
            id="raid6",
        ),
        pytest.param(
            "md_raid10",
            "fedora:raid10",
            "624a35d7-72a4-01e5-b00e-ec9905cd32e7",
            10,
            0x200000,
            512,
            id="raid10",
        ),
        pytest.param(
            "md_90_raid1",
            None,
            "810dec20-9deb-9b1b-6ae8-99361087e917",
            1,
            0x178000,
            512,
            id="90_raid1",
        ),
    ],
)
def test_md_read(
    fixture: str, name: str, uuid: str, level: int, size: int, num_test_blocks: int, request: pytest.FixtureRequest
) -> None:
    md = MD(request.getfixturevalue(fixture))

    conf = md.configurations
    assert len(conf) == 1
    assert len(conf[0].virtual_disks) == 1

    vd = conf[0].virtual_disks[0]
    assert vd.name == name
    assert vd.uuid == uuid
    assert vd.level == level
    assert vd.size == size

    fh = vd.open()
    for i in range(1, num_test_blocks + 1):
        assert fh.read(4096) == i.to_bytes(2, "little") * 2048, f"Failed at block {i}"


def test_md_raid0_zones(md_raid0: list[BinaryIO]) -> None:
    md = MD(md_raid0)

    conf = md.configurations
    assert len(conf) == 1
    assert len(conf[0].virtual_disks) == 1

    vd = conf[0].virtual_disks[0]

    fh = vd.open()

    assert isinstance(fh, RAID0Stream)
    assert len(fh.zones) == 2

    assert fh.zones[0].zone_end == 3145728
    assert fh.zones[0].dev_start == 0
    assert len(fh.zones[0].devices) == 3

    assert fh.zones[1].zone_end == 5242880
    assert fh.zones[1].dev_start == 1048576
    assert len(fh.zones[1].devices) == 2


def test_md_search_sb_none_size() -> None:
    fh = BytesIO()
    fh.size = None

    assert find_super_block(fh) == (None, None, None)


def test_md_raid1_multiple_disks(md_raid1: list[BinaryIO]) -> None:
    md = MD(md_raid1[-1:])

    conf = md.configurations
    vd = conf[0].virtual_disks[0]

    assert vd.level == 1
    assert vd.size == 0x200000

    fh = vd.open()
    for i in range(1, 512 + 1):
        assert fh.read(4096) == i.to_bytes(2, "little") * 2048, f"Failed at block {i}"
