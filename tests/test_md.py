from typing import BinaryIO

import pytest

from dissect.volume.md.md import MD
from dissect.volume.md.stream import RAID0Stream


@pytest.mark.parametrize(
    "fixture, name, level",
    [
        ("md_linear", "fedora:linear", -1),
        ("md_raid0", "fedora:raid0", 0),
        ("md_raid1", "fedora:raid1", 1),
        ("md_raid4", "fedora:raid4", 4),
        ("md_raid5", "fedora:raid5", 5),
        ("md_raid6", "fedora:raid6", 6),
        ("md_raid10", "fedora:raid10", 10),
        ("md_90_raid1", None, 1),
    ],
)
def test_md_read(fixture: str, name: str, level: int, request: pytest.FixtureRequest) -> None:
    md = MD(request.getfixturevalue(fixture))

    assert md.name == name
    assert md.level == level
    fh = md.open()

    for i in range(1, 513):
        assert fh.read(4096) == i.to_bytes(2, "little") * 2048


def test_md_raid0_zones(md_raid0: list[BinaryIO]) -> None:
    md = MD(md_raid0)

    fh = md.open()

    assert isinstance(fh, RAID0Stream)
    assert len(fh.zones) == 2

    assert fh.zones[0].zone_end == 6144
    assert fh.zones[0].dev_start == 0
    assert len(fh.zones[0].devices) == 3

    assert fh.zones[1].zone_end == 10240
    assert fh.zones[1].dev_start == 2048
    assert len(fh.zones[1].devices) == 2
