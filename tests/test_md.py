from typing import BinaryIO

import pytest

from dissect.volume.exceptions import RAIDError
from dissect.volume.md.md import MD
from dissect.volume.raid.stream import RAID0Stream


@pytest.mark.parametrize(
    "fixture, name, level, size, num_test_blocks",
    [
        ("md_linear", "fedora:linear", -1, 0x200000, 512),
        ("md_raid0", "fedora:raid0", 0, 0x500000, 512),
        ("md_raid1", "fedora:raid1", 1, 0x200000, 512),
        ("md_raid4", "fedora:raid4", 4, 0x200000, 512),
        ("md_raid5", "fedora:raid5", 5, 0x200000, 512),
        ("md_raid6", "fedora:raid6", 6, 0x200000, 512),
        ("md_raid10_near", "fedora:raid10", 10, 0x200000, 512),
        ("md_raid10_offset", "fedora:raid10", 10, 0x200000, 512),
        ("md_raid10_far", "fedora:raid10", 10, 0x200000, 512),
        ("md_90_raid1", None, 1, 0x178000, 512),
    ],
)
def test_md_read(
    fixture: str, name: str, level: int, size: int, num_test_blocks: int, request: pytest.FixtureRequest
) -> None:
    md = MD(request.getfixturevalue(fixture))

    conf = md.configurations
    assert len(conf) == 1
    assert len(conf[0].virtual_disks) == 1

    vd = conf[0].virtual_disks[0]
    assert vd.name == name
    assert vd.level == level
    assert vd.size == size

    fh = vd.open()
    for i in range(1, num_test_blocks + 1):
        assert fh.read(4096) == i.to_bytes(2, "little") * 2048


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


def test_md_raid10_fallback(md_raid10: list[BinaryIO]) -> None:
    dev0, dev1, dev2, dev3 = md_raid10

    for devices in [
        # Missing any one device should work
        [dev0, dev1, dev2],
        [dev0, dev1, dev3],
        [dev0, dev2, dev3],
        [dev1, dev2, dev3],
        # Missing two devices should work as long as we have the right 2
        [dev0, dev2],
        [dev1, dev3],
        [dev0, dev3],
        [dev1, dev2],
    ]:
        md = MD(devices)
        assert len(md.devices) == len(devices)

        fh = md.configurations[0].virtual_disks[0].open()
        for j in range(1, 513):
            assert fh.read(4096) == j.to_bytes(2, "little") * 2048

    # If we're missing a mirror or stripe, we should get an error
    for devices in [
        # Wrong 2 devices
        [dev0, dev1],
        [dev2, dev3],
        # We don't have a mirror or stripe
        [dev0],
        [dev1],
        [dev2],
        [dev3],
    ]:
        md = MD(list(devices))
        assert len(md.devices) == len(devices)

        with pytest.raises(RAIDError, match=r"Unable to find device for offset \d+"):
            fh = md.configurations[0].virtual_disks[0].open()
            for j in range(1, 513):
                assert fh.read(4096) == j.to_bytes(2, "little") * 2048
