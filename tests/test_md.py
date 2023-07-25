from typing import BinaryIO

import pytest

from dissect.volume.md.md import MD


def assert_test_bytes(fh: BinaryIO, num_test_sectors: int) -> None:
    for sector in range(num_test_sectors):
        assert fh.read(512) == bytes([sector & 0xFF] * 512)


@pytest.mark.parametrize(
    "fixture, name, level, num_test_sectors",
    [
        ("md_linear", "fedora:1", -1, 4096),
        ("md_raid0", "fedora:9", 0, 22528),
        ("md_raid1", "fedora:3", 1, 4096),
        ("md_raid4", "fedora:5", 4, 4096),
        ("md_raid5", "fedora:8", 5, 4096),
        ("md_raid6", "fedora:7", 6, 4096),
        ("md_raid10", "fedora:4", 10, 4096),
        ("md_90", None, 1, 4096),
    ],
)
def test_md_read(fixture: str, name: str, level: int, num_test_sectors: int, request: pytest.FixtureRequest):
    md = MD(request.getfixturevalue(fixture))

    assert md.name == name
    assert md.level == level
    fh = md.open()

    assert_test_bytes(fh, num_test_sectors)
