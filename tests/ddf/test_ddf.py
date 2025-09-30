from __future__ import annotations

import pytest

from dissect.volume.ddf.ddf import DDF


@pytest.mark.parametrize(
    ("fixture", "name", "uuid", "level", "size", "num_test_blocks"),
    [
        pytest.param(
            "ddf_raid0",
            "ddf-raid0",
            "4c696e75782d4d44deadbeef0000000051fd9ba95e686be3",
            0,
            0xC00000,
            3072,
            id="raid0",
        ),
        pytest.param(
            "ddf_raid1",
            "ddf-raid1",
            "4c696e75782d4d44deadbeef0000000051fd9baae22e20c0",
            1,
            0x400000,
            512,
            id="raid1",
        ),
        pytest.param(
            "ddf_raid4",
            "ddf-raid4",
            "4c696e75782d4d44deadbeef0000000051fd9baa07f09007",
            4,
            0x400000,
            512,
            id="raid4",
        ),
        pytest.param(
            "ddf_raid5",
            "ddf-raid5",
            "4c696e75782d4d44deadbeef0000000051fd9bab56784e83",
            5,
            0x800000,
            512,
            id="raid5",
        ),
        pytest.param(
            "ddf_raid6",
            "ddf-raid6",
            "4c696e75782d4d44deadbeef0000000051fd9bac35e3d6c2",
            6,
            0x800000,
            512,
            id="raid6",
        ),
        pytest.param(
            "ddf_raid10",
            "ddf-raid10",
            "4c696e75782d4d44deadbeef0000000051fd9bad5648f635",
            10,
            0x800000,
            512,
            id="raid10",
        ),
    ],
)
def test_ddf_read(
    fixture: str, name: str, uuid: str, level: int, size: int, num_test_blocks: int, request: pytest.FixtureRequest
) -> None:
    ddf = DDF(request.getfixturevalue(fixture))
    assert len(ddf.configurations) == 1
    assert len(ddf.configurations[0].virtual_disks) == 1  # These test files only have one volume

    vd = ddf.configurations[0].virtual_disks[0]
    assert vd.name == name
    assert vd.uuid == uuid
    assert vd.level == level
    assert vd.size == size

    fh = vd.open()
    for i in range(1, num_test_blocks + 1):
        assert fh.read(4096) == i.to_bytes(2, "little") * 2048
