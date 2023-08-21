import pytest

from dissect.volume.ddf.ddf import DDF


@pytest.mark.parametrize(
    "fixture, name, level, size, num_test_blocks",
    [
        ("ddf_raid0", "ddf-raid0", 0, 0xC00000, 3072),
        ("ddf_raid1", "ddf-raid1", 1, 0x400000, 512),
        ("ddf_raid4", "ddf-raid4", 4, 0x400000, 512),
        ("ddf_raid5", "ddf-raid5", 5, 0x800000, 512),
        ("ddf_raid6", "ddf-raid6", 6, 0x800000, 512),
        ("ddf_raid10", "ddf-raid10", 10, 0x800000, 512),
    ],
)
def test_ddf_read(fixture: str, name: str, level: int, size: int, num_test_blocks: int, request: pytest.FixtureRequest):
    ddf = DDF(request.getfixturevalue(fixture))
    assert len(ddf.configurations) == 1
    assert len(ddf.configurations[0].virtual_disks) == 1  # These test files only have one volume

    vd = ddf.configurations[0].virtual_disks[0]
    assert vd.name == name
    assert vd.level == level
    assert vd.size == size

    fh = vd.open()
    for i in range(1, num_test_blocks + 1):
        assert fh.read(4096) == i.to_bytes(2, "little") * 2048
