from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from typing import BinaryIO

import pytest

from dissect.volume.raid.stream import Layout, Level
from dissect.volume.vinum.c_vinum import c_vinum
from dissect.volume.vinum.vinum import (
    Vinum,
    VinumMirrorDisk,
    VinumPhysicalDisk,
    VinumPlexDisk,
)


@pytest.mark.parametrize(
    (
        "disk_files",
        "name",
        "uuid",
        "size",
        "is_mirror",
        "level",
        "layout",
        "stripe_size",
        "num_disks",
        "physical_disks",
        "read_offset",
    ),
    [
        (
            "vinum_concat",
            "my-concat-vol",
            "my-concat-vol.p0",
            1825792,
            False,
            Level.LINEAR,
            0,
            0,
            2,
            (
                (
                    (0, 0, b"gvinumdrive2", datetime(2024, 10, 21, 13, 14, 44, 653168, tzinfo=timezone.utc)),
                    (1, 0, b"gvinumdrive3", datetime(2024, 10, 21, 13, 14, 44, 653168, tzinfo=timezone.utc)),
                ),
            ),
            1024 * 1024,
        ),
        (
            "vinum_mirror",
            "my-mirror-vol",
            "my-mirror-vol",
            912896,
            True,
            Level.LINEAR,
            0,
            0,
            1,
            (
                ((0, 0, b"gvinumdrive8", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),),
                ((0, 0, b"gvinumdrive9", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),),
            ),
            512 * 1024,
        ),
        (
            "vinum_raid5",
            "my-raid5-vol",
            "my-raid5-vol.p0",
            2359296,
            False,
            Level.RAID5,
            0,
            262144,
            4,
            (
                (
                    (0, 0, b"gvinumdrive4", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),
                    (1, 0, b"gvinumdrive5", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),
                    (2, 0, b"gvinumdrive6", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),
                    (3, 0, b"gvinumdrive7", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),
                ),
            ),
            1024 * 1024,
        ),
        (
            "vinum_striped",
            "my-striped-vol",
            "my-striped-vol.p0",
            1572864,
            False,
            Level.RAID0,
            0,
            262144,
            2,
            (
                (
                    (0, 0, b"gvinumdrive0", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),
                    (1, 0, b"gvinumdrive1", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),
                ),
            ),
            1024 * 1024,
        ),
        (
            "vinum_stripedmirror",
            "my-stripedmirror-vol",
            "my-stripedmirror-vol",
            1572864,
            True,
            Level.RAID0,
            0,
            262144,
            2,
            (
                (
                    (0, 0, b"gvinumdrive10", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),
                    (1, 0, b"gvinumdrive12", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),
                ),
                (
                    (0, 0, b"gvinumdrive11", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),
                    (1, 0, b"gvinumdrive13", datetime(2024, 9, 24, 12, 20, 54, 919756, tzinfo=timezone.utc)),
                ),
            ),
            1024 * 1024,
        ),
    ],
)
def test_vinum(
    disk_files: str,
    name: bytes,
    uuid: bytes,
    size: int,
    is_mirror: bool,
    level: Level,
    layout: Layout,
    stripe_size: int,
    num_disks: int,
    physical_disks: tuple[tuple[tuple[int, int, bytes, datetime]]],  # disk_idx, data_offset, name, config_time
    read_offset: int,
    request: pytest.FixtureRequest,
) -> None:
    disk_files: list[BinaryIO] = request.getfixturevalue(disk_files)
    vinum = Vinum(disk_files)

    assert len(vinum.configurations) == 1
    assert len(vinum.configurations[0].virtual_disks) == 1

    vd = vinum.configurations[0].virtual_disks[0]
    assert vd.name == name
    assert vd.uuid == uuid
    assert vd.size == size

    if is_mirror:
        # these are always the same for any mirror type
        assert vd.level == Level.RAID1
        assert isinstance(vd, VinumMirrorDisk)
        assert vd.layout == 0
        assert vd.stripe_size == 0
        assert vd.num_disks == 2
        assert len(vd.disk_map) == 2
        disk_map = vd.disk_map
    else:
        # fake disk map
        disk_map = {0: (0, vd)}

    for check_idx, (idx_i, (disk_offset_i, vd_i)) in enumerate(disk_map.items()):
        # as the indexes in a disk_map for a mirror set are artificial, they
        # should increase monotonously from 0.
        assert idx_i == check_idx

        if is_mirror:
            # these are always the same for any mirror type and virtual disk in the mirror set
            assert disk_offset_i == 0
            assert isinstance(vd_i, VinumPlexDisk)
            vd_i_uuid = f"{uuid}.p{idx_i}"
            assert vd_i.name == name
            assert vd_i.uuid == vd_i_uuid
            assert vd_i.size == size  # Always the same as vd.size

        # in our case, for mirror sets these are the same for each virtual disk in the set
        assert vd_i.level == level
        assert vd_i.layout == layout
        assert vd_i.stripe_size == stripe_size
        assert vd_i.num_disks == num_disks
        assert len(vd_i.disk_map) == num_disks

        pdisks = physical_disks[idx_i]
        vpds = sorted(vd_i.disk_map.items())
        for idx, (disk_idx, (disk_offset, vpd)) in enumerate(vpds):
            # these happen to be always the same for our test images
            assert isinstance(vpd, VinumPhysicalDisk)
            assert vpd.offset == 135680
            assert vpd.size == 1048576

            # these are always specific for any physical disk in in the virtual
            # disk, also for the disks in the virtual disks of a mirror set
            pdisk = pdisks[idx]
            assert disk_idx == pdisk[0]
            assert disk_offset == pdisk[1]
            assert vpd.id == pdisk[2]
            assert vpd.name == pdisk[2].decode(errors="backslashreplace")
            assert vpd.config_time == pdisk[3]

    disk = vd.open()
    disk.seek(read_offset)
    data = disk.read(256)
    expected = bytearray(ii for ii in range(256))
    assert data == expected


@pytest.mark.parametrize(
    ("magic", "active"),
    [
        (0x494E2056494E4F00, True),
        (0x4E4F2056494E4F00, False),
        (0x56494E554D2D3100, True),
        (0x56494E554D2D2D00, False),
    ],
)
def test_vinum_physical_disk(magic: int, active: bool) -> None:
    header = c_vinum.header()
    header.magic = magic

    fake_disk = BytesIO(b"\x00" * c_vinum.GV_HDR_OFFSET + bytes(header))
    vpd = VinumPhysicalDisk(fake_disk)

    assert vpd.active == active


def test_vinum_physical_disk_invalid() -> None:
    header = c_vinum.header()
    header.magic = 0xDEADCAFEDEADCAFE

    fake_disk = BytesIO(b"\x00" * c_vinum.GV_HDR_OFFSET + bytes(header))
    with pytest.raises(ValueError, match="File-like object is not a Vinum device"):
        VinumPhysicalDisk(fake_disk)
