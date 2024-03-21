from typing import BinaryIO

import pytest

from dissect.volume.dm.thin import ThinPool
from dissect.volume.exceptions import DMError


def test_dm_thin(dm_thin: list[BinaryIO]) -> None:
    metadata_fh, data_fh = dm_thin

    pool = ThinPool(metadata_fh, data_fh)
    thin_0 = pool.open(0, 2 * 1024 * 1024)
    thin_1 = pool.open(1, 2 * 1024 * 1024)

    with pytest.raises(DMError, match="Device ID is not known in pool: 2"):
        pool.open(2)

    for fh in [thin_0, thin_1]:
        for i in range(1, 513):
            assert fh.read(4096) == i.to_bytes(2, "little") * 2048

    thin_no_size = pool.open(0)
    thin_no_size.seek((1024 * 1024 * 2) - 512)
    assert len(thin_no_size.read(1024)) == 1024


def test_dm_thin_empty(dm_thin_empty: list[BinaryIO]) -> None:
    metadata_fh, data_fh = dm_thin_empty
    pool = ThinPool(metadata_fh, data_fh)

    dev = pool.open(0)

    assert dev.read(512) == b"\x00" * 512

    dev.seek(512 * 1024)
    assert dev.read(512) != b"\x00" * 512

    # Far beyond the file boundary
    dev.seek(2**64)
    assert dev.read(512) == b"\x00" * 512

    dev = pool.open(0, 512)
    assert dev.read(512) == b"\x00" * 512
    assert dev.read(512) == b""
