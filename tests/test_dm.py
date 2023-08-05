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
