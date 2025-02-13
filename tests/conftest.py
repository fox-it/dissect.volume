from __future__ import annotations

import contextlib
import gzip
from pathlib import Path
from typing import IO, TYPE_CHECKING, BinaryIO

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


def absolute_path(filename: str) -> Path:
    return Path(__file__).parent / filename


def open_file(name: str, mode: str = "rb") -> Iterator[IO]:
    with absolute_path(name).open(mode) as fh:
        yield fh


def open_file_gz(name: str, mode: str = "rb") -> Iterator[IO]:
    with gzip.GzipFile(absolute_path(name), mode) as fh:
        yield fh


def open_files_gz(names: list[str], mode: str = "rb") -> Iterator[list[gzip.GzipFile]]:
    with contextlib.ExitStack() as stack:
        yield [stack.enter_context(gzip.GzipFile(absolute_path(name), mode)) for name in names]


@pytest.fixture
def lvm() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/lvm/lvm.bin.gz")


@pytest.fixture
def lvm_thin() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/lvm/lvm-thin.bin.gz")


@pytest.fixture
def lvm_mirror() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/lvm/lvm-mirror-1.bin.gz", "_data/lvm/lvm-mirror-2.bin.gz"])


@pytest.fixture
def mbr() -> Iterator[BinaryIO]:
    yield from open_file("_data/disk/mbr.bin")


@pytest.fixture
def gpt() -> Iterator[BinaryIO]:
    yield from open_file("_data/disk/gpt.bin")


@pytest.fixture
def gpt_hybrid() -> Iterator[BinaryIO]:
    yield from open_file("_data/disk/gpt_hybrid.bin")


@pytest.fixture
def gpt_4k() -> Iterator[BinaryIO]:
    yield from open_file("_data/disk/gpt_4k.bin")


@pytest.fixture
def gpt_esxi() -> Iterator[BinaryIO]:
    yield from open_file("_data/disk/gpt_esxi.bin")


@pytest.fixture
def gpt_no_name_xff() -> Iterator[BinaryIO]:
    yield from open_file("_data/disk/gpt_no_name_xff.bin")


@pytest.fixture
def apm() -> Iterator[BinaryIO]:
    yield from open_file("_data/disk/apm.bin")


@pytest.fixture
def bsd() -> Iterator[BinaryIO]:
    yield from open_file("_data/disk/bsd.bin")


@pytest.fixture
def bsd64() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/disk/bsd64.bin.gz")


@pytest.fixture
def dm_thin() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/dm/dm-thin-metadata.bin.gz", "_data/dm/dm-thin-data.bin.gz"])


@pytest.fixture
def dm_thin_empty() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/dm/dm-thin-empty-metadata.bin.gz", "_data/dm/dm-thin-empty-data.bin.gz"])


@pytest.fixture
def md_linear() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/md/md-linear-1.bin.gz", "_data/md/md-linear-2.bin.gz"])


@pytest.fixture
def md_raid0() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/md/md-raid0-1.bin.gz", "_data/md/md-raid0-2.bin.gz", "_data/md/md-raid0-3.bin.gz"])


@pytest.fixture
def md_raid1() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/md/md-raid1-1.bin.gz", "_data/md/md-raid1-2.bin.gz"])


@pytest.fixture
def md_raid4() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/md/md-raid4-1.bin.gz", "_data/md/md-raid4-2.bin.gz", "_data/md/md-raid4-3.bin.gz"])


@pytest.fixture
def md_raid5() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/md/md-raid5-1.bin.gz", "_data/md/md-raid5-2.bin.gz", "_data/md/md-raid5-3.bin.gz"])


@pytest.fixture
def md_raid6() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "_data/md/md-raid6-1.bin.gz",
            "_data/md/md-raid6-2.bin.gz",
            "_data/md/md-raid6-3.bin.gz",
            "_data/md/md-raid6-4.bin.gz",
        ]
    )


@pytest.fixture
def md_raid10() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/md/md-raid10-1.bin.gz", "_data/md/md-raid10-2.bin.gz"])


@pytest.fixture
def md_90_raid1() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/md/md-90-raid1-1.bin.gz", "_data/md/md-90-raid1-2.bin.gz"])


@pytest.fixture
def ddf_raid0() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        ["_data/ddf/ddf-raid0-1.bin.gz", "_data/ddf/ddf-raid0-2.bin.gz", "_data/ddf/ddf-raid0-3.bin.gz"]
    )


@pytest.fixture
def ddf_raid1() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/ddf/ddf-raid1-1.bin.gz", "_data/ddf/ddf-raid1-2.bin.gz"])


@pytest.fixture
def ddf_raid0_raid1() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/ddf/ddf-raid0-raid1-1.bin.gz", "_data/ddf/ddf-raid0-raid1-2.bin.gz"])


@pytest.fixture
def ddf_raid4() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/ddf/ddf-raid4-1.bin.gz", "_data/ddf/ddf-raid4-2.bin.gz"])


@pytest.fixture
def ddf_raid5() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        ["_data/ddf/ddf-raid5-1.bin.gz", "_data/ddf/ddf-raid5-2.bin.gz", "_data/ddf/ddf-raid5-3.bin.gz"]
    )


@pytest.fixture
def ddf_raid6() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "_data/ddf/ddf-raid6-1.bin.gz",
            "_data/ddf/ddf-raid6-2.bin.gz",
            "_data/ddf/ddf-raid6-3.bin.gz",
            "_data/ddf/ddf-raid6-4.bin.gz",
        ]
    )


@pytest.fixture
def ddf_raid10() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "_data/ddf/ddf-raid10-1.bin.gz",
            "_data/ddf/ddf-raid10-2.bin.gz",
            "_data/ddf/ddf-raid10-3.bin.gz",
            "_data/ddf/ddf-raid10-4.bin.gz",
        ]
    )


@pytest.fixture
def vinum_concat() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "_data/vinum/vinum-concat_diska.bin.gz",
            "_data/vinum/vinum-concat_diskb.bin.gz",
        ]
    )


@pytest.fixture
def vinum_mirror() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "_data/vinum/vinum-mirror_diska.bin.gz",
            "_data/vinum/vinum-mirror_diskb.bin.gz",
        ]
    )


@pytest.fixture
def vinum_raid5() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "_data/vinum/vinum-raid5_diska.bin.gz",
            "_data/vinum/vinum-raid5_diskb.bin.gz",
            "_data/vinum/vinum-raid5_diskc.bin.gz",
            "_data/vinum/vinum-raid5_diskd.bin.gz",
        ]
    )


@pytest.fixture
def vinum_striped() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "_data/vinum/vinum-striped_diska.bin.gz",
            "_data/vinum/vinum-striped_diskb.bin.gz",
        ]
    )


@pytest.fixture
def vinum_stripedmirror() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "_data/vinum/vinum-stripedmirror_diska.bin.gz",
            "_data/vinum/vinum-stripedmirror_diskb.bin.gz",
            "_data/vinum/vinum-stripedmirror_diskc.bin.gz",
            "_data/vinum/vinum-stripedmirror_diskd.bin.gz",
        ]
    )
