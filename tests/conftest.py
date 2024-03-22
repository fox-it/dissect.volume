import contextlib
import gzip
import os
from typing import IO, BinaryIO, Iterator

import pytest


def absolute_path(filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), filename)


def open_file(name: str, mode: str = "rb") -> Iterator[IO]:
    with open(absolute_path(name), mode) as f:
        yield f


def open_file_gz(name: str, mode: str = "rb") -> Iterator[IO]:
    with gzip.GzipFile(absolute_path(name), mode) as f:
        yield f


def open_files_gz(names: list[str], mode: str = "rb") -> Iterator[list[gzip.GzipFile]]:
    with contextlib.ExitStack() as stack:
        yield [stack.enter_context(gzip.GzipFile(absolute_path(name), mode)) for name in names]


@pytest.fixture
def lvm() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/lvm/lvm.bin.gz")


@pytest.fixture
def lvm_thin() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/lvm/lvm-thin.bin.gz")


@pytest.fixture
def lvm_mirror() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/lvm/lvm-mirror-1.bin.gz", "data/lvm/lvm-mirror-2.bin.gz"])


@pytest.fixture
def mbr() -> Iterator[BinaryIO]:
    yield from open_file("data/mbr.bin")


@pytest.fixture
def gpt() -> Iterator[BinaryIO]:
    yield from open_file("data/gpt.bin")


@pytest.fixture
def gpt_hybrid() -> Iterator[BinaryIO]:
    yield from open_file("data/gpt_hybrid.bin")


@pytest.fixture
def gpt_4k() -> Iterator[BinaryIO]:
    yield from open_file("data/gpt_4k.bin")


@pytest.fixture
def gpt_esxi() -> Iterator[BinaryIO]:
    yield from open_file("data/gpt_esxi.bin")


@pytest.fixture
def gpt_no_name_xff() -> Iterator[BinaryIO]:
    yield from open_file("data/gpt_no_name_xff.bin")


@pytest.fixture
def apm() -> Iterator[BinaryIO]:
    yield from open_file("data/apm.bin")


@pytest.fixture
def bsd() -> Iterator[BinaryIO]:
    yield from open_file("data/bsd.bin")


@pytest.fixture
def bsd64() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/bsd64.bin.gz")


@pytest.fixture
def dm_thin() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/dm/dm-thin-metadata.bin.gz", "data/dm/dm-thin-data.bin.gz"])


@pytest.fixture
def md_linear() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/md/md-linear-1.bin.gz", "data/md/md-linear-2.bin.gz"])


@pytest.fixture
def md_raid0() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/md/md-raid0-1.bin.gz", "data/md/md-raid0-2.bin.gz", "data/md/md-raid0-3.bin.gz"])


@pytest.fixture
def md_raid1() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/md/md-raid1-1.bin.gz", "data/md/md-raid1-2.bin.gz"])


@pytest.fixture
def md_raid4() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/md/md-raid4-1.bin.gz", "data/md/md-raid4-2.bin.gz", "data/md/md-raid4-3.bin.gz"])


@pytest.fixture
def md_raid5() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/md/md-raid5-1.bin.gz", "data/md/md-raid5-2.bin.gz", "data/md/md-raid5-3.bin.gz"])


@pytest.fixture
def md_raid6() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "data/md/md-raid6-1.bin.gz",
            "data/md/md-raid6-2.bin.gz",
            "data/md/md-raid6-3.bin.gz",
            "data/md/md-raid6-4.bin.gz",
        ]
    )


@pytest.fixture
def md_raid10() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/md/md-raid10-1.bin.gz", "data/md/md-raid10-2.bin.gz"])


@pytest.fixture
def md_90_raid1() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/md/md-90-raid1-1.bin.gz", "data/md/md-90-raid1-2.bin.gz"])


@pytest.fixture
def ddf_raid0() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        ["data/ddf/ddf-raid0-1.bin.gz", "data/ddf/ddf-raid0-2.bin.gz", "data/ddf/ddf-raid0-3.bin.gz"]
    )


@pytest.fixture
def ddf_raid1() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/ddf/ddf-raid1-1.bin.gz", "data/ddf/ddf-raid1-2.bin.gz"])


@pytest.fixture
def ddf_raid0_raid1() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/ddf/ddf-raid0-raid1-1.bin.gz", "data/ddf/ddf-raid0-raid1-2.bin.gz"])


@pytest.fixture
def ddf_raid4() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/ddf/ddf-raid4-1.bin.gz", "data/ddf/ddf-raid4-2.bin.gz"])


@pytest.fixture
def ddf_raid5() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        ["data/ddf/ddf-raid5-1.bin.gz", "data/ddf/ddf-raid5-2.bin.gz", "data/ddf/ddf-raid5-3.bin.gz"]
    )


@pytest.fixture
def ddf_raid6() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "data/ddf/ddf-raid6-1.bin.gz",
            "data/ddf/ddf-raid6-2.bin.gz",
            "data/ddf/ddf-raid6-3.bin.gz",
            "data/ddf/ddf-raid6-4.bin.gz",
        ]
    )


@pytest.fixture
def ddf_raid10() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "data/ddf/ddf-raid10-1.bin.gz",
            "data/ddf/ddf-raid10-2.bin.gz",
            "data/ddf/ddf-raid10-3.bin.gz",
            "data/ddf/ddf-raid10-4.bin.gz",
        ]
    )
