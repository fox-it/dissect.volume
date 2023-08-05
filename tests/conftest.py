import contextlib
import gzip
import os
from typing import IO, BinaryIO, Iterator

import pytest


def absolute_path(filename) -> str:
    return os.path.join(os.path.dirname(__file__), filename)


def open_file(name, mode="rb") -> Iterator[IO]:
    with open(absolute_path(name), mode) as f:
        yield f


def open_file_gz(name, mode="rb") -> Iterator[IO]:
    with gzip.GzipFile(absolute_path(name), mode) as f:
        yield f


def open_files_gz(names: list[str], mode: str = "rb") -> Iterator[list[gzip.GzipFile]]:
    with contextlib.ExitStack() as stack:
        yield [stack.enter_context(gzip.GzipFile(absolute_path(name), mode)) for name in names]


@pytest.fixture
def lvm() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/lvm.bin.gz")


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
