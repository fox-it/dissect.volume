import gzip
import os

import pytest


def absolute_path(filename):
    return os.path.join(os.path.dirname(__file__), filename)


def open_file(name, mode="rb"):
    with open(absolute_path(name), mode) as f:
        yield f


def open_file_gz(name, mode="rb"):
    with gzip.GzipFile(absolute_path(name), mode) as f:
        yield f


@pytest.fixture
def lvm():
    yield from open_file_gz("data/lvm.bin.gz")


@pytest.fixture
def mbr():
    yield from open_file("data/mbr.bin")


@pytest.fixture
def gpt():
    yield from open_file("data/gpt.bin")


@pytest.fixture
def gpt_hybrid():
    yield from open_file("data/gpt_hybrid.bin")


@pytest.fixture
def gpt_4k():
    yield from open_file("data/gpt_4k.bin")


@pytest.fixture
def gpt_esxi():
    yield from open_file("data/gpt_esxi.bin")


@pytest.fixture
def gpt_no_name_xff():
    yield from open_file("data/gpt_no_name_xff.bin")


@pytest.fixture
def apm():
    yield from open_file("data/apm.bin")


@pytest.fixture
def bsd():
    yield from open_file("data/bsd.bin")


@pytest.fixture
def bsd64():
    yield from open_file_gz("data/bsd64.bin.gz")
