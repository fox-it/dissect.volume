import os
import gzip

import pytest


def absolute_path(filename):
    return os.path.join(os.path.dirname(__file__), filename)


@pytest.fixture
def lvm():
    name = "data/lvm.bin.gz"
    with gzip.GzipFile(absolute_path(name), "rb") as f:
        yield f


@pytest.fixture
def mbr():
    name = "data/mbr.bin"
    with open(absolute_path(name), "rb") as f:
        yield f


@pytest.fixture
def gpt():
    name = "data/gpt.bin"
    with open(absolute_path(name), "rb") as f:
        yield f


@pytest.fixture
def gpt_hybrid():
    name = "data/gpt_hybrid.bin"
    with open(absolute_path(name), "rb") as f:
        yield f


@pytest.fixture
def gpt_4k():
    name = "data/gpt_4k.bin"
    with open(absolute_path(name), "rb") as f:
        yield f


@pytest.fixture
def gpt_esxi():
    name = "data/gpt_esxi.bin"
    with open(absolute_path(name), "rb") as f:
        yield f


@pytest.fixture
def apm():
    name = "data/apm.bin"
    with open(absolute_path(name), "rb") as f:
        yield f


@pytest.fixture
def gpt_no_name_xff():
    name = "data/gpt_no_name_xff.bin"
    with open(absolute_path(name), "rb") as f:
        yield f
