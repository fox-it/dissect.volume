from __future__ import annotations

import io
import re
from typing import BinaryIO

import pytest

from dissect.volume.exceptions import LVM2Error
from dissect.volume.lvm import LVM2
from dissect.volume.lvm.metadata import StripedSegment


def test_lvm(lvm: BinaryIO) -> None:
    vs = LVM2(lvm)

    vg = vs.vg
    assert vg.name == "vg_test"
    assert vg.id == "8HfEjs-9DNH-0dy1-U5u8-EYBF-Vce4-8BcSWU"
    assert len(vg.lv) == 1
    assert len(vg.pv) == 1

    lv = vg.lv[0]
    assert lv.name == "lv_test"
    assert lv.id == "TnYdWo-zRE9-wf2T-5nt0-M1aD-vtoP-fASCxK"
    assert lv.type == "striped"
    assert len(lv.segments) == 1

    seg = lv.segments[0]
    assert isinstance(seg, StripedSegment)
    assert len(seg.stripes) == seg.stripe_count
    assert seg.extent_count == 1
    assert seg.extent_count * vg.extent_size == 8192

    fh = lv.open()
    assert fh.read(1024) == b"\xde\xad\xbe\xef" * 256
    assert fh.read(1024) == b"\x00" * 1024

    vg.pv[0]._dev = None
    with pytest.raises(
        LVM2Error,
        match=re.escape(
            "Physical volume not found: pv0 (id=2Svcy0-cRH2-3Xrz-87Fv-zNUI-9CoI-Ycoyql, device=/dev/loop1)"
        ),
    ):
        fh = lv.open()


def test_lvm_invalid() -> None:
    buf = io.BytesIO(4096 * b"\x00")

    with pytest.raises(LVM2Error):
        LVM2(buf)


def test_lvm_thin(lvm_thin: BinaryIO) -> None:
    lvm = LVM2(lvm_thin)

    for lv_name in ["lv-1", "lv-2"]:
        lv = lvm.vg.logical_volumes[lv_name]
        assert lv.type == "thin"

        fh = lv.open()
        for i in range(1, 513):
            assert fh.read(4096) == i.to_bytes(2, "little") * 2048

    pool = lvm.vg.logical_volumes["data"]
    assert pool.type == "thin-pool"

    with pytest.raises(
        RuntimeError, match="Opening a thin-pool for reading is not possible, use open_pool\\(\\) instead"
    ):
        pool.open()


def test_lvm_mirror(lvm_mirror: list[BinaryIO]) -> None:
    lvm = LVM2(lvm_mirror)
    lv = lvm.vg.logical_volumes["mirrormirror"]

    assert lv.type == "mirror"

    fh = lv.open()
    for i in range(1, 513):
        assert fh.read(4096) == i.to_bytes(2, "little") * 2048


def test_lvm_sizes_mismatch(lvm_inconsistent_sizes: BinaryIO) -> None:
    lvm = LVM2(lvm_inconsistent_sizes)

    lv = lvm.vg.logical_volumes["lv"]

    # Size manually adjusted to be smaller than the actual data
    # This is the offset the data of a text file starts
    assert next(iter(lvm.devices.values())).size == 0x300000
    physical_volume = lvm.vg.pv[0]
    assert physical_volume.size == 0x8000000

    fh = lv.open()
    # Size of the stripe
    assert fh.size == 0x400000

    segment_stream = fh._runs[0][2]
    pv_stream = segment_stream._runs[0][2]
    # Check whether the underlying pv stream size is correct (The non adjusted size)
    assert pv_stream.size == 0x8000000

    fh.seek(0x3B8800)

    expected_data = b"A small file at the end of the disk"
    assert fh.read(len(expected_data)) == expected_data


def test_lvm_sizes_mismatch_missing_dev_size(lvm_inconsistent_sizes: BinaryIO) -> None:
    lvm = LVM2(lvm_inconsistent_sizes)

    lv = lvm.vg.logical_volumes["lv"]

    # Size manually adjusted to be smaller than the actual data
    # This is the offset the data of a text file starts
    assert next(iter(lvm.devices.values())).size == 0x300000
    physical_volume = lvm.vg.pv[0]
    # physical_volume.size will calculate size based on the the data offset
    # and size of the stripes if dev_size is not defined
    physical_volume.dev_size = None
    assert physical_volume.size == 0x500000

    fh = lv.open()
    # Size of the stripe
    assert fh.size == 0x400000

    fh.seek(0x3B8800)

    expected_data = b"A small file at the end of the disk"
    assert fh.read(len(expected_data)) == expected_data
