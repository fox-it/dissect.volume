import io
from typing import BinaryIO

import pytest

from dissect.volume.exceptions import LVM2Error
from dissect.volume.lvm import LVM2
from dissect.volume.lvm.segment import StripedSegment


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
    assert len(lv.segments) == 1

    seg = lv.segments[0]
    assert isinstance(seg, StripedSegment)
    assert len(seg.stripes) == seg.stripe_count
    assert seg.extent_count == 1
    assert seg.extent_count * vg.extent_size == 8192

    fh = lv.open()
    assert fh.read(1024) == b"\xde\xad\xbe\xef" * 256
    assert fh.read(1024) == b"\x00" * 1024


def test_lvm_invalid() -> None:
    buf = io.BytesIO(4096 * b"\x00")

    with pytest.raises(LVM2Error):
        LVM2(buf)


def test_lvm_thin(lvm_thin: BinaryIO) -> None:
    lvm = LVM2(lvm_thin)

    for lv_name in ["lv-1", "lv-2"]:
        lv = lvm.vg.logical_volumes[lv_name]
        fh = lv.open()
        for sector in range(4096):
            assert fh.read(512) == bytes([sector & 0xFF] * 512)


def test_lvm_mirror(lvm_mirror: list[BinaryIO]) -> None:
    lvm = LVM2(lvm_mirror)

    fh = lvm.vg.logical_volumes["mirrormirror"].open()
    for sector in range(4096):
        assert fh.read(512) == bytes([sector & 0xFF] * 512)
