import io

import pytest

from dissect.volume.exceptions import LVM2Error
from dissect.volume.lvm import LVM2


def test_lvm(lvm):
    lvm = LVM2(lvm)

    vg = lvm.vg
    assert vg.name == "vg_test"
    assert vg.id == "8HfEjs-9DNH-0dy1-U5u8-EYBF-Vce4-8BcSWU"
    assert len(vg.lv) == 1
    assert len(vg.pv) == 1

    lv = vg.lv[0]
    assert lv.metadata.name == "lv_test"
    assert lv.metadata.id == "TnYdWo-zRE9-wf2T-5nt0-M1aD-vtoP-fASCxK"
    assert len(lv.segments) == 1

    seg = lv.segments[0]
    assert seg.metadata.name == "segment1"
    assert len(seg.stripes) == seg.metadata.stripe_count
    assert seg.stripe_size == 8192

    assert lv.read(1024) == b"\xde\xad\xbe\xef" * 256
    assert lv.read(1024) == b"\x00" * 1024


def test_lvm_invalid():
    buf = io.BytesIO(4096 * b"\x00")

    with pytest.raises(LVM2Error):
        LVM2(buf)
