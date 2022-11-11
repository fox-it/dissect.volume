from dissect.volume import disk
from dissect.volume.disk.schemes import BSD


def test_bsd(bsd):
    vs = BSD(bsd)

    assert len(vs.partitions) == 3

    assert vs.partitions[0].number == 1
    assert vs.partitions[0].offset == 0x800
    assert vs.partitions[0].size == 0x200
    assert vs.partitions[0].type == 7
    assert vs.partitions[0].open().read() == b"\x01" * 512

    assert vs.partitions[1].number == 2
    assert vs.partitions[1].offset == 0xC00
    assert vs.partitions[1].size == 0x200
    assert vs.partitions[1].type == 1
    assert vs.partitions[1].open().read() == b"\x02" * 512

    assert vs.partitions[2].number == 5
    assert vs.partitions[2].offset == 0x1000
    assert vs.partitions[2].size == 0x200
    assert vs.partitions[2].type == 27
    assert vs.partitions[2].open().read() == b"\x05" * 512


def test_bsd64(bsd64):
    # The disklabel64 partition is a separate GPT partition in this test data
    d = disk.Disk(bsd64)
    vol = d.partitions[1].open()
    vs = BSD(vol)

    assert len(vs.partitions) == 3

    assert vs.partitions[0].number == 1
    assert vs.partitions[0].offset == 0x100000
    assert vs.partitions[0].size == 0x1000
    assert vs.partitions[0].type == 7
    assert vs.partitions[0].open().read(512) == b"\x01" * 512

    assert vs.partitions[1].number == 2
    assert vs.partitions[1].offset == 0x101000
    assert vs.partitions[1].size == 0x1000
    assert vs.partitions[1].type == 7
    assert vs.partitions[1].open().read(512) == b"\x02" * 512

    assert vs.partitions[2].number == 5
    assert vs.partitions[2].offset == 0x102000
    assert vs.partitions[2].size == 0x1000
    assert vs.partitions[2].type == 7
    assert vs.partitions[2].open().read(512) == b"\x05" * 512
