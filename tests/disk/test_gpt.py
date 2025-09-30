from __future__ import annotations

import io
import re
from typing import BinaryIO
from uuid import UUID

import pytest

from dissect.volume import disk
from dissect.volume.disk.schemes import GPT


def test_gpt(gpt: BinaryIO) -> None:
    d = disk.Disk(gpt)

    assert isinstance(d.scheme, GPT)
    assert len(d.partitions) == 3
    assert len(d.scheme.partitions) == 3
    assert d.scheme.mbr.mbr.bootsig == 0xAA55

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 20480
    assert d.partitions[0].size == 0xC7FFE00
    assert d.partitions[0].type == UUID("c12a7328-f81f-11d2-ba4b-00a0c93ec93b")
    assert d.partitions[0].guid == UUID("27d920bc-e414-45e0-9503-2606de7a1056")
    assert d.partitions[0].name == "EFI System Partition"

    assert d.partitions[1].number == 2
    assert d.partitions[1].offset == 0xC900000
    assert d.partitions[1].size == 0x7465FFE00
    assert d.partitions[1].type == UUID("ebd0a0a2-b9e5-4433-87c0-68b6b72699c7")
    assert d.partitions[1].guid == UUID("a14cecf3-b364-4d6d-a540-e245e6df9d11")
    assert d.partitions[1].name == ""


def test_gpt_invalid(gpt: BinaryIO) -> None:
    buf = io.BytesIO(gpt.read(512) + 896 * b"\x00")
    with pytest.raises(disk.DiskError):
        GPT(buf)


def test_hybrid_gpt(gpt_hybrid: BinaryIO) -> None:
    d = disk.Disk(gpt_hybrid)

    assert isinstance(d.scheme, GPT)
    assert len(d.partitions) == 5
    assert len(d.scheme.partitions) == 5
    assert d.scheme.mbr.mbr.bootsig == 0xAA55

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 0x100000
    assert d.partitions[0].size == 0x80200
    assert d.partitions[0].type == 0x83
    assert d.partitions[0].guid is None
    assert d.partitions[0].name is None

    assert d.partitions[1].number == 2
    assert d.partitions[1].offset == 0x180200
    assert d.partitions[1].size == 0x7BC00
    assert d.partitions[1].type == 0x83
    assert d.partitions[1].guid is None
    assert d.partitions[1].name is None

    assert d.partitions[2].number == 1
    assert d.partitions[2].offset == 0x100000
    assert d.partitions[2].size == 0x80000
    assert d.partitions[2].type == UUID("0fc63daf-8483-4772-8e79-3d69d8477de4")
    assert d.partitions[2].guid == UUID("44eea528-8489-4bbc-a480-56bd208cd233")
    assert d.partitions[2].name == "Linux filesystem"

    assert d.partitions[3].number == 2
    assert d.partitions[3].offset == 0x4400
    assert d.partitions[3].size == 0xFBA00
    assert d.partitions[3].type == UUID("0fc63daf-8483-4772-8e79-3d69d8477de4")
    assert d.partitions[3].guid == UUID("8f4bcd34-d9d4-4060-a683-6f75c90b795b")
    assert d.partitions[3].name == "Linux filesystem"

    assert d.partitions[4].number == 3
    assert d.partitions[4].offset == 0x180200
    assert d.partitions[4].size == 0x7BA00
    assert d.partitions[4].type == UUID("0fc63daf-8483-4772-8e79-3d69d8477de4")
    assert d.partitions[4].guid == UUID("b6aa0017-3abb-4c2b-b00c-5189e66d9896")
    assert d.partitions[4].name == "Linux filesystem"


def test_gpt_4k(gpt_4k: BinaryIO) -> None:
    with pytest.raises(
        disk.DiskError, match=re.escape("Found GPT type partition, but MBR scheme detected. Maybe 4K sector size.")
    ):
        disk.Disk(gpt_4k)

    gpt_4k.seek(0)
    d = disk.Disk(gpt_4k, sector_size=4096)

    assert isinstance(d.scheme, GPT)
    assert len(d.partitions) == 3

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 0x100000
    assert d.partitions[0].size == 0x100000
    assert d.partitions[0].type == UUID("0fc63daf-8483-4772-8e79-3d69d8477de4")
    assert d.partitions[0].guid == UUID("21b90a6e-0918-4e72-aa1a-85f8ba8ef8cc")
    assert d.partitions[0].name == "Linux filesystem"

    assert d.partitions[1].number == 2
    assert d.partitions[1].offset == 0x300000
    assert d.partitions[1].size == 0x100000
    assert d.partitions[1].type == UUID("0fc63daf-8483-4772-8e79-3d69d8477de4")
    assert d.partitions[1].guid == UUID("c6f4ad42-4652-448d-89d7-7cfa7710abe7")
    assert d.partitions[1].name == "Linux filesystem"

    assert d.partitions[2].number == 3
    assert d.partitions[2].offset == 0x500000
    assert d.partitions[2].size == 0xB5A000
    assert d.partitions[2].type == UUID("0fc63daf-8483-4772-8e79-3d69d8477de4")
    assert d.partitions[2].guid == UUID("b7230707-dcaa-4483-823b-06f9b718ee55")
    assert d.partitions[2].name == "Linux filesystem"


def test_gpt_esxi(gpt_esxi: BinaryIO) -> None:
    d = disk.Disk(gpt_esxi)

    assert isinstance(d.scheme, GPT)
    assert len(d.partitions) == 5

    assert d.partitions[0].number == 1
    assert d.partitions[0].offset == 0x8000
    assert d.partitions[0].size == 0x63FFE00
    assert d.partitions[0].type == UUID("c12a7328-f81f-11d2-ba4b-00a0c93ec93b")
    assert d.partitions[0].guid == UUID("0f7a6017-09ed-474c-b4b2-b377059d593a")
    assert d.partitions[0].name == "BOOT"

    assert d.partitions[1].number == 5
    assert d.partitions[1].offset == 0x6600000
    assert d.partitions[1].size == 0xFFEFFE00
    assert d.partitions[1].type == UUID("ebd0a0a2-b9e5-4433-87c0-68b6b72699c7")
    assert d.partitions[1].guid == UUID("cc02273b-7b9b-4075-9e93-b1755f07dca5")
    assert d.partitions[1].name == "BOOTBANK1"

    assert d.partitions[2].number == 6
    assert d.partitions[2].offset == 0x106600000
    assert d.partitions[2].size == 0xFFEFFE00
    assert d.partitions[2].type == UUID("ebd0a0a2-b9e5-4433-87c0-68b6b72699c7")
    assert d.partitions[2].guid == UUID("ec9f58a6-4b6c-45f8-a703-f212e8c28a0a")
    assert d.partitions[2].name == "BOOTBANK2"

    assert d.partitions[3].number == 7
    assert d.partitions[3].offset == 0x206600000
    assert d.partitions[3].size == 0x1DF99FFE00
    assert d.partitions[3].type == UUID("4eb2ea39-7855-4790-a79e-fae495e21f8d")
    assert d.partitions[3].guid == UUID("fc7f0906-62a5-47b2-8f40-98d9427fefe0")
    assert d.partitions[3].name == "OSDATA"

    assert d.partitions[4].number == 8
    assert d.partitions[4].offset == 0x2000100000
    assert d.partitions[4].size == 0x37FEFBC00
    assert d.partitions[4].type == UUID("aa31e02a-400f-11db-9590-000c2911d1b8")
    assert d.partitions[4].guid == UUID("327aa3da-0d50-4e97-b28f-014d92724aab")
    assert d.partitions[4].name == "datastore1"


def test_gpt_esxi_no_name_xff(gpt_no_name_xff: BinaryIO) -> None:
    d = disk.Disk(gpt_no_name_xff)

    assert len(d.partitions) == 1
    assert d.partitions[0].name == ""
