from __future__ import annotations

import ast
import logging
import os
import re
from bisect import bisect_right
from datetime import datetime
from functools import cached_property
from typing import Any, BinaryIO, Iterator, Optional, Union

from dissect.cstruct import Instance, Structure
from dissect.util import ts
from dissect.util.stream import MappingStream, RunlistStream

from dissect.volume.exceptions import LVM2Error
from dissect.volume.lvm.c_lvm2 import LABEL_SCAN_SECTORS, SECTOR_SIZE, c_lvm
from dissect.volume.lvm.segment import Segment

log = logging.getLogger(__name__)
log.setLevel(os.getenv("DISSECT_LOG_LVM", "CRITICAL"))


class LVM2:
    """Logical Volume Manager"""

    def __init__(self, fh: Union[list[Union[BinaryIO, LVM2Device]], Union[BinaryIO, LVM2Device]]):
        self.fh = [fh] if not isinstance(fh, list) else fh
        if not self.fh:
            raise ValueError("At least one file-like object is required")

        devices = [LVM2Device(fh) if not isinstance(fh, LVM2Device) else fh for fh in self.fh]
        self.devices = {device.id: device for device in devices}

        self.metadata = devices[0].metadata
        self.contents: str = self.metadata["contents"]
        self.version: int = self.metadata["version"]
        self.description: Optional[str] = self.metadata.get("description")
        self.creation_host: Optional[str] = self.metadata.get("creation_host")
        self.creation_time: Optional[datetime] = None
        if creation_time := self.metadata.get("creation_time"):
            self.creation_time: Optional[datetime] = ts.from_unix(creation_time)

        vg = [VolumeGroup.from_dict(key, value) for key, value in self.metadata.items() if isinstance(value, dict)]
        if len(vg) != 1:
            raise LVM2Error(f"Found multiple volume groups, expected only one: {vg}")
        self.volume_group = vg[0]
        self.volume_group.attach(self.devices)

    def __repr__(self) -> str:
        return f"<LVM2 vg={self.vg}>"

    @property
    def vg(self) -> VolumeGroup:
        return self.volume_group


class LVM2Device:
    def __init__(self, fh: BinaryIO):
        self.fh = fh

        for i in range(LABEL_SCAN_SECTORS):
            fh.seek(i * SECTOR_SIZE)
            lbl = c_lvm.label_header(fh)
            if lbl.id == b"LABELONE":
                self.label = lbl
                self.label_offset = i * SECTOR_SIZE
                break
        else:
            raise LVM2Error("Can't find physical volume label header")

        fh.seek(self.label_offset + self.label.offset)
        self.header = c_lvm.pv_header(fh)
        self.id = self.header.pv_uuid.decode()
        self.size = self.header.device_size

        self._data_area_descriptors = _read_descriptors(fh, c_lvm.disk_locn)
        self._metadata_area_descriptors = _read_descriptors(fh, c_lvm.disk_locn)

        self._metadata_areas = []
        for desc in self._metadata_area_descriptors:
            self.fh.seek(desc.offset)
            header = c_lvm.mda_header(self.fh)
            raw_location_descriptors = _read_descriptors(self.fh, c_lvm.raw_locn)
            self._metadata_areas.append((header, raw_location_descriptors))

        self._data_area_offsets = []

        offset = 0
        for area in self._data_area_descriptors:
            if offset != 0:
                self._data_area_offsets.append(offset)
            offset += area.size

    def __repr__(self) -> str:
        return f"<LVMDevice id={self.id}, size={self.size:#x}>"

    @cached_property
    def metadata(self) -> Optional[dict]:
        if not self._metadata_areas:
            return None

        # Only support the first metadata area for now
        # Unsure when a second area is used
        mda_header, raw_locn = self._metadata_areas[0]

        self.fh.seek(mda_header.start + raw_locn[0].offset)
        return parse_metadata(self.fh.read(raw_locn[0].size - 1).decode())

    def read_sectors(self, sector: int, count: int) -> bytes:
        log.debug("LVMDevice::read_sectors(0x%x, 0x%x)", sector, count)

        r = []
        area_idx = bisect_right(self._data_area_offsets, sector * SECTOR_SIZE)

        while count > 0:
            area = self._data_area_descriptors[area_idx]

            area_size = area.size or self.size
            area_remaining_sectors = (area_size // SECTOR_SIZE) - (sector - area.offset // SECTOR_SIZE)
            read_sectors = min(area_remaining_sectors, count)

            self.fh.seek(area.offset + sector * SECTOR_SIZE)
            r.append(self.fh.read(read_sectors * SECTOR_SIZE))
            sector += read_sectors
            count -= read_sectors

            area_idx += 1

        return b"".join(r)

    def open(self) -> BinaryIO:
        runlist = []

        for area in self._data_area_descriptors:
            runlist.append((area.offset // SECTOR_SIZE, (area.size or self.size) // SECTOR_SIZE))

        return RunlistStream(self.fh, runlist, self.size, SECTOR_SIZE)


class VolumeGroup:
    def __init__(self):
        self.name: str = None
        self.id: str = None
        self.seqno: int = None
        self.status: list[str] = []
        self.flags: list[str] = []
        self.extent_size: int = None
        self.max_lv: int = None
        self.max_pv: int = None
        self.physical_volumes: dict[str, PhysicalVolume] = {}
        self.logical_volumes: dict[str, LogicalVolume] = {}

        self.system_id: Optional[str] = None
        self.allocation_policy: Optional[str] = None
        self.profile: Optional[str] = None
        self.metadata_copies: Optional[int] = None
        self.tags: list[str] = None
        self.historical_logical_volumes: Optional[dict[str, HistoricalLogicalVolume]] = None
        self.format: Optional[str] = None
        self.lock_type: Optional[str] = None
        self.lock_args: Optional[str] = None

    def __repr__(self) -> str:
        return f"<VolumeGroup name={self.name} id={self.id}>"

    @property
    def pv(self) -> list[PhysicalVolume]:
        return list(self.physical_volumes.values())

    @property
    def lv(self) -> list[LogicalVolume]:
        return list(self.logical_volumes.values())

    def attach(self, devices: dict[str, LVM2Device]) -> None:
        for pv in self.physical_volumes.values():
            pv.dev = devices.get(pv.id.replace("-", ""))

    @classmethod
    def from_dict(cls, name: str, metadata: dict) -> VolumeGroup:
        vg = cls()
        vg.name = name
        vg.id = metadata["id"]
        vg.seqno = metadata["seqno"]
        vg.status = metadata["status"]
        vg.flags = metadata["flags"]
        vg.extent_size = metadata["extent_size"]
        vg.max_lv = metadata["max_lv"]
        vg.max_pv = metadata["max_pv"]

        pv = metadata["physical_volumes"]
        vg.physical_volumes = {key: PhysicalVolume.from_dict(vg, key, value) for key, value in pv.items()}
        lv = metadata.get("logical_volumes", {})
        vg.logical_volumes = {key: LogicalVolume.from_dict(vg, key, value) for key, value in lv.items()}

        vg.system_id = metadata.get("system_id")
        vg.allocation_policy = metadata.get("allocation_policy")
        vg.profile = metadata.get("profile")
        vg.metadata_copies = metadata.get("metadata_copies")
        vg.tags = metadata.get("tags", [])
        hlv = metadata.get("historical_logical_volumes", {})
        vg.historical_logical_volumes = {
            key: HistoricalLogicalVolume.from_dict(vg, key, value) for key, value in hlv.items()
        }
        vg.format = metadata.get("format")
        vg.lock_type = metadata.get("lock_type")
        vg.lock_args = metadata.get("lock_args")

        return vg


class PhysicalVolume:
    def __init__(self):
        self.volume_group: VolumeGroup = None
        self.name: str = None
        self.id: str = None
        self.status: list[str] = None
        self.pe_start: int = None
        self.pe_count: int = None

        self.dev_size: Optional[int] = None
        self.device: Optional[str] = None
        self.device_id: Optional[str] = None
        self.device_id_type: Optional[str] = None
        self.ba_start: Optional[int] = None
        self.ba_size: Optional[int] = None
        self.tags: list[str] = None

        self.dev: Optional[LVM2Device] = None

    def __repr__(self) -> str:
        return f"<PhysicalVolume name={self.name} id={self.id}>"

    @property
    def vg(self) -> VolumeGroup:
        return self.volume_group

    @classmethod
    def from_dict(cls, vg: VolumeGroup, name: str, metadata: dict) -> PhysicalVolume:
        pv = cls()
        pv.volume_group = vg
        pv.name = name
        pv.id = metadata["id"]
        pv.status = metadata["status"]
        pv.pe_start = metadata["pe_start"]
        pv.pe_count = metadata["pe_count"]

        pv.dev_size = metadata.get("dev_size")
        pv.device = metadata.get("device")
        pv.device_id = metadata.get("device_id")
        pv.device_id_type = metadata.get("device_id_type")
        pv.ba_start = metadata.get("ba_start")
        pv.ba_size = metadata.get("ba_size")
        pv.tags = metadata.get("tags", [])

        return pv


class LogicalVolume:
    def __init__(self):
        self.volume_group: VolumeGroup = None
        self.name: str = None
        self.id: str = None
        self.status: list[str] = None
        self.flags: list[str] = None
        self.segment_count: int = None
        self.segments: list[Segment] = None

        self.creation_time: Optional[datetime] = None
        self.creation_host: Optional[str] = None
        self.lock_args: Optional[str] = None
        self.allocation_policy: Optional[str] = None
        self.profile: Optional[str] = None
        self.read_ahead: Optional[int] = None
        self.tags: list[str] = None

    def __repr__(self) -> str:
        return f"<LogicalVolume name={self.name} id={self.id} segments={len(self.segments)}>"

    @property
    def vg(self) -> VolumeGroup:
        return self.volume_group

    @property
    def is_visible(self) -> bool:
        return "VISIBLE" in self.status

    def open(self) -> BinaryIO:
        stream = MappingStream()
        extent_size = self.volume_group.extent_size * SECTOR_SIZE
        for segment in self.segments:
            offset = segment.start_extent * extent_size
            size = segment.extent_count * extent_size
            stream.add(offset, size, segment.open())
        return stream

    @classmethod
    def from_dict(cls, vg: VolumeGroup, name: str, metadata: dict) -> LogicalVolume:
        lv = cls()
        lv.volume_group = vg
        lv.name = name
        lv.id = metadata["id"]
        lv.status = metadata["status"]
        lv.flags = metadata["flags"]
        lv.segment_count = metadata["segment_count"]
        lv.segments = [Segment.from_dict(lv, value) for value in metadata.values() if isinstance(value, dict)]

        if creation_time := metadata.get("creation_time"):
            lv.creation_time = ts.from_unix(creation_time)

        lv.creation_host = metadata.get("creation_host")
        lv.lock_args = metadata.get("lock_args")
        lv.allocation_policy = metadata.get("allocation_policy")
        lv.profile = metadata.get("profile")
        lv.read_ahead = metadata.get("read_ahead")
        lv.tags = metadata.get("tags", [])

        return lv


class HistoricalLogicalVolume:
    def __init__(self):
        self.volume_group: VolumeGroup = None
        self.id: str = None
        self.name: Optional[str] = None
        self.creation_time: Optional[datetime] = None
        self.removal_time: Optional[datetime] = None
        self.origin: Optional[str] = None
        self.descendants: list[str] = None

    def __repr__(self) -> str:
        return f"<HistoricalLogicalVolume name={self.name} id={self.id} removal_time={self.removal_time}>"

    @property
    def vg(self) -> VolumeGroup:
        return self.volume_group

    @classmethod
    def from_dict(cls, vg: VolumeGroup, name: str, metadata: dict) -> HistoricalLogicalVolume:
        hlv = HistoricalLogicalVolume()
        hlv.volume_group = vg
        hlv.id = metadata["id"]

        hlv.name = metadata.get("name", name)
        if creation_time := metadata.get("creation_time"):
            hlv.creation_time = ts.from_unix(creation_time)
        if removal_time := metadata.get("removal_time"):
            hlv.removal_time = ts.from_unix(removal_time)
        hlv.origin = metadata.get("origin")
        hlv.descendants = metadata.get("descendants", [])

        return hlv


def _read_descriptors(fh: BinaryIO, ctype: Structure) -> list[Instance]:
    descriptors = []
    while True:
        desc = ctype(fh)
        if all(v == 0 for v in desc._values.values()):
            break
        descriptors.append(desc)

    return descriptors


def parse_metadata(string: str) -> dict:
    root = {}
    current = root
    parents = {}

    s = re.sub(r"(#[^\"]+?)$", "", string, flags=re.M)

    it = iter(s.split("\n"))
    for line in it:
        line = line.strip()
        if not line or line[0] == "#":
            continue

        if line[-1] == "{":
            name = line[:-1].strip()

            child = {}
            parent = current
            parents[id(child)] = parent
            parent[name] = child
            current = child
            continue

        if line[-1] == "}":
            current = parents[id(current)]
            continue

        k, v = _parse_key_value(line, it)
        current[k] = v

    root["_raw"] = string
    return root


def _parse_key_value(s: str, it: Iterator[str]) -> tuple[str, Any]:
    key, value = s.strip().split("=", 1)
    key = key.strip()
    value = value.strip()

    if value[0] == "[" and value[-1] != "]":
        lines = [value]
        for line in it:
            lines.append(line)
            if line[-1] == "]":
                break
        value = "".join(lines)

    value = ast.literal_eval(value)

    return key, value
