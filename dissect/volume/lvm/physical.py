from __future__ import annotations

import ast
import re
from bisect import bisect_right
from functools import cached_property
from typing import TYPE_CHECKING, Any, BinaryIO

from dissect.util.stream import RunlistStream

from dissect.volume.exceptions import LVM2Error
from dissect.volume.lvm.c_lvm2 import LABEL_SCAN_SECTORS, SECTOR_SIZE, c_lvm

if TYPE_CHECKING:
    from collections.abc import Iterator


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
    def metadata(self) -> dict | None:
        if not self._metadata_areas:
            return None

        # Only support the first metadata area for now
        # Unsure when a second area is used
        mda_header, raw_locn = self._metadata_areas[0]

        self.fh.seek(mda_header.start + raw_locn[0].offset)
        return parse_metadata(self.fh.read(raw_locn[0].size - 1).decode())

    def read_sectors(self, sector: int, count: int) -> bytes:
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
        runlist = [
            (area.offset // SECTOR_SIZE, (area.size or self.size) // SECTOR_SIZE)
            for area in self._data_area_descriptors
        ]

        return RunlistStream(self.fh, runlist, self.size, SECTOR_SIZE)


def _read_descriptors(
    fh: BinaryIO, ctype: type[c_lvm.disk_locn | c_lvm.raw_locn]
) -> list[c_lvm.disk_locn | c_lvm.raw_locn]:
    descriptors = []
    while True:
        desc = ctype(fh)
        if all(v == 0 for v in desc.__values__.values()):
            break
        descriptors.append(desc)

    return descriptors


def parse_metadata(string: str) -> dict:
    root = {}
    current = root
    parents = {}

    s = re.sub(r"(#[^\"]+?)$", "", string, flags=re.MULTILINE)

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
