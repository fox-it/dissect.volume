import os
import logging
from bisect import bisect_right

from dissect.util.stream import AlignedStream

from dissect.volume.lvm.physical import PhysicalVolume, Stripe, Segment


log = logging.getLogger(__name__)
log.setLevel(os.getenv("DISSECT_LOG_LVM", "CRITICAL"))

SECTOR_SIZE = 512

STATUS_FLAG_ALLOCATABLE = "ALLOCATABLE"  # pv only
STATUS_FLAG_RESIZEABLE = "RESIZEABLE"  # vg only
STATUS_FLAG_READ = "READ"
STATUS_FLAG_VISIBLE = "VISIBLE"  # lv only
STATUS_FLAG_WRITE = "WRITE"


class LVM2:
    """Logical Volume Manager"""

    def __init__(self, fhs):
        if not isinstance(fhs, list):
            fhs = [fhs]

        if not fhs:
            raise ValueError("No physical volumes given")

        fhs = [PhysicalVolume(fh) if not isinstance(fh, PhysicalVolume) else fh for fh in fhs]

        self.fhs = fhs

        vg = VolumeGroup()
        vg.physical_volumes = fhs
        metadata = vg.physical_volumes[0].read_metadata()
        vg.metadata = metadata.volume_group

        pv_lookup = {}
        for pvmeta in vg.metadata.physical_volumes:
            for pv in vg.physical_volumes:
                if pv.id == pvmeta.id.replace("-", ""):
                    pv.metadata = pvmeta
                    pv_lookup[pvmeta.name] = pv
                    break

        logical_volumes = []
        for lv_meta in vg.metadata.logical_volumes:
            segments = []
            for seg_meta in lv_meta.segments:
                stripes = []
                for stripe_meta in seg_meta.stripes:
                    stripes.append(Stripe(pv_lookup[stripe_meta.physical_volume_name], stripe_meta, vg))

                segments.append(Segment(stripes, seg_meta, vg))

            logical_volumes.append(LogicalVolume(segments, lv_meta, vg))

        vg.logical_volumes = logical_volumes
        self.volume_group = vg
        self.metadata = metadata

    def __repr__(self):
        return f"<LVM2 vg={self.vg}>"

    @property
    def vg(self):
        return self.volume_group


class VolumeGroup:
    def __init__(self, physical_volumes=None, logical_volumes=None, metadata=None):
        self.physical_volumes = physical_volumes or []
        self.logical_volumes = logical_volumes or []
        self.metadata = metadata

    def __repr__(self):
        return f"<VolumeGroup name={self.name} id={self.id}>"

    @property
    def name(self):
        return self.metadata.name

    @property
    def id(self):
        return self.metadata.id

    @property
    def pv(self):
        return self.physical_volumes

    @property
    def lv(self):
        return self.logical_volumes


class LogicalVolume(AlignedStream):
    def __init__(self, segments, metadata, vg):
        self.segments = segments
        self.metadata = metadata
        self.vg = vg

        self._segment_offsets = []

        size = 0
        for s in self.segments:
            if size != 0:
                self._segment_offsets.append(s.sector_offset)
            size += s.size

        super().__init__(size)

    def __repr__(self):
        return f"<LogicalVolume name={self.metadata.name} id={self.metadata.id} segments={len(self.segments)}>"

    def read_sectors(self, sector, count):
        log.debug("LogicalVolume::read_sectors(0x%x, 0x%x)", sector, count)
        r = []

        seg_idx = bisect_right(self._segment_offsets, sector)

        while count > 0:
            seg = self.segments[seg_idx]

            seg_remaining_sectors = seg.sector_count - (sector - seg.sector_offset)
            seg_sectors = min(seg_remaining_sectors, count)

            r.append(seg.read_sectors(sector, seg_sectors))

            sector += seg_sectors
            count -= seg_sectors
            seg_idx += 1

        return b"".join(r)

    def _read(self, offset, length):
        log.debug("LogicalVolume::read(0x%x, 0x%x)", offset, length)
        sector_offset = offset // SECTOR_SIZE
        sector_count = (length + SECTOR_SIZE - 1) // SECTOR_SIZE

        r = self.read_sectors(sector_offset, sector_count)
        return r
