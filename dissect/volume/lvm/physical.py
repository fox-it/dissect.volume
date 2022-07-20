import logging
import os

from bisect import bisect_right

from dissect import cstruct
from dissect.volume.exceptions import LVM2Error
from dissect.volume.lvm.metadata import Metadata

log = logging.getLogger(__name__)
log.setLevel(os.getenv("DISSECT_LOG_LVM", "CRITICAL"))


SECTOR_SIZE = 512

lvm_def = """
struct label_header {
    char    signature[8];
    uint64  sector_number;
    uint32  checksum;
    uint32  data_offset;
    char    type_indicator[8];
};

struct pv_header {
    char    identifier[32];
    uint64  volume_size;
};

struct data_area_descriptor {
    uint64  offset;
    uint64  size;
};

// Metadata area header
struct mda_header {
    uint32  checksum;
    char    signature[16];
    uint32  version;
    uint64  offset;         // Metadata area offset
    uint64  size;           // Metadata area size
};

struct raw_locn {
    uint64  offset;         // Data area offset
    uint64  size;           // Data area size
    uint32  checksum;
    uint32  flags;
};

#define RAW_LOCN_IGNORED    0x00000001
"""

c_lvm = cstruct.cstruct()
c_lvm.load(lvm_def)


class PhysicalVolume:
    def __init__(self, fh):
        self.fh = fh
        self.label = None
        self.label_offset = None

        self.header = None
        self.data_area_descriptors = []
        self.metadata_area_descriptors = []
        self.metadata_areas = []
        self.metadata = None

        for i in range(4):
            fh.seek(i * SECTOR_SIZE)
            lbl = c_lvm.label_header(fh)
            if lbl.signature == b"LABELONE":
                self.label = lbl
                self.label_offset = i * SECTOR_SIZE
                break
        else:
            raise LVM2Error("Can't find physical volume label header")

        fh.seek(self.label_offset + self.label.data_offset)
        self.header = c_lvm.pv_header(fh)
        self.id = self.header.identifier.decode()

        self.data_area_descriptors = _read_descriptors(fh, c_lvm.data_area_descriptor)
        self.metadata_area_descriptors = _read_descriptors(fh, c_lvm.data_area_descriptor)

        metadata_areas = []
        for desc in self.metadata_area_descriptors:
            fh.seek(desc.offset)
            metadata_areas.append(MetadataArea(fh))
        self.metadata_areas = metadata_areas

        self._data_area_offsets = []

        size_ctr = 0
        for area in self.data_area_descriptors:
            if size_ctr != 0:
                self._data_area_offsets.append(size_ctr)
            size_ctr += area.size

    def __getattr__(self, k):
        if self.metadata:
            return getattr(self.metadata, k)

        return object.__getattribute__(self, k)

    def __repr__(self):
        return f"<PhysicalVolume id={self.header.identifier}, size=0x{self.header.volume_size:x}>"

    @property
    def size(self):
        return self.metadata.dev_size * SECTOR_SIZE

    def has_metadata(self):
        return len(self.metadata_areas) and len(self.metadata_areas[0].raw_location_descriptors)

    def read_metadata(self):
        if self.has_metadata():
            mda_area = self.metadata_areas[0]
            raw_locn = mda_area.raw_location_descriptors[0]
            self.fh.seek(mda_area.header.offset + raw_locn.offset)
            return Metadata.parse(self.fh.read(raw_locn.size - 1).decode())

    def read_sectors(self, sector, count):
        log.debug("PhysicalVolume::read_sectors(0x%x, 0x%x)", sector, count)

        area_idx = bisect_right(self._data_area_offsets, sector * SECTOR_SIZE)
        r = []

        while count > 0:
            area = self.data_area_descriptors[area_idx]

            area_size = self.size if not area.size else area.size
            area_remaining_sectors = (area_size // SECTOR_SIZE) - (sector - area.offset // SECTOR_SIZE)
            area_sectors = min(area_remaining_sectors, count)

            self.fh.seek(area.offset + sector * SECTOR_SIZE)
            r.append(self.fh.read(area_sectors * SECTOR_SIZE))
            sector += area_sectors
            count -= area_sectors

            area_idx += 1

        return b"".join(r)


class MetadataArea:
    def __init__(self, fh):
        self.fh = fh
        self.header = c_lvm.mda_header(fh)
        self.raw_location_descriptors = _read_descriptors(fh, c_lvm.raw_locn)


class Segment:
    def __init__(self, stripes, metadata, vg):
        self.stripes = stripes
        self.metadata = metadata
        self.vg = vg

        self.sector_count = metadata.extent_count * vg.metadata.extent_size
        self.sector_offset = metadata.start_extent * vg.metadata.extent_size
        self.size = self.sector_count * SECTOR_SIZE
        self.offset = self.sector_offset * SECTOR_SIZE

        if metadata.stripe_count > 1:
            self.stripe_size = metadata.stripe_size * vg.metadata.extent_size
        else:
            self.stripe_size = self.sector_count

    def __repr__(self):
        return f"<Segment name={self.metadata.name} offset={self.offset} size={self.size}>"

    def read_sectors(self, sector, count):
        log.debug("Segment::read_sectors(0x%x, 0x%x)", sector, count)
        relsector = sector - self.sector_offset
        absolute_stripe_idx = relsector // self.stripe_size
        stripe_idx = absolute_stripe_idx % self.metadata.stripe_count

        r = []

        while count > 0:
            stripe = self.stripes[stripe_idx]

            stripe_remaining_sectors = self.stripe_size - (relsector - (absolute_stripe_idx * self.stripe_size))
            stripe_sectors = min(stripe_remaining_sectors, count)

            r.append(stripe.read_sectors(relsector, stripe_sectors))
            relsector += stripe_sectors
            count -= stripe_sectors

            stripe_idx = (stripe_idx + 1) % self.metadata.stripe_count

        return b"".join(r)


class Stripe:
    def __init__(self, physical_volume, metadata, vg):
        self.physical_volume = physical_volume
        self.metadata = metadata
        self.vg = vg

        self.sector_offset = metadata.extent_offset * vg.metadata.extent_size
        self.offset = self.sector_offset * SECTOR_SIZE

    def __repr__(self):
        return f"<Stripe pv={self.physical_volume} offset={self.offset}>"

    @property
    def pv(self):
        return self.physical_volume

    def read_sectors(self, sector, count):
        log.debug("Stripe::read_sectors(0x%x, 0x%x)", sector, count)
        return self.physical_volume.read_sectors(self.sector_offset + sector, count)


def _read_descriptors(fh, ctype):
    descriptors = []
    while True:
        desc = ctype(fh)
        if all(v == 0 for v in desc._values.values()):
            break
        descriptors.append(desc)

    return descriptors
