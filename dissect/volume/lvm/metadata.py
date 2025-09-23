from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime  # noqa: TC003
from functools import cache
from types import UnionType  # novermin
from typing import BinaryIO, get_args, get_origin, get_type_hints

from dissect.util import ts
from dissect.util.stream import MappingStream

from dissect.volume.dm.thin import ThinPool
from dissect.volume.exceptions import LVM2Error
from dissect.volume.lvm.c_lvm2 import SECTOR_SIZE
from dissect.volume.lvm.physical import LVM2Device  # noqa: TC001


@dataclass(init=False)
class MetaBase:
    @classmethod
    def from_dict(cls, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> MetaBase:
        inst = cls()
        inst._from_dict(obj, name=name, parent=parent)
        return inst

    def _from_dict(self, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> None:
        for field_name, field_type in get_type_hints(self.__class__).items():
            if field_name.startswith("_"):
                continue

            type_ = get_origin(field_type)

            if type_ is UnionType and field_type == field_type | None:
                value = obj.get(field_name)
                field_type = get_args(field_type)[0]
                type_ = get_origin(field_type)
            else:
                value = obj[field_name]

            if type_ is dict and issubclass((value_type := get_args(field_type)[1]), MetaBase):
                value = {k: value_type.from_dict(v, name=k, parent=self) for k, v in (value or {}).items()}

            setattr(self, field_name, value)


@dataclass(init=False)
class VolumeGroup(MetaBase):
    id: str
    seqno: int
    status: list[str]
    flags: list[str]
    extent_size: int
    max_lv: int
    max_pv: int
    physical_volumes: dict[str, PhysicalVolume]
    logical_volumes: dict[str, LogicalVolume]

    system_id: str | None
    allocation_policy: str | None
    profile: str | None
    metadata_copies: int | None
    tags: list[str] | None
    historical_logical_volumes: dict[str, HistoricalLogicalVolume] | None
    format: str | None
    lock_type: str | None
    lock_args: str | None

    # Internal fields
    _name: str

    def __repr__(self) -> str:
        return f"<VolumeGroup name={self.name} id={self.id}>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def pv(self) -> list[PhysicalVolume]:
        return list(self.physical_volumes.values())

    @property
    def lv(self) -> list[LogicalVolume]:
        return list(self.logical_volumes.values())

    def attach(self, devices: dict[str, LVM2Device]) -> None:
        for pv in self.physical_volumes.values():
            pv._dev = devices.get(pv.id.replace("-", ""))

    def _from_dict(self, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self._name = name


@dataclass(init=False)
class PhysicalVolume(MetaBase):
    id: str
    status: list[str]
    pe_start: int
    pe_count: int

    dev_size: int | None
    device: str | None
    device_id: str | None
    device_id_type: str | None
    ba_start: int | None
    ba_size: int | None
    tags: list[str] | None

    # Internal fields
    _name: str
    _volume_group: VolumeGroup
    _dev: LVM2Device | None

    def __repr__(self) -> str:
        return f"<PhysicalVolume name={self.name} id={self.id}>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def vg(self) -> VolumeGroup:
        return self._volume_group

    @property
    def volume_group(self) -> VolumeGroup:
        return self._volume_group

    @property
    def dev(self) -> LVM2Device | None:
        return self._dev

    def _from_dict(self, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self._name = name
        self._volume_group = parent


@dataclass(init=False)
class LogicalVolume(MetaBase):
    id: str
    status: list[str]
    flags: list[str]
    segment_count: int

    creation_time: datetime | None
    creation_host: str | None
    lock_args: str | None
    allocation_policy: str | None
    profile: str | None
    read_ahead: int | None
    tags: list[str] | None

    # Internal fields
    _name: str
    _volume_group: VolumeGroup
    _segments: list[Segment]

    def __repr__(self) -> str:
        return f"<LogicalVolume name={self.name} id={self.id} segments={len(self.segments)}>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def vg(self) -> VolumeGroup:
        return self._volume_group

    @property
    def volume_group(self) -> VolumeGroup:
        return self._volume_group

    @property
    def segments(self) -> list[Segment]:
        return self._segments

    @property
    def is_visible(self) -> bool:
        return "VISIBLE" in self.status

    @property
    def type(self) -> str | None:
        return self.segments[0].type if len(self.segments) else None

    def open(self) -> BinaryIO:
        stream = MappingStream()
        extent_size = self.volume_group.extent_size * SECTOR_SIZE
        for segment in self.segments:
            offset = segment.start_extent * extent_size
            size = segment.extent_count * extent_size
            stream.add(offset, size, segment.open())
        return stream

    def _from_dict(self, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self._name = name
        self._volume_group = parent
        self._segments = [Segment.from_dict(v, name=k, parent=self) for k, v in obj.items() if isinstance(v, dict)]


@dataclass(init=False)
class HistoricalLogicalVolume(MetaBase):
    id: str
    name: str | None
    creation_time: datetime | None
    removal_time: datetime | None
    origin: str | None
    descendants: list[str] | None

    # Internal fields
    _volume_group: VolumeGroup

    def __repr__(self) -> str:
        return f"<HistoricalLogicalVolume name={self.name} id={self.id} removal_time={self.removal_time}>"

    @property
    def vg(self) -> VolumeGroup:
        return self._volume_group

    @property
    def volume_group(self) -> VolumeGroup:
        return self._volume_group

    def _from_dict(self, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self.name = self.name or name
        self._volume_group = parent

        if self.creation_time:
            self.creation_time = ts.from_unix(self.creation_time)

        if self.removal_time:
            self.removal_time = ts.from_unix(self.removal_time)


@dataclass(init=False)
class Segment(MetaBase):
    start_extent: int
    extent_count: int
    type: str

    reshape_count: int | None
    data_copies: int | None
    tags: list[str] | None

    # Internal fields
    _name: str
    _logical_volume: LogicalVolume
    _flags: list[str]

    def __repr__(self) -> str:
        fields = (
            f"start_extent={self.start_extent} extent_count={self.extent_count} type={self.type} flags={self.flags}"
        )
        return f"<{self.__class__.__name__} {fields}>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def lv(self) -> LogicalVolume:
        return self._logical_volume

    @property
    def logical_volume(self) -> LogicalVolume:
        return self._logical_volume

    @property
    def flags(self) -> list[str]:
        return self._flags

    def open(self) -> BinaryIO:
        raise NotImplementedError(f"{self.__class__.__name__} is not implemented yet")

    @classmethod
    def from_dict(cls, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> Segment:
        SEGMENT_CLASS_MAP = {
            "linear": StripedSegment,
            "striped": StripedSegment,
            "mirror": MirrorSegment,
            "snapshot": SnapshotSegment,
            "thin": ThinSegment,
            "thin-pool": ThinPoolSegment,
            "cache": CacheSegment,
            "cache-pool": CachePoolSegment,
            "writecache": WriteCacheSegment,
            "integrity": IntegritySegment,
            "error": ErrorSegment,
            "free": FreeSegment,
            "zero": ZeroSegment,
            "vdo": VdoSegment,
            "vdo-pool": VdoPoolSegment,
            "raid": RAIDSegment,
            "raid0": RAIDSegment,
            "raid0_meta": RAIDSegment,
            "raid1": RAIDSegment,
            "raid10": RAIDSegment,
            "raid10_near": RAIDSegment,
            "raid4": RAIDSegment,
            "raid5": RAIDSegment,
            "raid5_n": RAIDSegment,
            "raid5_la": RAIDSegment,
            "raid5_ls": RAIDSegment,
            "raid5_ra": RAIDSegment,
            "raid5_rs": RAIDSegment,
            "raid6": RAIDSegment,
            "raid6_nc": RAIDSegment,
            "raid6_nr": RAIDSegment,
            "raid6_zr": RAIDSegment,
            "raid6_la_6": RAIDSegment,
            "raid6_ls_6": RAIDSegment,
            "raid6_ra_6": RAIDSegment,
            "raid6_rs_6": RAIDSegment,
            "raid6_n_6": RAIDSegment,
        }

        type = obj["type"].split("+", 1)[0]
        return super(Segment, SEGMENT_CLASS_MAP.get(type, Segment)).from_dict(obj, name=name, parent=parent)

    def _from_dict(self, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self.type, *self._flags = self.type.split("+")
        self._name = name
        self._logical_volume = parent


@dataclass(init=False)
class StripedSegment(Segment):
    stripe_count: int
    stripe_size: int | None
    stripes: list[tuple[str, int]]

    def open(self) -> BinaryIO:
        stream = MappingStream()
        pv = self._logical_volume._volume_group.physical_volumes
        extent_size = self._logical_volume._volume_group.extent_size * SECTOR_SIZE

        opened_pv = {}

        offset = 0
        for pv_name, extent_offset in self.stripes:
            if pv_name not in opened_pv:
                if (pv_dev := pv[pv_name].dev) is not None:
                    pv_fh = pv_dev.open()
                else:
                    raise LVM2Error(
                        f"Physical volume not found: {pv_name} (id={pv[pv_name].id}, device={pv[pv_name].device})"
                    )
                opened_pv[pv_name] = pv_fh
            else:
                pv_fh = opened_pv[pv_name]

            stripe_size = (self.stripe_size or self.extent_count) * extent_size
            stream.add(offset, stripe_size, pv_fh, extent_offset * extent_size)
            offset += stripe_size

        return stream

    def _from_dict(self, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self.stripes = [tuple(self.stripes[i : i + 2]) for i in range(0, len(self.stripes), 2)]


@dataclass(init=False)
class MirrorSegment(Segment):
    mirror_count: int

    extents_moved: int | None
    region_size: int | None
    mirror_log: str | None
    mirrors: list[tuple[str, int]] | None

    def open(self) -> BinaryIO:
        # Just open the first mirror we can
        for lv_name, _ in self.mirrors:
            try:
                lv = self._logical_volume._volume_group.logical_volumes[lv_name]
                return lv.open()
            except Exception:  # noqa: PERF203
                pass
        else:
            raise LVM2Error("No mirrors available")

    def _from_dict(self, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)

        if self.mirrors:
            self.mirrors = [tuple(self.mirrors[i : i + 2]) for i in range(0, len(self.mirrors), 2)]


@dataclass(init=False)
class SnapshotSegment(Segment):
    chunk_size: int

    merging_store: str | None
    cow_store: str | None
    origin: str | None


@dataclass(init=False)
class ThinSegment(Segment):
    thin_pool: str
    transaction_id: int

    origin: str | None
    merge: str | None
    device_id: int | None
    external_origin: str | None

    def open(self) -> BinaryIO:
        extent_size = self._logical_volume._volume_group.extent_size * SECTOR_SIZE

        thin_pool_lv = self._logical_volume._volume_group.logical_volumes[self.thin_pool]
        thin_pool_segment: ThinPoolSegment = thin_pool_lv.segments[0]
        thin_pool = thin_pool_segment.open_pool()
        return thin_pool.open(self.device_id, self.extent_count * extent_size)


@dataclass(init=False)
class ThinPoolSegment(Segment):
    metadata: str
    pool: str
    transaction_id: int
    chunk_size: int

    discards: str | None
    zero_new_blocks: int | None
    crop_metadata: int | None

    def __init__(self):
        self.open_pool = cache(self.open_pool)

    def open(self) -> BinaryIO:
        raise RuntimeError("Opening a thin-pool for reading is not possible, use open_pool() instead")

    def open_pool(self) -> ThinPool:
        lvs = self._logical_volume._volume_group.logical_volumes
        return ThinPool(lvs[self.metadata].open(), lvs[self.pool].open())


@dataclass(init=False)
class CacheSegment(Segment):
    cache_pool: str
    origin: str

    cleaner: int | None
    chunk_size: int | None
    cache_mode: str | None
    policy: str | None
    policy_settings: dict[str, int] | None
    metadata_format: int | None
    metadata_start: int | None
    metadata_len: int | None
    data_start: int | None
    data_len: int | None
    metadata_id: str | None
    data_id: str | None


@dataclass(init=False)
class CachePoolSegment(Segment):
    data: str
    metadata: str

    metadata_format: int | None
    chunk_size: int | None
    cache_mode: str | None
    policy: str | None
    policy_settings: dict[str, int] | None


@dataclass(init=False)
class WriteCacheSegment(Segment):
    origin: str
    writecache: str
    writecache_block_size: int

    high_watermark: int | None
    low_watermark: int | None
    writeback_jobs: int | None
    autocommit_blocks: int | None
    autocommit_time: int | None
    fua: int | None
    nofua: int | None
    cleaner: int | None
    max_age: int | None
    metadata_only: int | None
    pause_writeback: int | None
    writecache_setting_key: str | None
    writecache_setting_val: str | None


@dataclass(init=False)
class IntegritySegment(Segment):
    origin: str
    data_sectors: int
    mode: str
    tag_size: int
    block_size: int
    internal_hash: str

    meta_dev: str | None
    recalculate: int | None
    journal_sectors: int | None
    interleave_sectors: int | None
    buffer_sectors: int | None
    journal_watermark: int | None
    commit_time: int | None
    bitmap_flush_interval: int | None
    sectors_per_bit: int | None


@dataclass(init=False)
class ErrorSegment(Segment):
    pass


@dataclass(init=False)
class FreeSegment(Segment):
    pass


@dataclass(init=False)
class ZeroSegment(Segment):
    pass


@dataclass(init=False)
class VdoSegment(Segment):
    vdo_pool: str
    vdo_offset: int


@dataclass(init=False)
class VdoPoolSegment(Segment):
    data: str
    header_size: int
    virtual_extents: int
    use_compression: bool
    use_deduplication: bool
    use_metadata_hints: bool
    minimum_io_size: int
    block_map_cache_size_mb: int
    block_map_era_length: int
    use_sparse_index: int
    index_memory_size_mb: int
    max_discard: int
    slab_size_mb: int
    ack_threads: int
    bio_threads: int
    bio_rotation: int
    cpu_threads: int
    hash_zone_threads: int
    logical_threads: int
    physical_threads: int

    write_policy: str | None


@dataclass(init=False)
class RAIDSegment(Segment):
    device_count: int
    stripe_count: int
    region_size: int
    stripe_size: int
    writebehind: int
    min_recovery_rate: int
    max_recovery_rate: int

    data_copies: int | None
    data_offset: int | None
    raids: list[str] | None
    raid0_lvs: list[str] | None

    def _from_dict(self, obj: dict, name: str | None = None, parent: MetaBase | None = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)

        if self.raids:
            self.raids = [tuple(self.raids[i : i + 2]) for i in range(0, len(self.raids), 2)]
