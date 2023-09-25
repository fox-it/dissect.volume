from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import cache
from typing import BinaryIO, Optional, Self, Union, get_args, get_origin, get_type_hints

from dissect.util import ts
from dissect.util.stream import MappingStream

from dissect.volume.dm.thin import ThinPool
from dissect.volume.exceptions import LVM2Error
from dissect.volume.lvm.c_lvm2 import SECTOR_SIZE
from dissect.volume.lvm.physical import LVM2Device


@dataclass(init=False)
class MetaBase:
    @classmethod
    def from_dict(cls, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> Self:
        inst = cls()
        inst._from_dict(obj, name=name, parent=parent)
        return inst

    def _from_dict(self, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> None:
        for field_name, field_type in get_type_hints(self.__class__).items():
            if field_name.startswith("_"):
                continue

            type_ = get_origin(field_type)

            if type_ is Union and field_type == Optional[field_type]:
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

    system_id: Optional[str]
    allocation_policy: Optional[str]
    profile: Optional[str]
    metadata_copies: Optional[int]
    tags: Optional[list[str]]
    historical_logical_volumes: Optional[dict[str, HistoricalLogicalVolume]]
    format: Optional[str]
    lock_type: Optional[str]
    lock_args: Optional[str]

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

    def _from_dict(self, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self._name = name


@dataclass(init=False)
class PhysicalVolume(MetaBase):
    id: str
    status: list[str]
    pe_start: int
    pe_count: int

    dev_size: Optional[int]
    device: Optional[str]
    device_id: Optional[str]
    device_id_type: Optional[str]
    ba_start: Optional[int]
    ba_size: Optional[int]
    tags: Optional[list[str]]

    # Internal fields
    _name: str
    _volume_group: VolumeGroup
    _dev: Optional[LVM2Device]

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
    def dev(self) -> Optional[LVM2Device]:
        return self._dev

    def _from_dict(self, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self._name = name
        self._volume_group = parent


class LogicalVolume(MetaBase):
    id: str
    status: list[str]
    flags: list[str]
    segment_count: int

    creation_time: Optional[datetime]
    creation_host: Optional[str]
    lock_args: Optional[str]
    allocation_policy: Optional[str]
    profile: Optional[str]
    read_ahead: Optional[int]
    tags: Optional[list[str]]

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
    def type(self) -> Optional[str]:
        return self.segments[0].type if len(self.segments) else None

    def open(self) -> BinaryIO:
        stream = MappingStream()
        extent_size = self.volume_group.extent_size * SECTOR_SIZE
        for segment in self.segments:
            offset = segment.start_extent * extent_size
            size = segment.extent_count * extent_size
            stream.add(offset, size, segment.open())
        return stream

    def _from_dict(self, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self._name = name
        self._volume_group = parent
        self._segments = [Segment.from_dict(v, name=k, parent=self) for k, v in obj.items() if isinstance(v, dict)]


@dataclass(init=False)
class HistoricalLogicalVolume(MetaBase):
    id: str
    name: Optional[str]
    creation_time: Optional[datetime]
    removal_time: Optional[datetime]
    origin: Optional[str]
    descendants: Optional[list[str]]

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

    def _from_dict(self, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> None:
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

    reshape_count: Optional[int]
    data_copies: Optional[int]
    tags: Optional[list[str]]

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
    def from_dict(cls, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> Self:
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

    def _from_dict(self, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self.type, *self._flags = self.type.split("+")
        self._name = name
        self._logical_volume = parent


@dataclass(init=False)
class StripedSegment(Segment):
    stripe_count: int
    stripe_size: Optional[int]
    stripes: list[tuple[str, int]]

    def open(self) -> BinaryIO:
        stream = MappingStream()
        pv = self._logical_volume._volume_group.physical_volumes
        extent_size = self._logical_volume._volume_group.extent_size * SECTOR_SIZE

        opened_pv = {}

        offset = 0
        for pv_name, extent_offset in self.stripes:
            if pv_name not in opened_pv:
                pv_fh = pv[pv_name].dev.open()
                opened_pv[pv_name] = pv_fh
            else:
                pv_fh = opened_pv[pv_name]

            stripe_size = (self.stripe_size or self.extent_count) * extent_size
            stream.add(offset, stripe_size, pv_fh, extent_offset * extent_size)
            offset += stripe_size

        return stream

    def _from_dict(self, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)
        self.stripes = [tuple(self.stripes[i : i + 2]) for i in range(0, len(self.stripes), 2)]


@dataclass(init=False)
class MirrorSegment(Segment):
    mirror_count: int

    extents_moved: Optional[int]
    region_size: Optional[int]
    mirror_log: Optional[str]
    mirrors: Optional[list[tuple[str, int]]]

    def open(self) -> BinaryIO:
        # Just open the first mirror we can
        for lv_name, _ in self.mirrors:
            try:
                lv = self._logical_volume._volume_group.logical_volumes[lv_name]
                return lv.open()
            except Exception:
                pass
        else:
            raise LVM2Error("No mirrors available")

    def _from_dict(self, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)

        if self.mirrors:
            self.mirrors = [tuple(self.mirrors[i : i + 2]) for i in range(0, len(self.mirrors), 2)]


@dataclass(init=False)
class SnapshotSegment(Segment):
    chunk_size: int

    merging_store: Optional[str]
    cow_store: Optional[str]
    origin: Optional[str]


@dataclass(init=False)
class ThinSegment(Segment):
    thin_pool: str
    transaction_id: int

    origin: Optional[str]
    merge: Optional[str]
    device_id: Optional[int]
    external_origin: Optional[str]

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

    discards: Optional[str]
    zero_new_blocks: Optional[int]
    crop_metadata: Optional[int]

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

    cleaner: Optional[int]
    chunk_size: Optional[int]
    cache_mode: Optional[str]
    policy: Optional[str]
    policy_settings: Optional[dict[str, int]]
    metadata_format: Optional[int]
    metadata_start: Optional[int]
    metadata_len: Optional[int]
    data_start: Optional[int]
    data_len: Optional[int]
    metadata_id: Optional[str]
    data_id: Optional[str]


@dataclass(init=False)
class CachePoolSegment(Segment):
    data: str
    metadata: str

    metadata_format: Optional[int]
    chunk_size: Optional[int]
    cache_mode: Optional[str]
    policy: Optional[str]
    policy_settings: Optional[dict[str, int]]


@dataclass(init=False)
class WriteCacheSegment(Segment):
    origin: str
    writecache: str
    writecache_block_size: int

    high_watermark: Optional[int]
    low_watermark: Optional[int]
    writeback_jobs: Optional[int]
    autocommit_blocks: Optional[int]
    autocommit_time: Optional[int]
    fua: Optional[int]
    nofua: Optional[int]
    cleaner: Optional[int]
    max_age: Optional[int]
    metadata_only: Optional[int]
    pause_writeback: Optional[int]
    writecache_setting_key: Optional[str]
    writecache_setting_val: Optional[str]


@dataclass(init=False)
class IntegritySegment(Segment):
    origin: str
    data_sectors: int
    mode: str
    tag_size: int
    block_size: int
    internal_hash: str

    meta_dev: Optional[str]
    recalculate: Optional[int]
    journal_sectors: Optional[int]
    interleave_sectors: Optional[int]
    buffer_sectors: Optional[int]
    journal_watermark: Optional[int]
    commit_time: Optional[int]
    bitmap_flush_interval: Optional[int]
    sectors_per_bit: Optional[int]


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

    write_policy: Optional[str]


@dataclass(init=False)
class RAIDSegment(Segment):
    device_count: int
    stripe_count: int
    region_size: int
    stripe_size: int
    writebehind: int
    min_recovery_rate: int
    max_recovery_rate: int

    data_copies: Optional[int]
    data_offset: Optional[int]
    raids: Optional[list[str]]
    raid0_lvs: Optional[list[str]]

    def _from_dict(self, obj: dict, name: Optional[str] = None, parent: Optional[MetaBase] = None) -> None:
        super()._from_dict(obj, name=name, parent=parent)

        if self.raids:
            self.raids = [tuple(self.raids[i : i + 2]) for i in range(0, len(self.raids), 2)]
