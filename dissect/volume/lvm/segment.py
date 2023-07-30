from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING, BinaryIO, Optional

from dissect.util.stream import MappingStream

from dissect.volume.dm.thin import ThinPool
from dissect.volume.exceptions import LVM2Error
from dissect.volume.lvm.c_lvm2 import SECTOR_SIZE

if TYPE_CHECKING:
    from dissect.volume.lvm.lvm2 import LogicalVolume


class Segment:
    def __init__(self):
        self.logical_volume: LogicalVolume = None
        self.start_extent: int = None
        self.extent_count: int = None
        self.type: str = None
        self.flags: list[str] = None

        self.reshape_count: Optional[int] = None
        self.data_copies: Optional[int] = None
        self.tags: list[str] = None

    def __repr__(self) -> str:
        fields = (
            f"start_extent={self.start_extent} extent_count={self.extent_count} type={self.type} flags={self.flags}"
        )
        return f"<{self.__class__.__name__} {fields}>"

    @property
    def lv(self) -> LogicalVolume:
        return self.logical_volume

    def open(self) -> BinaryIO:
        raise NotImplementedError(f"{self.__class__.__name__} is not implemented yet")

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> Segment:
        seg = cls()
        seg.logical_volume = lv
        seg.start_extent = metadata["start_extent"]
        seg.extent_count = metadata["extent_count"]
        seg.type, *seg.flags = metadata["type"].split("+")

        seg.reshape_count = metadata.get("reshape_count")
        seg.data_copies = metadata.get("data_copies")
        seg.tags = metadata.get("tags", [])

        return seg

    @staticmethod
    def from_dict(lv: LogicalVolume, metadata: dict) -> Segment:
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
        type = metadata["type"].split("+", 1)[0]
        return SEGMENT_CLASS_MAP.get(type, Segment)._from_dict(lv, metadata)


class StripedSegment(Segment):
    def __init__(self):
        super().__init__()
        self.stripe_count: int = None
        self.stripe_size: Optional[int] = None
        self.stripes: list[tuple(str, int)] = None

    def open(self) -> BinaryIO:
        stream = MappingStream()
        pv = self.logical_volume.volume_group.physical_volumes
        extent_size = self.logical_volume.volume_group.extent_size * SECTOR_SIZE

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

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> StripedSegment:
        seg: StripedSegment = super()._from_dict(lv, metadata)
        seg.stripe_count = metadata["stripe_count"]
        seg.stripe_size = metadata.get("stripe_size")

        stripes = metadata["stripes"]
        seg.stripes = [tuple(stripes[i : i + 2]) for i in range(0, len(stripes), 2)]

        return seg


class MirrorSegment(Segment):
    def __init__(self):
        super().__init__()
        self.mirror_count: int = None

        self.extents_moved: Optional[int] = None
        self.region_size: Optional[int] = None
        self.mirror_log: Optional[str] = None
        self.mirrors: list[str] = None

    def open(self) -> BinaryIO:
        # Just open the first mirror we can
        for lv_name, _ in self.mirrors:
            try:
                lv = self.logical_volume.volume_group.logical_volumes[lv_name]
                return lv.open()
            except Exception:
                pass
        else:
            raise LVM2Error("No mirrors available")

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> MirrorSegment:
        seg: MirrorSegment = super()._from_dict(lv, metadata)
        seg.mirror_count = metadata["mirror_count"]

        seg.extents_moved = metadata.get("extents_moved")
        seg.region_size = metadata.get("region_size")
        seg.mirror_log = metadata.get("mirror_log")

        mirrors = metadata.get("mirrors", [])
        seg.mirrors = [tuple(mirrors[i : i + 2]) for i in range(0, len(mirrors), 2)]

        return seg


class SnapshotSegment(Segment):
    def __init__(self):
        super().__init__()
        self.chunk_size: int = None

        self.merging_store: Optional[str] = None
        self.cow_store: Optional[str] = None
        self.origin: Optional[str] = None

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> SnapshotSegment:
        seg: SnapshotSegment = super()._from_dict(lv, metadata)
        seg.chunk_size = metadata["chunk_size"]

        seg.merging_store = metadata.get("merging_store")
        seg.cow_store = metadata.get("cow_store")
        seg.origin = metadata.get("origin")

        return seg


class ThinSegment(Segment):
    def __init__(self):
        super().__init__()
        self.thin_pool: str = None
        self.transaction_id: int = None

        self.origin: Optional[str] = None
        self.merge: Optional[str] = None
        self.device_id: Optional[int] = None
        self.external_origin: Optional[str] = None

    def open(self) -> BinaryIO:
        extent_size = self.logical_volume.volume_group.extent_size * SECTOR_SIZE

        thin_pool_lv = self.logical_volume.volume_group.logical_volumes[self.thin_pool]
        thin_pool_segment: ThinPoolSegment = thin_pool_lv.segments[0]
        thin_pool = thin_pool_segment.open_pool()
        return thin_pool.open(self.device_id, self.extent_count * extent_size)

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> ThinSegment:
        seg: ThinSegment = super()._from_dict(lv, metadata)
        seg.thin_pool = metadata["thin_pool"]
        seg.transaction_id = metadata["transaction_id"]

        seg.origin = metadata.get("origin")
        seg.merge = metadata.get("merge")
        seg.device_id = metadata.get("device_id")
        seg.external_origin = metadata.get("external_origin")

        return seg


class ThinPoolSegment(Segment):
    def __init__(self):
        super().__init__()
        self.metadata: str = None
        self.pool: str = None
        self.transaction_id: int = None
        self.chunk_size: int = None

        self.discards: Optional[str] = None
        self.zero_new_blocks: Optional[int] = None
        self.crop_metadata: Optional[int] = None

        self.open_pool = cache(self.open_pool)

    def open(self) -> BinaryIO:
        raise RuntimeError("Opening a thin-pool for reading is not possible, use open_pool() instead")

    def open_pool(self) -> ThinPool:
        lv = self.logical_volume.volume_group.logical_volumes
        return ThinPool(lv[self.metadata].open(), lv[self.pool].open())

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> ThinPoolSegment:
        seg: ThinPoolSegment = super()._from_dict(lv, metadata)
        seg.metadata = metadata["metadata"]
        seg.pool = metadata["pool"]
        seg.transaction_id = metadata["transaction_id"]
        seg.chunk_size = metadata["chunk_size"]

        seg.discards = metadata.get("discards")
        seg.zero_new_blocks = metadata.get("zero_new_blocks")
        seg.crop_metadata = metadata.get("crop_metadata")

        return seg


class CacheSegment(Segment):
    def __init__(self):
        super().__init__()
        self.cache_pool: str = None
        self.origin: str = None

        self.cleaner: Optional[int] = None
        self.chunk_size: Optional[int] = None
        self.cache_mode: Optional[str] = None
        self.policy: Optional[str] = None
        self.policy_settings: Optional[dict[str, int]] = None
        self.metadata_format: Optional[int] = None
        self.metadata_start: Optional[int] = None
        self.metadata_len: Optional[int] = None
        self.data_start: Optional[int] = None
        self.data_len: Optional[int] = None
        self.metadata_id: Optional[str] = None
        self.data_id: Optional[str] = None

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> CacheSegment:
        seg: CacheSegment = super()._from_dict(lv, metadata)
        seg.cache_pool = metadata["cache_pool"]
        seg.origin = metadata["origin"]

        seg.cleaner = metadata.get("cleaner")
        seg.chunk_size = metadata.get("chunk_size")
        seg.cache_mode = metadata.get("cache_mode")
        seg.policy = metadata.get("policy")
        seg.policy_settings = metadata.get("policy_settings")
        seg.metadata_format = metadata.get("metadata_format")
        seg.metadata_start = metadata.get("metadata_start")
        seg.metadata_len = metadata.get("metadata_len")
        seg.data_start = metadata.get("data_start")
        seg.data_len = metadata.get("data_len")
        seg.metadata_id = metadata.get("metadata_id")
        seg.data_id = metadata.get("data_id")

        return seg


class CachePoolSegment(Segment):
    def __init__(self):
        super().__init__()
        self.data: str = None
        self.metadata: str = None

        self.metadata_format: Optional[int] = None
        self.chunk_size: Optional[int] = None
        self.cache_mode: Optional[str] = None
        self.policy: Optional[str] = None
        self.policy_settings: Optional[dict[str, int]] = None

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> CachePoolSegment:
        seg: CachePoolSegment = super()._from_dict(lv, metadata)
        seg.data = metadata["data"]
        seg.metadata = metadata["metadata"]

        seg.metadata_format = metadata.get("metadata_format")
        seg.chunk_size = metadata.get("chunk_size")
        seg.cache_mode = metadata.get("cache_mode")
        seg.policy = metadata.get("policy")
        seg.policy_settings = metadata.get("policy_settings")

        return seg


class WriteCacheSegment(Segment):
    def __init__(self):
        super().__init__()
        self.origin: str = None
        self.writecache: str = None
        self.writecache_block_size: int = None

        self.high_watermark: Optional[int] = None
        self.low_watermark: Optional[int] = None
        self.writeback_jobs: Optional[int] = None
        self.autocommit_blocks: Optional[int] = None
        self.autocommit_time: Optional[int] = None
        self.fua: Optional[int] = None
        self.nofua: Optional[int] = None
        self.cleaner: Optional[int] = None
        self.max_age: Optional[int] = None
        self.metadata_only: Optional[int] = None
        self.pause_writeback: Optional[int] = None
        self.writecache_setting_key: Optional[str] = None
        self.writecache_setting_val: Optional[str] = None

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> WriteCacheSegment:
        seg: WriteCacheSegment = super()._from_dict(lv, metadata)
        seg.origin = metadata.get["origin"]
        seg.writecache = metadata.get["writecache"]
        seg.writecache_block_size = metadata["writecache_block_size"]

        seg.high_watermark = metadata.get("high_watermark")
        seg.low_watermark = metadata.get("low_watermark")
        seg.writeback_jobs = metadata.get("writeback_jobs")
        seg.autocommit_blocks = metadata.get("autocommit_blocks")
        seg.autocommit_time = metadata.get("autocommit_time")
        seg.fua = metadata.get("fua")
        seg.nofua = metadata.get("nofua")
        seg.cleaner = metadata.get("cleaner")
        seg.max_age = metadata.get("max_age")
        seg.metadata_only = metadata.get("metadata_only")
        seg.pause_writeback = metadata.get("pause_writeback")
        seg.writecache_setting_key = metadata.get("writecache_setting_key")
        seg.writecache_setting_val = metadata.get("writecache_setting_val")

        return seg


class IntegritySegment(Segment):
    def __init__(self):
        super().__init__()
        self.origin: str = None
        self.data_sectors: int = None
        self.mode: str = None
        self.tag_size: int = None
        self.block_size: int = None
        self.internal_hash: str = None

        self.meta_dev: Optional[str] = None
        self.recalculate: Optional[int] = None
        self.journal_sectors: Optional[int] = None
        self.interleave_sectors: Optional[int] = None
        self.buffer_sectors: Optional[int] = None
        self.journal_watermark: Optional[int] = None
        self.commit_time: Optional[int] = None
        self.bitmap_flush_interval: Optional[int] = None
        self.sectors_per_bit: Optional[int] = None

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> IntegritySegment:
        seg: IntegritySegment = super()._from_dict(lv, metadata)
        seg.origin = metadata["origin"]
        seg.data_sectors = metadata["data_sectors"]
        seg.mode = metadata["mode"]
        seg.tag_size = metadata["tag_size"]
        seg.block_size = metadata["block_size"]
        seg.internal_hash = metadata["internal_hash"]

        seg.meta_dev = metadata.get("meta_dev")
        seg.recalculate = metadata.get("recalculate")
        seg.journal_sectors = metadata.get("journal_sectors")
        seg.interleave_sectors = metadata.get("interleave_sectors")
        seg.buffer_sectors = metadata.get("buffer_sectors")
        seg.journal_watermark = metadata.get("journal_watermark")
        seg.commit_time = metadata.get("commit_time")
        seg.bitmap_flush_interval = metadata.get("bitmap_flush_interval")
        seg.sectors_per_bit = metadata.get("sectors_per_bit")

        return seg


class ErrorSegment(Segment):
    pass


class FreeSegment(Segment):
    pass


class ZeroSegment(Segment):
    pass


class VdoSegment(Segment):
    def __init__(self):
        super().__init__()
        self.vdo_pool: str = None
        self.vdo_offset: int = None

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> VdoSegment:
        seg: VdoSegment = super()._from_dict(lv, metadata)
        seg.vdo_pool = metadata["vdo_pool"]
        seg.vdo_offset = metadata["vdo_offset"]

        return seg


class VdoPoolSegment(Segment):
    def __init__(self):
        super().__init__()
        self.data: str = None
        self.header_size: int = None
        self.virtual_extents: int = None
        self.use_compression: bool = None
        self.use_deduplication: bool = None
        self.use_metadata_hints: bool = None
        self.minimum_io_size: int = None
        self.block_map_cache_size_mb: int = None
        self.block_map_era_length: int = None
        self.use_sparse_index: int = None
        self.index_memory_size_mb: int = None
        self.max_discard: int = None
        self.slab_size_mb: int = None
        self.ack_threads: int = None
        self.bio_threads: int = None
        self.bio_rotation: int = None
        self.cpu_threads: int = None
        self.hash_zone_threads: int = None
        self.logical_threads: int = None
        self.physical_threads: int = None
        self.write_policy: Optional[str] = None

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> VdoPoolSegment:
        seg: VdoPoolSegment = super()._from_dict(lv, metadata)
        seg.data = metadata["data"]
        seg.header_size = metadata["header_size"]
        seg.virtual_extents = metadata["virtual_extents"]
        seg.use_compression = metadata["use_compression"]
        seg.use_deduplication = metadata["use_deduplication"]
        seg.use_metadata_hints = metadata["use_metadata_hints"]
        seg.minimum_io_size = metadata["minimum_io_size"]
        seg.block_map_cache_size_mb = metadata["block_map_cache_size_mb"]
        seg.block_map_era_length = metadata["block_map_era_length"]
        seg.use_sparse_index = metadata["use_sparse_index"]
        seg.index_memory_size_mb = metadata["index_memory_size_mb"]
        seg.max_discard = metadata["max_discard"]
        seg.slab_size_mb = metadata["slab_size_mb"]
        seg.ack_threads = metadata["ack_threads"]
        seg.bio_threads = metadata["bio_threads"]
        seg.bio_rotation = metadata["bio_rotation"]
        seg.cpu_threads = metadata["cpu_threads"]
        seg.hash_zone_threads = metadata["hash_zone_threads"]
        seg.logical_threads = metadata["logical_threads"]
        seg.physical_threads = metadata["physical_threads"]

        seg.write_policy = metadata.get("write_policy")

        return seg


class RAIDSegment(Segment):
    def __init__(self):
        super().__init__()
        self.device_count: int = None
        self.stripe_count: int = None
        self.region_size: int = None
        self.stripe_size: int = None
        self.writebehind: int = None
        self.min_recovery_rate: int = None
        self.max_recovery_rate: int = None

        self.data_copies: Optional[int] = None
        self.data_offset: Optional[int] = None
        self.raids: list[str] = None
        self.raid0_lvs: list[str] = None

    @classmethod
    def _from_dict(cls, lv: LogicalVolume, metadata: dict) -> RAIDSegment:
        seg: RAIDSegment = super()._from_dict(lv, metadata)
        seg.device_count = metadata["device_count"]
        seg.stripe_count = metadata["stripe_count"]
        seg.region_size = metadata["region_size"]
        seg.stripe_size = metadata["stripe_size"]
        seg.writebehind = metadata["writebehind"]
        seg.min_recovery_rate = metadata["min_recovery_rate"]
        seg.max_recovery_rate = metadata["max_recovery_rate"]

        seg.data_copies = metadata.get("data_copies")
        seg.data_offset = metadata.get("data_offset")

        raids = metadata.get("raids", [])
        seg.raids = [tuple(raids[i : i + 2]) for i in range(0, len(raids), 2)]
        seg.raid0_lvs = metadata.get("raid0_lvs", [])

        return seg
