# This file is still a WIP
from __future__ import annotations

import logging
import os
import uuid
from collections import defaultdict
from functools import cached_property
from typing import BinaryIO

from dissect.cstruct import Structure, cstruct
from dissect.util.stream import AlignedStream

from dissect.volume.exceptions import Error

log = logging.getLogger(__name__)
log.setLevel(os.getenv("DISSECT_LOG_VSS", "CRITICAL"))

vss_def = """
flag _VSS_VOLUME_SNAPSHOT_ATTRIBUTES : uint32 {
    VSS_VOLSNAP_ATTR_PERSISTENT             = 0x00000001,
    VSS_VOLSNAP_ATTR_NO_AUTORECOVERY        = 0x00000002,
    VSS_VOLSNAP_ATTR_CLIENT_ACCESSIBLE      = 0x00000004,
    VSS_VOLSNAP_ATTR_NO_AUTO_RELEASE        = 0x00000008,
    VSS_VOLSNAP_ATTR_NO_WRITERS             = 0x00000010,
    VSS_VOLSNAP_ATTR_TRANSPORTABLE          = 0x00000020,
    VSS_VOLSNAP_ATTR_NOT_SURFACED           = 0x00000040,
    VSS_VOLSNAP_ATTR_NOT_TRANSACTED         = 0x00000080,
    VSS_VOLSNAP_ATTR_HARDWARE_ASSISTED      = 0x00010000,
    VSS_VOLSNAP_ATTR_DIFFERENTIAL           = 0x00020000,
    VSS_VOLSNAP_ATTR_PLEX                   = 0x00040000,
    VSS_VOLSNAP_ATTR_IMPORTED               = 0x00080000,
    VSS_VOLSNAP_ATTR_EXPOSED_LOCALLY        = 0x00100000,
    VSS_VOLSNAP_ATTR_EXPOSED_REMOTELY       = 0x00200000,
    VSS_VOLSNAP_ATTR_AUTORECOVER            = 0x00400000,
    VSS_VOLSNAP_ATTR_ROLLBACK_RECOVERY      = 0x00800000,
    VSS_VOLSNAP_ATTR_DELAYED_POSTSNAPSHOT   = 0x01000000,
    VSS_VOLSNAP_ATTR_TXF_RECOVERY           = 0x02000000
};

enum RECORD_TYPE : uint32 {
    VOLUME_HEADER       = 0x1,
    CATALOG             = 0x2,
    STORE_INDEX         = 0x3,
    STORE_HEADER        = 0x4,
    STORE_BLOCK_RANGE   = 0x5,
    STORE_BITMAP        = 0x6
};

flag BLOCK_FLAG : uint32 {
    IS_FORWARDER    = 0x1,
    IS_OVERLAY      = 0x2,
    NOT_USED        = 0x4
};

struct volume_header {
    char    identifier[16];
    uint32  version;
    uint32  record_type;
    uint64  current_offset;
    uint64  unk0;
    uint64  unk1;
    uint64  catalog_offset;
    uint64  maximum_size;
    char    volume_identifier[16];
    char    store_volume_identifier[16];
    uint32  unk2;
    char    unk3[412];
};

struct catalog_header {
    char    identifier[16];
    uint32  version;
    uint32  record_type;
    uint64  relative_offset;
    uint64  offset;
    uint64  next_offset;
    char    unk0[80];
};

struct catalog_entry_1 {
    uint64  entry_type;
    char    unk0[120];
};

struct catalog_entry_2 {
    uint64  entry_type;
    uint64  volume_size;
    char    store_identifier[16];
    uint64  unk0;
    uint64  unk1;
    uint64  creation_time;
    char    unk2[72];
};

struct catalog_entry_3 {
    uint64  entry_type;
    uint64  store_block_list_offset;
    char    store_identifier[16];
    uint64  store_header_offset;
    uint64  store_range_list_offset;
    uint64  store_bitmap_offset;
    uint64  metadata_reference;
    uint64  allocated_size;
    uint64  store_previous_bitmap_offset;
    uint64  unk0;
    char    unk1[40];
};

struct store_header {
    char        identifier[16];
    uint32      version;
    RECORD_TYPE record_type;
    uint64      relative_offset;
    uint64      offset;
    uint64      next_offset;
    uint64      size;
    char        unk0[72];
};

struct store_information {
    char    unk_identifier[16];
    char    copy_identifier[16];
    char    copy_set_identifier[16];
    uint32  type;
    uint32  provider;
    _VSS_VOLUME_SNAPSHOT_ATTRIBUTES attributes;
    uint32  unk0;
    uint16  operating_machine_len;
    wchar   operating_machine[operating_machine_len / 2];
    uint16  service_machine_len;
    wchar   service_machine[service_machine_len / 2];
};

struct block_descriptor {
    uint64  original_offset;
    uint64  relative_offset;
    uint64  store_offset;
    BLOCK_FLAG  flags;
    uint32  allocation_bitmap;
};

struct range_descriptor {
    uint64  store_offset;
    uint64  relative_offset;
    uint64  size;
};
"""

c_vss = cstruct().load(vss_def)

RECORD_TYPE = c_vss.RECORD_TYPE
BLOCK_FLAG = c_vss.BLOCK_FLAG

VSS_IDENTIFIER = b"\x6b\x87\x08\x38\x76\xc1\x48\x4e\xb7\xae\x04\x04\x6e\x6c\xc7\x52"
VOLUME_HEADER_OFFSET = 0x1E00
BLOCK_SIZE = 0x4000

CATALOG_BLOCK_SIZE = 0x4000
CATALOG_ENTRY_SIZE = 128

STORE_BLOCK_SIZE = 0x4000

STORE_BLOCKLIST_ENTRY_SIZE = 32
STORE_RANGELIST_ENTRY_SIZE = 24


class VSS:
    def __init__(self, fh: BinaryIO):
        self.fh = fh

        fh.seek(VOLUME_HEADER_OFFSET)
        self.header = c_vss.volume_header(fh)
        if self.header.identifier != VSS_IDENTIFIER:
            raise Error(
                f"Not a valid VSS identifier (got {self.header.identifier.hex()}, expected {VSS_IDENTIFIER.hex()}"
            )

        if self.header.catalog_offset == 0:
            raise Error("Catalog offset is 0")

        self.catalog = Catalog(self, self.header.catalog_offset)

    def __repr__(self) -> str:
        return (
            f"<VSS volume_identifier={self.volume_identifier} store_volume_identifier={self.store_volume_identifier}>"
        )

    @property
    def volume_identifier(self) -> uuid.UUID:
        return uuid.UUID(bytes_le=self.header.volume_identifier)

    @property
    def store_volume_identifier(self) -> uuid.UUID:
        return uuid.UUID(bytes_le=self.header.store_volume_identifier)


class Catalog:
    def __init__(self, vss: VSS, offset: int):
        self.vss = vss
        self.fh = vss.fh
        self.header, data = read_block(vss.fh, offset, c_vss.catalog_header)
        buf = memoryview(data)

        store_map = defaultdict(list)

        self.entries = []
        for entry_offset in range(0, len(data), CATALOG_ENTRY_SIZE):
            entry_buf = buf[entry_offset : entry_offset + CATALOG_ENTRY_SIZE]
            entry_type = c_vss.uint64(entry_buf[:8])

            entry = None
            if entry_type == 1:
                entry = c_vss.catalog_entry_1(entry_buf)
            elif entry_type == 2:
                entry = c_vss.catalog_entry_2(entry_buf)
            elif entry_type == 3:
                entry = c_vss.catalog_entry_3(entry_buf)
            elif entry_type == 0:
                break
            else:
                raise Error("Invalid catalog entry")

            if entry_type in (2, 3):
                store_map[entry.store_identifier].append(entry)

            self.entries.append(entry)

        self.stores = []
        for store_descriptors in store_map.values():
            self.stores.append(Store(self, store_descriptors))

        self.stores = sorted(self.stores, key=lambda store: store.creation_time)
        prev_store = None
        for i, store in enumerate(self.stores):
            store.index = i
            store.previous_store = prev_store
            if prev_store:
                prev_store.next_store = store
            prev_store = store

    def __repr__(self) -> str:
        return f"<Catalog stores={len(self.stores)}>"


DEBUG = False


class Store:
    def __init__(self, catalog: Catalog, descriptors: list):
        self.catalog = catalog
        self.fh = catalog.fh
        self.descriptors = descriptors
        self.previous_store = None
        self.next_store = None
        self.index = None

        if len(descriptors) != 2:
            raise NotImplementedError("Only got one descriptor")

        self.has_store_data = False
        if len(descriptors) == 2:
            self.has_store_data = True

        self.desc2, self.desc3 = descriptors
        self.volume_size = self.desc2.volume_size
        self.size = self.volume_size
        self.creation_time = self.desc2.creation_time

        # Only if store data
        self.fh.seek(self.desc3.store_header_offset)
        self.header = c_vss.store_header(self.fh)

        if self.header.identifier != VSS_IDENTIFIER:
            raise Error(
                f"Not a valid VSS identifier (got {self.header.identifier.hex()}, expected {VSS_IDENTIFIER.hex()}"
            )

        self.information = c_vss.store_information(self.fh)

        self.copy_identifier = uuid.UUID(bytes_le=self.information.copy_identifier)
        self.copy_set_identifier = uuid.UUID(bytes_le=self.information.copy_set_identifier)

        self._block_list = None
        self._range_list = None
        self._bitmap = None
        self._previous_bitmap = None

    def open(self) -> StoreStream:
        return StoreStream(self)

    @cached_property
    def block_list(self) -> BlockList:
        return BlockList(self, self.desc3.store_block_list_offset)

    @cached_property
    def range_list(self) -> RangeList:
        return RangeList(self, self.desc3.store_range_list_offset)

    @cached_property
    def bitmap(self) -> StoreBitmap:
        return StoreBitmap(self, self.desc3.store_bitmap_offset)

    @cached_property
    def previous_bitmap(self) -> StoreBitmap:
        return StoreBitmap(self, self.desc3.store_previous_bitmap_offset)

    def read_block(self, block: int, active_store: Store | None = None) -> bytes:
        active_store = active_store or self

        buf = None
        descriptor = self.block_list.map.map.get(block)
        if DEBUG:
            print(block, descriptor)
        # if descriptor:
        #     import ipdb; ipdb.set_trace()

        # if descriptor and descriptor.is_overlay:
        #     import ipdb; ipdb.set_trace()

        if descriptor:
            if descriptor.is_forwarder:
                if self.next_store:
                    print("DEBUG THIS")
                    buf = self.next_store.read_block(descriptor.relative_offset // 0x4000, self)
                else:
                    self.fh.seek(descriptor.relative_offset)
                    buf = self.fh.read(BLOCK_SIZE)
            elif not descriptor.is_overlay:
                # print("reading from store")
                self.fh.seek(descriptor.store_offset)
                buf = self.fh.read(BLOCK_SIZE)

        if not descriptor or descriptor.is_overlay:
            if self.next_store:
                # print("reading from next store")
                buf = self.next_store.read_block(block, self)
            else:
                reverse_descriptor = self.block_list.map.reverse.get(block)
                if reverse_descriptor:
                    # print("found reverse descriptor, reading from volume")
                    self.fh.seek(block * BLOCK_SIZE)
                    buf = self.fh.read(BLOCK_SIZE)
                elif self.bitmap.in_use(block) and (not self.previous_bitmap or self.previous_bitmap.in_use(block)):
                    # print("sparse block?")
                    buf = b"\x00" * BLOCK_SIZE
                else:
                    # print("nothing matched, reading from volume")
                    self.fh.seek(block * BLOCK_SIZE)
                    buf = self.fh.read(BLOCK_SIZE)

        if not buf:
            print("booboo")
            print("Descriptor", descriptor)
            raise ValueError("Error reading block")

        if descriptor and active_store is self and (descriptor.is_overlay or descriptor.overlay):
            # print("overlaying data")
            buf = bytearray(buf)
            overlay = descriptor if descriptor.is_overlay else descriptor.overlay
            overlay_offset = overlay.store_offset
            bitmap = overlay.bitmap

            for i in range(32):
                if (bitmap >> i) & 0x00000001:
                    self.fh.seek(overlay_offset + (i * 512))
                    buf[i * 512 : (i + 1) * 512] = self.fh.read(512)

        return bytes(buf)

        #     if descriptor.is_forwarder:
        #         block_data_offset = descriptor.relative_offset
        #     else:
        #         block_data_offset = descriptor.store_offset

        #     if overlay:
        #         if active_store is not self and descriptor is overlay:
        #             print('weird shit')
        #             descriptor = None
        #         else:
        #             overlay_offset = overlay.original_offset
        #             overlay_bitmap = overlay.bitmap
        #             pass

        # if descriptor:
        #     if descriptor.is_forwarder:
        #         block_data_offset = descriptor.relative_offset
        #     else:
        #         block_data_offset = descriptor.store_offset

        #     if descriptor.is_overlay:
        #         overlay_block_descriptor = descriptor
        #     else:
        #         overlay_block_descriptor = descriptor.overlay

        #     if overlay_block_descriptor:
        #         if self.index != active_store.index and descriptor is overlay_block_descriptor:
        #             descriptor = None
        #         else:
        #             overlay_block_offset = overlay_block_descriptor.original_offset
        #             overlay_bitmap = overlay_block_descriptor.bitmap
        #             # bitmap stuff

        # if not descriptor:
        #     if not self.next_store and self.index == active_store.index:
        #         reverse_block_descriptor = self.block_list.map.reverse.get(block)
        #         in_bitmap = self.bitmap.in_use(block)
        #         in_previous_bitmap = self.previous_bitmap.in_use(block)

        # if descriptor:
        #     if descriptor.is_forwarder and self.next_store:
        #         print("reading forwarder from next store")
        #         return self.next_store.read_block(block, active_store)
        #     else:
        #         print("reading from current store")
        #         self.fh.seek(block_data_offset)
        #         return self.fh.read(BLOCK_SIZE)
        # else:
        #     if self.next_store:
        #         print("reading from next store")
        #         return self.next_store.read_block(block, active_store)
        #     elif not reverse_block_descriptor and not in_bitmap and not in_previous_bitmap:
        #         print("zero'ing")
        #         return b'\x00' * BLOCK_SIZE
        #     else:
        #         print("reading from volume")
        #         self.fh.seek(block * BLOCK_SIZE)
        #         return self.fh.read(BLOCK_SIZE)


class StoreStream(AlignedStream):
    def __init__(self, store: Store):
        self.store = store
        super().__init__(size=store.size, align=BLOCK_SIZE)

    def _read(self, offset: int, length: int) -> bytes:
        r = []
        blockidx = offset // BLOCK_SIZE

        while length > 0:
            r.append(self.store.read_block(blockidx))
            blockidx += 1
            length -= BLOCK_SIZE

        return b"".join(r)


class BlockList:
    def __init__(self, store: Store, offset: int):
        self.store = store
        self.offset = offset

        # t = time.time()
        self.header, data = read_block(store.fh, offset, c_vss.store_header)
        # print time.time() - t

        if self.header.record_type != RECORD_TYPE.STORE_INDEX:
            raise Error(
                "invalid store header type for block list"
                f" (got 0x{self.header.record_type:x}, expected 0x{RECORD_TYPE.STORE_INDEX:x}"
            )

        # t = time.time()
        buf = memoryview(data)
        self.map = BlockMap()
        for entry_offset in range(0, len(data), STORE_BLOCKLIST_ENTRY_SIZE):
            entry_buf = buf[entry_offset : entry_offset + STORE_BLOCKLIST_ENTRY_SIZE]
            if entry_buf == b"\x00" * STORE_BLOCKLIST_ENTRY_SIZE:
                break
            entry = BlockDescriptor(entry_buf)
            # self.entries.append(entry)
            self.map.add(entry)
        # print time.time() - t


class RangeList:
    def __init__(self, store: Store, offset: int):
        self.store = store
        self.offset = offset

        self.header, data = read_block(store.fh, offset, c_vss.store_header)

        if self.header.record_type != RECORD_TYPE.STORE_BLOCK_RANGE:
            raise Error(
                "invalid store header type for block list"
                f" (got 0x{self.header.record_type:x}, expected 0x{RECORD_TYPE.STORE_BLOCK_RANGE:x}"
            )

        buf = memoryview(data)
        self.entries = []
        for entry_offset in range(0, len(data), STORE_RANGELIST_ENTRY_SIZE):
            entry_buf = buf[entry_offset : entry_offset + STORE_RANGELIST_ENTRY_SIZE]
            if entry_buf == b"\x00" * STORE_RANGELIST_ENTRY_SIZE:
                break
            entry = c_vss.range_descriptor(entry_buf)
            self.entries.append(entry)


class StoreBitmap:
    def __init__(self, store: Store, offset: int):
        self.store = store
        self.offset = offset

        self.header, self.data = read_block(store.fh, offset, c_vss.store_header)

        if self.header.record_type != RECORD_TYPE.STORE_BITMAP:
            raise Error(
                "invalid store header type for bitmap"
                f" (got 0x{self.header.record_type:x}, expected 0x{RECORD_TYPE.STORE_BITMAP:x}"
            )

        buf = memoryview(self.data)
        self.test = []
        for i in range(0, len(self.data) // 4, 4):
            b = buf[i : i + 4]
            val = c_vss.uint32(b)
            for _ in range(32):
                if val & 0x00000001 == 0:
                    self.test.append(False)
                else:
                    self.test.append(True)
                val >>= 1

    def has_offset(self, offset: int) -> bool:
        return self.in_use(offset // BLOCK_SIZE)

    def in_use(self, block: int) -> bool:
        return not self.is_set(block)

    def is_set(self, block: int) -> bool:
        return (self.data[block // 8] & (1 << (block % 8))) != 0

    def __getitem__(self, block: int) -> bool:
        return self.in_use(block)


class BlockMap:
    def __init__(self):
        self.map = {}
        self.reverse = {}

    def add(self, descriptor: BlockDescriptor) -> None:
        if not descriptor.is_used:
            return

        blockmap = self.map
        reversemap = self.reverse
        key = descriptor.original_offset // BLOCK_SIZE

        if not descriptor.is_overlay:
            revkey = descriptor.relative_offset // BLOCK_SIZE
            try:
                revexist = reversemap[revkey]
            except KeyError:
                revexist = None

            if revexist:
                descriptor.original_offset = revexist.relative_offset
                del reversemap[revkey]

        if descriptor.is_forwarder and descriptor.original_offset == descriptor.relative_offset:
            return

        try:
            existing = blockmap[key]
        except KeyError:
            existing = None

        if existing:
            if descriptor.is_overlay:
                overlay = existing if existing.is_overlay else existing.overlay

                if overlay:
                    overlay.bitmap |= descriptor.bitmap
                else:
                    existing.overlay = descriptor
                return

            if existing.is_overlay:
                descriptor.overlay = existing
            else:
                descriptor.overlay = existing.overlay

        blockmap[key] = descriptor

        if descriptor.is_forwarder:
            revkey = descriptor.relative_offset // BLOCK_SIZE
            reversemap[revkey] = descriptor

    def get_descriptor(self, offset: int) -> None:
        pass

    def __getitem__(self, block: int) -> BlockMap:
        return self


class BlockDescriptor:
    __slots__ = (
        "bitmap",
        "flags",
        "is_forwarder",
        "is_overlay",
        "is_used",
        "original_offset",
        "overlay",
        "relative_offset",
        "store",
        "store_offset",
    )

    def __init__(self, buf: bytes):
        # self.store = store

        entry = c_vss.block_descriptor(buf)
        self.original_offset = entry.original_offset
        self.relative_offset = entry.relative_offset
        self.store_offset = entry.store_offset
        self.flags = entry.flags
        self.bitmap = entry.allocation_bitmap

        self.overlay = None
        self.is_used = not bool(self.flags & BLOCK_FLAG.NOT_USED)
        self.is_overlay = bool(self.flags & BLOCK_FLAG.IS_OVERLAY)
        self.is_forwarder = bool(self.flags & BLOCK_FLAG.IS_FORWARDER)

    def __eq__(self, other: object) -> bool:
        if not isinstance(BlockDescriptor, other):
            return False

        return (
            self.original_offset == other.original_offset
            and self.relative_offset == other.relative_offset
            and self.store_offset == other.store_offset
            and self.flags == other.flags
            and self.bitmap == other.bitmap
        )

    def __repr__(self) -> str:
        return (
            f"<BlockDescriptor original_offset=0x{self.original_offset:08x}"
            f" relative_offset=0x{self.relative_offset:08x} store_offset=0x{self.store_offset:08x}"
            f" flags={self.flags} bitmap=0x{self.bitmap:08x}>"
        )


# libvshadow_store_descriptor_read_buffer


def read_block(fh: BinaryIO, offset: int, struct: type[Structure]) -> tuple[Structure, bytes]:
    header, buf = read_block_data(fh, offset, struct)

    r = [buf]
    offset = header.next_offset
    while offset != 0:
        nheader, buf = read_block_data(fh, offset, struct)
        offset = nheader.next_offset
        r.append(buf)

    return header, b"".join(r)


def read_block_data(fh: BinaryIO, offset: int, struct: type[Structure]) -> tuple[Structure, bytes]:
    fh.seek(offset)
    buf = fh.read(BLOCK_SIZE)
    header = struct(buf)

    if header.identifier != VSS_IDENTIFIER:
        raise Error(f"not a valid VSS identifier (got {header.identifier.hex()}, expected {VSS_IDENTIFIER.hex()}")

    return header, buf[128:]
