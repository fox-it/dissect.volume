from __future__ import annotations

from functools import lru_cache
from typing import BinaryIO, Union

from dissect.volume.dm.c_dm import SECTOR_SIZE, c_dm


class BTree:
    def __init__(self, fh: BinaryIO, root: int, block_size: int):
        self.fh = fh
        self.root = root
        self.block_size = block_size
        self._block_size_bytes = self.block_size * SECTOR_SIZE

        self._read_node = lru_cache(64)(self._read_node)

    def _read_node(self, block: int) -> Node:
        self.fh.seek(block * self._block_size_bytes)
        return Node(self.fh.read(self._block_size_bytes))

    def lookup(self, keys: Union[int, list[int]], want_high: bool = False) -> int:
        keys = [keys] if not isinstance(keys, list) else keys

        root = self.root
        last_level = len(keys) - 1
        value = None
        for i, key in enumerate(keys):
            found_key, value = self._lookup(root, key, want_high)
            if found_key != key:
                return None

            if i < last_level:
                root = int.from_bytes(value, "little")

        return value

    def _lookup(self, root: int, key: int, want_high: bool = False) -> tuple[int, bytes]:
        block = root
        while True:
            node = self._read_node(block)

            low = -1
            high = node.num_entries
            while high - low > 1:
                mid = low + ((high - low) // 2)
                cmp_key = node.key(mid)

                if cmp_key == key:
                    result = mid
                    break

                if cmp_key < key:
                    low = mid
                else:
                    high = mid
            else:
                result = high if want_high else low

            if node.is_internal:
                block = result
            elif node.is_leaf:
                return node.key(result), node.value(result)


class Node:
    def __init__(self, buf: bytes):
        self.buf = memoryview(buf)
        self.header = c_dm.node_header(self.buf)

        self.num_entries = self.header.nr_entries
        self.max_entries = self.header.max_entries
        self.value_size = self.header.value_size

        key_area_start = len(c_dm.node_header)
        key_area_size = self.max_entries * 8
        key_area_end = key_area_start + key_area_size
        value_area_size = self.max_entries * self.value_size

        self._key_area = self.buf[key_area_start:key_area_end]
        self._value_area = self.buf[key_area_end : key_area_end + value_area_size]

        self.key = lru_cache(1024)(self.key)

    @property
    def is_internal(self) -> bool:
        return bool(self.header.flags & c_dm.node_flags.INTERNAL_NODE.value)

    @property
    def is_leaf(self) -> bool:
        return bool(self.header.flags & c_dm.node_flags.LEAF_NODE.value)

    def key(self, idx: int) -> int:
        if idx >= self.num_entries:
            raise IndexError("Key index out of bounds")
        area = idx * 8
        return int.from_bytes(self._key_area[area : area + 8], "little")

    def value(self, idx: int) -> bytes:
        if idx >= self.num_entries:
            raise IndexError("Value index out of bounds")
        area = idx * self.value_size
        return self._value_area[area : area + self.value_size]
