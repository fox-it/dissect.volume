from __future__ import annotations

from typing import TYPE_CHECKING

from dissect.util.stream import AlignedStream, RelativeStream

if TYPE_CHECKING:
    from dissect.volume.ddf.ddf import VirtualDisk


class VirtualDiskStream(AlignedStream):
    def __init__(self, vd: VirtualDisk):
        self.vd = vd
        self.extents = [
            RelativeStream(disk.fh, offset * disk.block_size) for disk, offset in zip(vd.disks, vd.vdc.starting_block)
        ]
        self.num_extents = len(self.extents)
        self.stripe_size = self.vd.stripe_size
        self.raid_config = (self.vd.vdc.primary_raid_level, self.vd.vdc.raid_level_qualifier)
        super().__init__(vd.size, align=vd.stripe_size)

    def _calculate_read(self, offset: int) -> tuple[int, int, int, int]:
        n = self.num_extents
        stripe_size = self.stripe_size

        if self.raid_config == (0x05, 0x03):
            parity_extent = (n - 1) - (((offset // stripe_size) // (n - 1)) % n)
            data_extent = (((offset // stripe_size) % (n - 1)) + parity_extent + 1) % n
            stripe = (offset // stripe_size) // (n - 1)
            block = offset % stripe_size

            return parity_extent, data_extent, stripe, block
        else:
            raise NotImplementedError(f"Unsupported RAID configuration: {self.raid_config}")

    def _read(self, offset: int, length: int) -> bytes:
        result = []
        while length > 0:
            _, data_extent, stripe, block = self._calculate_read(offset)

            read_size = max(0, min(length, self.stripe_size))
            fh = self.extents[data_extent]
            fh.seek((stripe * self.stripe_size) + block)
            result.append(fh.read(read_size))

            offset += read_size
            length -= read_size

        return b"".join(result)
