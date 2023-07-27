from __future__ import annotations

import operator
from typing import TYPE_CHECKING, NamedTuple, Optional

from dissect.util.stream import AlignedStream, MappingStream

from dissect.volume.exceptions import MDError
from dissect.volume.md.c_md import SECTOR_SIZE, c_md

if TYPE_CHECKING:
    from dissect.volume.md.md import MD, Device


class LinearStream(MappingStream):
    """Implements a stream on a linear RAID set."""

    def __init__(self, md: MD):
        super().__init__()
        self.md = md

        offset = 0
        for device in md.devices:
            # Ignore special device roles for now
            if device.raid_disk is None:
                continue

            device_size = device.data_size * SECTOR_SIZE
            self.add(offset, device_size, device.open(), 0)
            offset += device_size


class Zone(NamedTuple):
    zone_end: int
    dev_start: int
    devices: list[Device]


class RAID0Stream(AlignedStream):
    """Implements a stream on a RAID0 set."""

    def __init__(self, md: MD):
        self.md = md
        self.devices = {dev.raid_disk: dev for dev in self.md.devices if dev.raid_disk is not None}
        if len(self.devices) != md.raid_disks:
            raise MDError(f"Missing disks in RAID0 set {self.md.uuid} ({self.md.name})")

        # Determine how many strip zones we need to construct
        # If a RAID0 set consists of devices with different sizes, additional strip zones
        # may exist on the larger devices but not on the smaller ones
        # Reference: create_strip_zones
        devices = sorted(self.devices.values(), key=operator.attrgetter("raid_disk"))
        rounded_sectors = {}

        num_strip_zones = 0
        for dev1 in devices:
            rounded_sectors[dev1] = (dev1.sectors // dev1.chunk_sectors) * dev1.chunk_sectors

            has_same_sectors = False
            # Check if dev1 is unequal in size to the sizes of any of the previous devices
            # If so, this means an extra strip zone is present
            for dev2 in devices:
                if dev1.dev_number == dev2.dev_number:
                    break

                if rounded_sectors[dev1] == dev2.sectors:
                    has_same_sectors = True
                    break

            if not has_same_sectors:
                num_strip_zones += 1

        # Determine the smallest device and calculate the overal volume size at the same time
        size = 0
        smallest = None
        for dev in devices:
            if not smallest or rounded_sectors[dev] < rounded_sectors[smallest]:
                smallest = dev

            # Calculate a rounded up size of the RAID0 set in sectors
            size += rounded_sectors[dev] & ~(self.md.chunk_sectors - 1)

        # Construct the strip zones
        zones = [Zone(rounded_sectors[smallest] * len(devices), 0, devices)]

        cur_zone_end = zones[0].zone_end
        for _ in range(1, num_strip_zones):
            zone_devices = []
            dev_start = rounded_sectors[smallest]
            smallest = None

            # Look for the next smallest device, that is: the smallest device that is larger than the "dev_start" device
            for dev in devices:
                if rounded_sectors[dev] <= dev_start:
                    continue

                zone_devices.append(dev)
                if not smallest or rounded_sectors[dev] < rounded_sectors[smallest]:
                    smallest = dev

            num_dev = len(zone_devices)
            sectors = (rounded_sectors[smallest] - dev_start) * num_dev

            cur_zone_end += sectors

            zones.append(Zone(cur_zone_end, dev_start, zone_devices))

        self.zones = zones

        super().__init__(size * SECTOR_SIZE)

    def _find_zone(self, offset: int) -> Optional[tuple[Zone, int]]:
        """Return the zone and the offset within that zone a given ``offset`` is in."""
        for i, zone in enumerate(self.zones):
            if offset < zone.zone_end:
                if i:
                    offset = offset - self.zones[i - 1].zone_end
                return zone, offset
        return None, None

    def _read(self, offset: int, length: int) -> bytes:
        result = []

        chunk_sectors = self.md.chunk_sectors
        offset_sector = offset // SECTOR_SIZE
        num_sectors = (length + SECTOR_SIZE - 1) // SECTOR_SIZE
        while length:
            zone, sector_in_zone = self._find_zone(offset_sector)
            if zone is None:
                break

            sector = sector_in_zone
            if len(self.zones) == 1 or len(self.zones[1].devices) == 1:
                sector = offset_sector

            sector, sector_in_chunk = divmod(sector, chunk_sectors)
            chunk = sector_in_zone // (chunk_sectors * len(zone.devices))

            sector_in_device = (chunk * chunk_sectors) + sector_in_chunk
            device = zone.devices[sector % len(zone.devices)]

            chunk_remaining = chunk_sectors - sector_in_chunk
            read_sectors = min(num_sectors, chunk_remaining)
            read_length = min(read_sectors * SECTOR_SIZE, length)

            sector_on_disk = device.data_offset + sector_in_device
            device.fh.seek(sector_on_disk * SECTOR_SIZE)
            result.append(device.fh.read(read_length))

            num_sectors -= read_sectors
            length -= read_length
            offset_sector += read_sectors

        return b"".join(result)


class RAID456Stream(AlignedStream):
    """Implements a stream on a RAID5 set."""

    def __init__(self, md: MD):
        self.md = md
        self.max_degraded = 2 if self.md.level == 6 else 1
        self.level = self.md.level
        self.algorithm = self.md.layout

        self.devices = {dev.raid_disk: dev for dev in self.md.devices if dev.raid_disk is not None}
        if len(self.devices) < md.raid_disks - self.max_degraded:
            raise MDError(f"Missing disks in RAID{self.level} set {self.md.uuid} ({self.md.name})")

        super().__init__(self.md.sb.size * SECTOR_SIZE, self.md.chunk_size)

    def _get_stripe_read_info(self, sector: int) -> tuple[int, int, int, int, Optional[int]]:
        """Calculate the stripe, offset in the stripe, data disk, parity disk and "Q" parity disk for a given sector."""

        # Reference: raid5_compute_sector
        sectors_per_chunk = self.md.chunk_sectors
        raid_disks = self.md.raid_disks
        data_disks = raid_disks - self.max_degraded

        chunk_number, chunk_offset = divmod(sector, sectors_per_chunk)
        stripe, dd_idx = divmod(chunk_number, data_disks)

        pd_idx = None
        qd_idx = None
        ddf_layout = False

        if self.level == 4:
            pd_idx = data_disks

        elif self.level == 5:
            if self.algorithm == c_md.ALGORITHM_LEFT_ASYMMETRIC:
                pd_idx = data_disks - (stripe % raid_disks)
                if dd_idx >= pd_idx:
                    dd_idx += 1

            elif self.algorithm == c_md.ALGORITHM_RIGHT_ASYMMETRIC:
                pd_idx = stripe % raid_disks
                if dd_idx >= pd_idx:
                    dd_idx += 1

            elif self.algorithm == c_md.ALGORITHM_LEFT_SYMMETRIC:
                pd_idx = data_disks - (stripe % raid_disks)
                dd_idx = (pd_idx + 1 + dd_idx) % raid_disks

            elif self.algorithm == c_md.ALGORITHM_RIGHT_SYMMETRIC:
                pd_idx = stripe % raid_disks
                dd_idx = (pd_idx + 1 + dd_idx) % raid_disks

            elif self.algorithm == c_md.ALGORITHM_PARITY_0:
                pd_idx = 0
                dd_idx += 1

            elif self.algorithm == c_md.ALGORITHM_PARITY_N:
                pd_idx = data_disks

            else:
                raise MDError(f"Invalid RAID algorithm: {self.algorithm}")

        elif self.level == 6:
            if self.algorithm == c_md.ALGORITHM_LEFT_ASYMMETRIC:
                pd_idx = raid_disks - 1 - (stripe % raid_disks)
                qd_idx = pd_idx + 1
                if pd_idx == raid_disks - 1:
                    # Q D D D P
                    dd_idx += 1
                    qd_idx = 0
                elif dd_idx >= pd_idx:
                    # D D P Q D
                    dd_idx += 2

            elif self.algorithm == c_md.ALGORITHM_RIGHT_ASYMMETRIC:
                pd_idx = stripe % raid_disks
                qd_idx = pd_idx + 1
                if pd_idx == raid_disks - 1:
                    # Q D D D P
                    dd_idx += 1
                    qd_idx = 0
                elif dd_idx >= pd_idx:
                    # D D P Q D
                    dd_idx += 2

            elif self.algorithm == c_md.ALGORITHM_LEFT_SYMMETRIC:
                pd_idx = raid_disks - 1 - (stripe % raid_disks)
                qd_idx = (pd_idx + 1) % raid_disks
                dd_idx = (pd_idx + 2 + dd_idx) % raid_disks

            elif self.algorithm == c_md.ALGORITHM_RIGHT_SYMMETRIC:
                pd_idx = stripe % raid_disks
                qd_idx = (pd_idx + 1) % raid_disks
                dd_idx = (pd_idx + 2 + dd_idx) % raid_disks

            elif self.algorithm == c_md.ALGORITHM_PARITY_0:
                pd_idx = 0
                qd_idx = 1
                dd_idx += 2

            elif self.algorithm == c_md.ALGORITHM_PARITY_N:
                pd_idx = data_disks
                qd_idx = data_disks + 1

            elif self.algorithm == c_md.ALGORITHM_ROTATING_ZERO_RESTART:
                # Exactly the same as RIGHT_ASYMMETRIC, but or of blocks for computing Q is different
                pd_idx = stripe % raid_disks
                qd_idx = pd_idx + 1
                if pd_idx == raid_disks - 1:
                    dd_idx += 1
                    qd_idx = 0
                elif dd_idx >= pd_idx:
                    # D D P Q D
                    dd_idx += 2
                ddf_layout = True

            elif self.algorithm == c_md.ALGORITHM_ROTATING_N_RESTART:
                # Same as left_asymmetric, but first stripe is
                # D D D P Q  rather than
                # Q D D D P
                pd_idx = raid_disks - 1 - ((stripe + 1) % raid_disks)
                qd_idx = pd_idx + 1
                if pd_idx == raid_disks - 1:
                    # Q D D D P
                    dd_idx += 1
                    qd_idx = 0
                elif dd_idx >= pd_idx:
                    # D D P Q D
                    dd_idx += 2
                ddf_layout = True

            elif self.algorithm == c_md.ALGORITHM_ROTATING_N_CONTINUE:
                # Same as left_symmetric but Q is before P
                pd_idx = raid_disks - 1 - (stripe % raid_disks)
                qd_idx = (pd_idx + raid_disks - 1) % raid_disks
                dd_idx = (pd_idx + 1 + dd_idx) % raid_disks
                ddf_layout = True

            elif self.algorithm == c_md.ALGORITHM_LEFT_ASYMMETRIC_6:
                # RAID5 left_asymmetric, with Q on last device
                pd_idx = data_disks - (stripe % (raid_disks - 1))
                if dd_idx >= pd_idx:
                    dd_idx += 1
                qd_idx = raid_disks - 1

            elif self.algorithm == c_md.ALGORITHM_RIGHT_ASYMMETRIC_6:
                pd_idx = stripe % (raid_disks - 1)
                if dd_idx >= pd_idx:
                    dd_idx += 1
                    qd_idx = raid_disks - 1

            elif self.algorithm == c_md.ALGORITHM_LEFT_SYMMETRIC_6:
                pd_idx = data_disks - (stripe % (raid_disks - 1))
                dd_idx = (pd_idx + 1 + dd_idx) % (raid_disks - 1)
                qd_idx = raid_disks - 1

            elif self.algorithm == c_md.ALGORITHM_RIGHT_SYMMETRIC_6:
                pd_idx = stripe % (raid_disks - 1)
                dd_idx = (pd_idx + 1 + dd_idx) % (raid_disks - 1)
                qd_idx = raid_disks - 1

            elif self.algorithm == c_md.ALGORITHM_PARITY_0_6:
                pd_idx = 0
                dd_idx += 1
                qd_idx = raid_disks - 1
            else:
                raise MDError(f"Invalid RAID algorithm: {self.algorithm}")
        else:
            raise MDError(f"Invalid RAID level: {self.level}")

        if ddf_layout:
            raise NotImplementedError("DDF layout")

        return stripe, chunk_offset, dd_idx, pd_idx, qd_idx

    def _read(self, offset: int, length: int) -> bytes:
        result = []

        chunk_sectors = self.md.chunk_sectors
        offset_sector = offset // SECTOR_SIZE
        num_sectors = (length + SECTOR_SIZE - 1) // SECTOR_SIZE

        while length:
            stripe, sector_in_chunk, dd_idx, pd_idx, qd_idx = self._get_stripe_read_info(offset_sector)
            sector_in_device = stripe * chunk_sectors + sector_in_chunk
            dd_dev = self.devices[dd_idx]

            chunk_remaining = chunk_sectors - sector_in_chunk
            read_sectors = min(num_sectors, chunk_remaining)
            read_length = min(read_sectors * SECTOR_SIZE, length)

            sector_on_disk = dd_dev.data_offset + sector_in_device
            dd_dev.fh.seek(sector_on_disk * SECTOR_SIZE)
            result.append(dd_dev.fh.read(length))

            num_sectors -= read_sectors
            length -= read_length
            offset_sector += read_sectors

        return b"".join(result)


class RAID10Stream(AlignedStream):
    """Implements a stream on a RAID10 set."""

    def __init__(self, md: MD):
        self.md = md
        self.raid_disks = self.md.raid_disks

        self.devices = {dev.raid_disk: dev for dev in self.md.devices if dev.raid_disk is not None}

        # Reference: setup_geo
        layout = md.layout
        self.near_copies = layout & 0xFF
        self.far_copies = (layout >> 8) & 0xFF
        self.far_offset = layout & (1 << 16)

        use_far_sets = layout >> 17
        if use_far_sets == 0:
            # Original layout
            self.far_set_size = self.raid_disks
        elif use_far_sets == 1:
            # "Improved" but bugged layout
            self.far_set_size = self.raid_disks // self.far_copies
        elif use_far_sets == 2:
            # "Improved" and fixed layout
            self.far_set_size = self.far_copies * self.near_copies
        else:
            raise ValueError("Invalid RAID10 layout: {layout:#x}")

        self.last_far_set_start = ((self.raid_disks / self.far_set_size) - 1) * self.far_set_size
        self.last_far_set_size = self.far_set_size + (self.raid_disks % self.far_set_size)

        self.chunk_mask = self.md.chunk_sectors - 1
        self.chunk_shift = ffz(~self.md.chunk_sectors)

        super().__init__(self.md.sb.size * SECTOR_SIZE, self.md.chunk_size)

    def _read(self, offset: int, length: int) -> bytes:
        result = []

        chunk_sectors = self.md.chunk_sectors
        offset_sector = offset // SECTOR_SIZE
        num_sectors = (length + SECTOR_SIZE - 1) // SECTOR_SIZE

        while length:
            # Reference: __raid10_find_phys
            chunk = (offset_sector >> self.chunk_shift) * self.near_copies
            sector = offset_sector & self.chunk_mask

            stripe, dev = divmod(chunk, self.raid_disks)
            if self.far_offset:
                stripe *= self.far_copies
            device = self.devices[dev]

            chunk_remaining = chunk_sectors - sector
            read_sectors = min(num_sectors, chunk_remaining)
            read_length = min(read_sectors * SECTOR_SIZE, length)

            sector_on_disk = device.data_offset + sector + (stripe << self.chunk_shift)
            device.fh.seek(sector_on_disk * SECTOR_SIZE)
            result.append(device.fh.read(length))

            num_sectors -= read_sectors
            length -= read_length
            offset_sector += read_sectors

        return b"".join(result)


def ffz(val: int) -> int:
    """Find the index of the first 0 bit using some bit flipping magic."""
    return (val ^ -(~val)).bit_length() - 1
