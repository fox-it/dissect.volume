from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING, NamedTuple, Optional

from dissect.util.stream import AlignedStream, MappingStream

from dissect.volume.exceptions import RAIDError

if TYPE_CHECKING:
    from dissect.volume.raid.raid import PhysicalDisk, VirtualDisk


class Level(IntEnum):
    """RAID level identifiers."""

    LINEAR = -1
    # The rest is really just the RAID number
    RAID0 = 0
    RAID1 = 1
    RAID3 = 3
    RAID4 = 4
    RAID5 = 5
    RAID6 = 6
    RAID10 = 10


class Layout(IntEnum):
    """RAID layout identifiers.

    "Coincidentally" these align with Linux MD.
    """

    LEFT_ASYMMETRIC = 0  # Rotating Parity N with Data Restart
    RIGHT_ASYMMETRIC = 1  # Rotating Parity 0 with Data Restart
    LEFT_SYMMETRIC = 2  # Rotating Parity N with Data Continuation
    RIGHT_SYMMETRIC = 3  # Rotating Parity 0 with Data Continuation
    PARITY_0 = 4  # P or P,Q are initial devices
    PARITY_N = 5  # P or P,Q are final devices
    ROTATING_ZERO_RESTART = 8  # DDF PRL=6 RLQ=1
    ROTATING_N_RESTART = 9  # DDF PRL=6 RLQ=2
    ROTATING_N_CONTINUE = 10  # DDF PRL=6 RLQ=3
    LEFT_ASYMMETRIC_6 = 16
    RIGHT_ASYMMETRIC_6 = 17
    LEFT_SYMMETRIC_6 = 18
    RIGHT_SYMMETRIC_6 = 19
    PARITY_0_6 = 20
    PARITY_N_6 = PARITY_N


class LinearStream(MappingStream):
    """Implements a stream on a linear RAID set."""

    def __init__(self, virtual_disk: VirtualDisk):
        super().__init__()
        self.virtual_disk = virtual_disk

        physical_disks: dict[int, tuple[int, PhysicalDisk]] = dict(sorted(virtual_disk.physical_disks.items()))
        if len(physical_disks) != virtual_disk.num_disks:
            raise RAIDError(f"Missing disks in linear RAID set {virtual_disk.uuid} ({virtual_disk.name})")

        offset = 0
        for disk_offset, disk in physical_disks.values():
            self.add(offset, disk.size, disk.open(), disk_offset)
            offset += disk.size


class Zone(NamedTuple):
    zone_end: int
    dev_start: int
    devices: list[tuple[int, PhysicalDisk]]


class RAID0Stream(AlignedStream):
    """Implements a stream on a RAID0 set."""

    def __init__(self, virtual_disk: VirtualDisk):
        self.virtual_disk = virtual_disk

        disks = self.virtual_disk.physical_disks
        if len(disks) != virtual_disk.num_disks:
            raise RAIDError(f"Missing disks in RAID0 set {virtual_disk.uuid} ({virtual_disk.name})")

        # Determine how many strip zones we need to construct
        # If a RAID0 set consists of devices with different sizes, additional strip zones
        # may exist on the larger devices but not on the smaller ones
        # Reference: create_strip_zones
        disks: dict[int, tuple[int, PhysicalDisk]] = dict(sorted(disks.items()))
        rounded_sizes = {}

        stripe_size = virtual_disk.stripe_size
        num_strip_zones = 0
        for idx1, (_, dev1) in disks.items():
            rounded_sizes[dev1] = (dev1.size // stripe_size) * stripe_size

            has_same_size = False
            # Check if dev1 is unequal in size to the sizes of any of the previous devices
            # If so, this means an extra strip zone is present
            for idx2, (_, dev2) in disks.items():
                if idx1 == idx2:
                    break

                if rounded_sizes[dev1] == dev2.size:
                    has_same_size = True
                    break

            if not has_same_size:
                num_strip_zones += 1

        # Determine the smallest device
        smallest = None
        for _, dev in disks.values():
            if not smallest or rounded_sizes[dev] < rounded_sizes[smallest]:
                smallest = dev

        # Construct the strip zones
        zones = [Zone(rounded_sizes[smallest] * len(disks), 0, disks)]

        cur_zone_end = zones[0].zone_end
        for _ in range(1, num_strip_zones):
            zone_devices = []
            dev_start = rounded_sizes[smallest]
            smallest = None

            # Look for the next smallest device, that is: the smallest device that is larger than the "dev_start" device
            for _, dev in disks.values():
                if rounded_sizes[dev] <= dev_start:
                    continue

                zone_devices.append(dev)
                if not smallest or rounded_sizes[dev] < rounded_sizes[smallest]:
                    smallest = dev

            num_dev = len(zone_devices)
            cur_size = (rounded_sizes[smallest] - dev_start) * num_dev

            cur_zone_end += cur_size

            zones.append(Zone(cur_zone_end, dev_start, zone_devices))

        self.zones = zones

        super().__init__(self.virtual_disk.size)

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

        stripe_size = self.virtual_disk.stripe_size
        while length:
            zone, offset_in_zone = self._find_zone(offset)
            if zone is None:
                break

            read_offset = offset_in_zone
            if len(self.zones) == 1 or len(self.zones[1].devices) == 1:
                read_offset = offset

            stripe, offset_in_stripe = divmod(read_offset, stripe_size)
            chunk = offset_in_zone // (stripe_size * len(zone.devices))

            offset_in_device = (chunk * stripe_size) + offset_in_stripe
            device_start, device = zone.devices[stripe % len(zone.devices)]

            stripe_remaining = stripe_size - offset_in_stripe
            read_length = min(length, stripe_remaining)

            offset_on_disk = device.offset + device_start + offset_in_device
            device.fh.seek(offset_on_disk)
            result.append(device.fh.read(read_length))

            length -= read_length
            offset += read_length

        return b"".join(result)


class RAID456Stream(AlignedStream):
    """Implements a stream on a RAID5 set."""

    def __init__(self, virtual_disk: VirtualDisk):
        self.virtual_disk = virtual_disk
        self.level = self.virtual_disk.level
        self.algorithm = self.virtual_disk.layout
        self.max_degraded = 2 if self.level == 6 else 1

        self.disks = self.virtual_disk.physical_disks
        if len(self.disks) < self.virtual_disk.num_disks - self.max_degraded:
            raise RAIDError(f"Missing disks in RAID{self.level} set {virtual_disk.uuid} ({virtual_disk.name})")

        super().__init__(self.virtual_disk.size, self.virtual_disk.stripe_size)

    def _get_stripe_read_info(self, offset: int) -> tuple[int, int, int, int, Optional[int]]:
        """Calculate the stripe, offset in the stripe, data disk, parity disk and "Q" parity disk for a given sector."""

        # Reference: raid5_compute_sector
        stripe_size = self.virtual_disk.stripe_size
        raid_disks = self.virtual_disk.num_disks
        data_disks = raid_disks - self.max_degraded

        stripe_number, offset_in_stripe = divmod(offset, stripe_size)
        stripe, dd_idx = divmod(stripe_number, data_disks)

        pd_idx = None
        qd_idx = None
        ddf_layout = False

        if self.level == 4:
            pd_idx = data_disks

        elif self.level == 5:
            if self.algorithm == Layout.LEFT_ASYMMETRIC:
                pd_idx = data_disks - (stripe % raid_disks)
                if dd_idx >= pd_idx:
                    dd_idx += 1

            elif self.algorithm == Layout.RIGHT_ASYMMETRIC:
                pd_idx = stripe % raid_disks
                if dd_idx >= pd_idx:
                    dd_idx += 1

            elif self.algorithm == Layout.LEFT_SYMMETRIC:
                pd_idx = data_disks - (stripe % raid_disks)
                dd_idx = (pd_idx + 1 + dd_idx) % raid_disks

            elif self.algorithm == Layout.RIGHT_SYMMETRIC:
                pd_idx = stripe % raid_disks
                dd_idx = (pd_idx + 1 + dd_idx) % raid_disks

            elif self.algorithm == Layout.PARITY_0:
                pd_idx = 0
                dd_idx += 1

            elif self.algorithm == Layout.PARITY_N:
                pd_idx = data_disks

            else:
                raise RAIDError(f"Invalid RAID algorithm: {self.algorithm}")

        elif self.level == 6:
            if self.algorithm == Layout.LEFT_ASYMMETRIC:
                pd_idx = raid_disks - 1 - (stripe % raid_disks)
                qd_idx = pd_idx + 1
                if pd_idx == raid_disks - 1:
                    # Q D D D P
                    dd_idx += 1
                    qd_idx = 0
                elif dd_idx >= pd_idx:
                    # D D P Q D
                    dd_idx += 2

            elif self.algorithm == Layout.RIGHT_ASYMMETRIC:
                pd_idx = stripe % raid_disks
                qd_idx = pd_idx + 1
                if pd_idx == raid_disks - 1:
                    # Q D D D P
                    dd_idx += 1
                    qd_idx = 0
                elif dd_idx >= pd_idx:
                    # D D P Q D
                    dd_idx += 2

            elif self.algorithm == Layout.LEFT_SYMMETRIC:
                pd_idx = raid_disks - 1 - (stripe % raid_disks)
                qd_idx = (pd_idx + 1) % raid_disks
                dd_idx = (pd_idx + 2 + dd_idx) % raid_disks

            elif self.algorithm == Layout.RIGHT_SYMMETRIC:
                pd_idx = stripe % raid_disks
                qd_idx = (pd_idx + 1) % raid_disks
                dd_idx = (pd_idx + 2 + dd_idx) % raid_disks

            elif self.algorithm == Layout.PARITY_0:
                pd_idx = 0
                qd_idx = 1
                dd_idx += 2

            elif self.algorithm == Layout.PARITY_N:
                pd_idx = data_disks
                qd_idx = data_disks + 1

            elif self.algorithm == Layout.ROTATING_ZERO_RESTART:
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

            elif self.algorithm == Layout.ROTATING_N_RESTART:
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

            elif self.algorithm == Layout.ROTATING_N_CONTINUE:
                # Same as left_symmetric but Q is before P
                pd_idx = raid_disks - 1 - (stripe % raid_disks)
                qd_idx = (pd_idx + raid_disks - 1) % raid_disks
                dd_idx = (pd_idx + 1 + dd_idx) % raid_disks
                ddf_layout = True

            elif self.algorithm == Layout.LEFT_ASYMMETRIC_6:
                # RAID5 left_asymmetric, with Q on last device
                pd_idx = data_disks - (stripe % (raid_disks - 1))
                if dd_idx >= pd_idx:
                    dd_idx += 1
                qd_idx = raid_disks - 1

            elif self.algorithm == Layout.RIGHT_ASYMMETRIC_6:
                pd_idx = stripe % (raid_disks - 1)
                if dd_idx >= pd_idx:
                    dd_idx += 1
                    qd_idx = raid_disks - 1

            elif self.algorithm == Layout.LEFT_SYMMETRIC_6:
                pd_idx = data_disks - (stripe % (raid_disks - 1))
                dd_idx = (pd_idx + 1 + dd_idx) % (raid_disks - 1)
                qd_idx = raid_disks - 1

            elif self.algorithm == Layout.RIGHT_SYMMETRIC_6:
                pd_idx = stripe % (raid_disks - 1)
                dd_idx = (pd_idx + 1 + dd_idx) % (raid_disks - 1)
                qd_idx = raid_disks - 1

            elif self.algorithm == Layout.PARITY_0_6:
                pd_idx = 0
                dd_idx += 1
                qd_idx = raid_disks - 1
            else:
                raise RAIDError(f"Invalid RAID algorithm: {self.algorithm}")
        else:
            raise RAIDError(f"Invalid RAID level: {self.level}")

        if ddf_layout:
            raise NotImplementedError("DDF layout")

        return stripe, offset_in_stripe, dd_idx, pd_idx, qd_idx

    def _read(self, offset: int, length: int) -> bytes:
        result = []

        stripe_size = self.virtual_disk.stripe_size
        while length:
            stripe, offset_in_stripe, dd_idx, pd_idx, qd_idx = self._get_stripe_read_info(offset)
            offset_in_device = stripe * stripe_size + offset_in_stripe
            dd_start, dd_dev = self.disks[dd_idx]

            stripe_remaining = stripe_size - offset_in_stripe
            read_length = min(length, stripe_remaining)

            offset_on_disk = dd_dev.offset + dd_start + offset_in_device
            dd_dev.fh.seek(offset_on_disk)
            result.append(dd_dev.fh.read(length))

            length -= read_length
            offset += read_length

        return b"".join(result)


class RAID10Stream(AlignedStream):
    """Implements a stream on a RAID10 set."""

    def __init__(self, virtual_disk: VirtualDisk):
        self.virtual_disk = virtual_disk
        self.raid_disks = self.virtual_disk.num_disks
        self.devices = virtual_disk.physical_disks

        # Reference: setup_geo
        layout = virtual_disk.layout
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

        super().__init__(self.virtual_disk.size, self.virtual_disk.stripe_size)

    def _read(self, offset: int, length: int) -> bytes:
        result = []

        stripe_size = self.virtual_disk.stripe_size
        while length:
            # Reference: __raid10_find_phys
            stripe_number, offset_in_stripe = divmod(offset, stripe_size)
            stripe, dev = divmod(stripe_number * self.near_copies, self.raid_disks)

            if self.far_offset:
                stripe *= self.far_copies
            device_start, device = self.devices[dev]

            stripe_remaining = stripe_size - offset_in_stripe
            read_length = min(length, stripe_remaining)

            offset_on_disk = device.offset + device_start + (stripe * stripe_size) + offset_in_stripe
            device.fh.seek(offset_on_disk)
            result.append(device.fh.read(length))

            length -= read_length
            offset += read_length

        return b"".join(result)
