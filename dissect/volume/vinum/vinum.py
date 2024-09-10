# For more information see:
# https://docs.freebsd.org/en/articles/vinum/
# https://www.usenix.org/legacy/events/usenix99/full_papers/lehey/lehey.pdf
from __future__ import annotations

import io
import logging
import os
from collections import defaultdict
from functools import cached_property
from typing import TYPE_CHECKING, BinaryIO, TypedDict, TypeVar

from dissect.util import ts

from dissect.volume.raid.raid import (
    RAID,
    Configuration,
    DiskMap,
    PhysicalDisk,
    VirtualDisk,
)
from dissect.volume.raid.stream import Layout, Level
from dissect.volume.vinum.c_vinum import MAGIC_ACTIVE, MAGIC_INACTIVE, c_vinum
from dissect.volume.vinum.config import (
    SD,
    Plex,
    PlexOrg,
    PlexState,
    SDState,
    Volume,
    parse_vinum_config,
)

if TYPE_CHECKING:
    VinumPhysicalDiskDescriptor = BinaryIO | "VinumPhysicalDisk"

log = logging.getLogger(__name__)
log.setLevel(os.getenv("DISSECT_LOG_VINUM", "CRITICAL"))


class Vinum(RAID):
    """Read Vinum RAID sets of one or multiple devices/file-like objects.

    Use this class to read from Vinum RAID sets.

    A single Vinum RAID set is defined by a Volume in the Vinum configuration.
    This configuration is present on all physical disks and contains
    information on all the RAID sets in the system A Vinum Volume can have one
    or more Plexes.

    A Plex can be thought of as one of the individual disks in a mirrored
    array. A Plex can contain one or more Vinum SDs. The Plex defines the type
    of RAID in which these SDs are organized.

    An SD contains information about the actual physical disk and points to the
    device of this disk.

    Args:
        fh: A single file-like object or :class:`VinumPhysicalDisk`, or a list
            of multiple belonging to the same RAID set.
    """

    def __init__(self, fh: list[VinumPhysicalDiskDescriptor] | VinumPhysicalDiskDescriptor):
        fhs = [fh] if not isinstance(fh, list) else fh
        physical_disks = [VinumPhysicalDisk(fh) if not isinstance(fh, VinumPhysicalDisk) else fh for fh in fhs]

        super().__init__([VinumConfiguration(physical_disks)])


T = TypeVar("T")
ByName = dict[bytes, T]
DefaultByName = defaultdict[bytes, T]


class Config(TypedDict):
    volumes: ByName[Volume]
    plexes: ByName[Plex]
    sds: ByName[SD]


class VinumConfiguration(Configuration):
    def __init__(self, physical_disks: list[VinumPhysicalDisk]):
        # These hold the most recent config for each volume/plex/sd
        config: Config = {"volumes": {}, "plexes": {}, "sds": {}}
        disks_by_name: ByName[VinumPhysicalDisk] = {}

        # Find the most recent configuration for each Volume/Plex/SD by merging
        # all configs from all physical disks.
        for disk in physical_disks:
            if not disk.active:
                # Assuming here that if a disk is marked as inactive, it's
                # configuration is also old and/or possibly inaccurate.
                continue

            disks_by_name[disk.id] = disk

            # For now we only use the first config block. The second config block
            # could be useful/used as a fallback if for instance the first one
            # fails to parse.
            #
            # The self.header.label.last_update timestamp is used to see if this
            # disk contains the latest/newest config of the disks in a set. The
            # disk is ignored if there is no header (which in our case would have
            # raised a ValueError) or if the state is not set to GV_DRIVE_UP (an
            # internal kernel state which we don't have and thus can ignore).
            #
            # Plexes with a non-existing Volume and SDs with a non-existing Plex or
            # Drive (VinumPhysicalDisk) can not be used. They result in an error in
            # the FreeBSD kernel code.
            config_data = parse_vinum_config(disk.config_time, disk.config)

            for config_type, new_items in config_data.items():
                cur_config = config[config_type]

                for new_item in new_items:
                    if cur_item := cur_config.get(new_item.name):
                        if new_item.timestamp > cur_item.timestamp:
                            cur_config[new_item.name] = new_item
                    else:
                        cur_config[new_item.name] = new_item

        # plexes_by_name contains all *active* plexes
        plexes_by_name: ByName[Plex] = dict()

        # plexes_by_volume contains all *active* plexes grouped by the name of
        # the volume they belong to
        plexes_by_volume: DefaultByName[list[Plex]] = defaultdict(list)
        # sds_by_plex_by_volume contains all sds for all *active* plexes
        # grouped by plex and volume name
        sds_by_plex_by_volume: DefaultByName[DefaultByName[list[SD]]] = defaultdict(lambda: defaultdict(list))

        # Check whether the found plexes are viable and filter them out if they
        # are not.
        # Note that if a complete Volume is down, we still try to use it (we don't check on Volume.state).
        for plex in config["plexes"].values():
            if config["volumes"].get(plex.volume):
                # A plex in the DOWN state is probably not fit to use
                if plex.state != PlexState.DOWN:
                    if plex.org != PlexOrg.DISORG:
                        plexes_by_name[plex.name] = plex
                        plexes_by_volume[plex.volume].append(plex)
                    else:
                        log.warning("Plex %r has an unknown organisation, ignoring plex", plex.name)
                else:
                    log.warning("Plex %r is down, ignoring plex", plex.name)
            else:
                log.warning("Unknown volume %r for plex %r, ignoring plex", plex.volume, plex.name)

        # Check whether the found sds are viable and filter them out if they
        # are not.
        for sd in config["sds"].values():
            if plex := plexes_by_name.get(sd.plex):
                sds_by_plex_by_volume[plex.volume][sd.plex].append(sd)
            else:
                log.warning("Unknown or inactive plex %r for sd %r, ignoring sd", sd.plex, sd.name)

        # The construction of the disk_map assumes the configuration
        # information is complete and no sd configs are missing.
        # Note that there are no checks done on the completeness of the sd
        # values, these can theoretically be None due to a corrupt
        # configuration.
        disk_map_by_plex_by_volume: DefaultByName[DefaultByName[list[SD]]] = defaultdict(lambda: defaultdict(list))
        for volume_id, sds_by_plex in sds_by_plex_by_volume.items():
            for plex_id, sds in sds_by_plex.items():
                sds = sorted(sds, key=lambda sd: sd.plexoffset)
                disk_map = {}
                for idx, sd in enumerate(sds):
                    if (
                        sd.state != SDState.DOWN and sd.state != SDState.DEGRADED and sd.state != SDState.INITIALIZING
                    ):  # sd's in these states are probably not fit to use
                        if sd.drive in disks_by_name:
                            disk_map[idx] = (0, disks_by_name[sd.drive])
                        else:
                            log.warning("Physical disk %r for sd %r is missing, ignoring sd", sd.drive, sd.name)
                    else:
                        log.warning("SD %r is not in a usable state: %r, ignoring sd", sd.name, sd.state)
                if disk_map:
                    disk_map_by_plex_by_volume[volume_id][plex_id] = disk_map

        # Each volume represents a separate virtual disk
        virtual_disks = []
        for volume_id, plexes in plexes_by_volume.items():
            volume = config["volumes"][volume_id]
            if plexes:
                # Special case if there is only 1 plex (no mirroring)
                if len(plexes) == 1:
                    plex = plexes[0]
                    disk_map = disk_map_by_plex_by_volume[volume.name][plex.name]
                    if disk_map:
                        sds = sds_by_plex_by_volume[volume.name][plex.name]
                        virtual_disks.append(VinumPlexDisk(volume, plex, sds, disk_map))
                else:
                    plex_map = {}
                    # There is no official order in the plexes. However if they
                    # are named automatically, they have a pseudo order due to
                    # there names being constructed with p0, p1, etc.
                    splexes = sorted(plexes, key=lambda plex: plex.name)
                    for idx, plex in enumerate(splexes):
                        disk_map = disk_map_by_plex_by_volume[volume.name][plex.name]
                        if disk_map:
                            sds = sds_by_plex_by_volume[volume.name][plex.name]
                            plex_disk = VinumPlexDisk(volume, plex, sds, disk_map)
                            plex_map[idx] = (0, plex_disk)

                    if plex_map:
                        virtual_disks.append(VinumMirrorDisk(volume, plexes, plex_map))

            else:
                log.warning("Volume %r has no or only inactive plexes, ignoring volume", volume.name)

        if not virtual_disks:
            raise ValueError(
                "Invalid vinum raid configuration, no volumes found with an active and complete set of disks"
            )

        super().__init__(physical_disks, virtual_disks)


org_to_level = {
    PlexOrg.CONCAT: Level.LINEAR,
    PlexOrg.STRIPED: Level.RAID0,
    PlexOrg.RAID5: Level.RAID5,
}

org_to_layout = {
    PlexOrg.CONCAT: 0,
    PlexOrg.STRIPED: 0,
    PlexOrg.RAID5: Layout.LEFT_ASYMMETRIC,
}


class VinumPlexDisk(VirtualDisk):
    def __init__(
        self,
        volume: Volume,
        plex: Plex,
        sds: list[SD],
        disk_map: DiskMap,
    ):
        self.volume = volume
        self.plex = plex
        self.sds = sds

        if (level := org_to_level.get(plex.org)) is None:
            raise ValueError(f"Plex {plex.name} has an unsupported RAID level: {plex.org}")
        if (layout := org_to_layout.get(plex.org)) is None:
            raise ValueError(f"Plex {plex.name} has an unsupported RAID level: {plex.org}")
        if plex.org == PlexOrg.CONCAT:
            stripe_size = 0  # concatenated disks don't have stripes
        else:
            stripe_size = plex.stripesize

        size = 0
        sd = sds[0]
        if plex.org == PlexOrg.RAID5:
            # SDs in a vinum RAID5 org are required to have equal size, so we
            # don't need to determine the smallest disk.
            size = (len(sds) - 1) * sd.length
        elif plex.org == PlexOrg.STRIPED:
            # SDs in a vinum STRIPED org are required to have equal size, so we
            # can just multiply
            size = len(sds) * sd.length
        else:
            for sd in sds:
                size += sd.length

        super().__init__(
            volume.name.decode(errors="backslashreplace"),
            plex.name.decode(errors="surrogateescape"),
            size,
            level,
            layout,
            stripe_size,
            len(sds),
            disk_map,
        )


class VinumMirrorDisk(VirtualDisk):
    def __init__(
        self,
        volume: Volume,
        plexes: list[Plex],
        plex_map: DiskMap,
    ):
        self.volume = volume
        self.plexes = plexes

        # The VinumConfiguration class will make sure there is at least 1 plex disk.
        _, plex_disk = next(iter(plex_map.values()))
        size = plex_disk.size
        super().__init__(
            volume.name.decode(errors="backslashreplace"),
            volume.name.decode(errors="surrogateescape"),
            size,
            Level.RAID1,
            0,  # simple mirrors don't have a layout
            0,  # simple mirrors don't have a stripe size
            len(plexes),
            plex_map,
        )


class VinumPhysicalDisk(PhysicalDisk):
    """Parse config from an Vinum device.

    Args:
        fh: The file-like object to read config from.
    """

    def __init__(self, fh: BinaryIO):
        self.fh = fh

        fh.seek(c_vinum.GV_HDR_OFFSET)
        self.header = c_vinum.header(fh)

        if self.header.magic in MAGIC_ACTIVE:
            self.active = True
        elif self.header.magic in MAGIC_INACTIVE:
            self.active = False
        else:
            raise ValueError("File-like object is not a Vinum device")

        self.id = self.header.label.name.rstrip(b"\x00")
        self.name = self.header.label.name.rstrip(b"\x00").decode(errors="backslashreplace")

        last_update = self.header.label.last_update
        config_epoch = last_update.sec + last_update.usec * 1e-6
        self.config_time = ts.from_unix(config_epoch)

        size = self.header.label.drive_size
        if not size:
            fh.seek(0, io.SEEK_END)
            size = fh.tell()

        super().__init__(fh, c_vinum.GV_DATA_START, size)

    def _read_config(self, config_offset) -> bytes:
        self.fh.seek(config_offset)
        config = self.fh.read(self.header.config_length)
        return config

    @cached_property
    def config(self) -> bytes:
        return self._read_config(c_vinum.GV_CFG_OFFSET)

    @cached_property
    def config2(self) -> bytes:
        return self._read_config(c_vinum.GV_CFG_OFFSET + c_vinum.GV_CFG_LEN)
