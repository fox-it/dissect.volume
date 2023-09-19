from __future__ import annotations

import io
from typing import BinaryIO, Union

from dissect.cstruct.types import Instance, Structure
from dissect.util import ts

from dissect.volume.ddf.c_ddf import DEFAULT_SECTOR_SIZE, c_ddf
from dissect.volume.exceptions import DDFError
from dissect.volume.raid.raid import RAID, Configuration, PhysicalDisk, VirtualDisk
from dissect.volume.raid.stream import Layout, Level

DECADE = 3600 * 24 * (365 * 10 + 2)


class DDF(RAID):
    def __init__(self, fh: list[Union[BinaryIO, DDFPhysicalDisk]], sector_size: int = DEFAULT_SECTOR_SIZE):
        fhs = [fh] if not isinstance(fh, list) else fh
        self.disks = [DDFPhysicalDisk(f, sector_size) if not isinstance(f, DDFPhysicalDisk) else f for f in fhs]
        self.sector_size = sector_size

        config_map = {}
        for pd in self.disks:
            config_map.setdefault(pd.anchor.DDF_Header_GUID, []).append(pd)

        super().__init__([DDFConfiguration(disks) for disks in config_map.values()])


class DDFConfiguration(Configuration):
    def __init__(self, fh: list[Union[BinaryIO, PhysicalDisk]], sector_size: int = DEFAULT_SECTOR_SIZE):
        fhs = [fh] if not isinstance(fh, list) else fh
        self.disks = [DDFPhysicalDisk(f, sector_size) if not isinstance(f, DDFPhysicalDisk) else f for f in fhs]
        self.sector_size = sector_size

        pd_map: dict[int, DDFPhysicalDisk] = {}
        vde_map: dict[bytes, VirtualDiskRecord] = {}
        vdcr_map: dict[bytes, VirtualDiskConfigurationRecord] = {}
        vdcr_uniq: dict[tuple[bytes, int], VirtualDiskConfigurationRecord] = {}

        for pd in self.disks:
            pd_map[pd.reference] = pd
            vde_map.update({vde.guid: vde for vde in pd.virtual_disk_records})
            vdcr_map.update({vdcr.guid: vdcr for vdcr in pd.virtual_disk_configuration_records})
            vdcr_uniq.update(
                {(vdcr.guid, vdcr.secondary_element_seq): vdcr for vdcr in pd.virtual_disk_configuration_records}
            )

        vd_map: dict = {}
        for vdcr in vdcr_uniq.values():
            vd_pd_map = vd_map.setdefault(vdcr.guid, {})

            count = vdcr.primary_element_count
            sec = 0 if vdcr.secondary_element_count == 1 else vdcr.secondary_element_seq

            i = 0
            for starting_block, pds in zip(vdcr.starting_block, vdcr.physical_disk_sequence):
                if matched_pd := pd_map.get(pds):
                    vd_pd_map[sec * count + i] = (starting_block, matched_pd)

                if pds != 0xFFFFFFFF:
                    i += 1

        virtual_disks = [DDFVirtualDisk(vdcr_map[guid], vde_map[guid], vd_map[guid]) for guid in vd_map.keys()]
        super().__init__(self.disks, virtual_disks)


class DDFVirtualDisk(VirtualDisk):
    def __init__(
        self,
        vdcr: VirtualDiskConfigurationRecord,
        vdr: VirtualDiskRecord,
        disks: dict[int, tuple[int, DDFPhysicalDisk]],
    ):
        self.vdcr = vdcr
        self.vdr = vdr
        self.disks = disks

        if (block_size := self.vdcr.block_size) == 0xFFFF:
            block_size = list(self.disks.values())[0][1].block_size

        level, layout, num_disks = _convert_raid_layout(
            vdcr.primary_raid_level,
            vdcr.raid_level_qualifier,
            vdcr.secondary_raid_level,
            vdcr.primary_element_count,
            vdcr.secondary_element_count,
        )

        super().__init__(
            self.vdr.name,
            self.vdr.guid,
            self.vdcr.size * block_size,
            level,
            layout,
            (2**self.vdcr.strip_size) * block_size,
            num_disks,
            disks,
        )

    @property
    def virtual_disk_configuration_record(self) -> VirtualDiskConfigurationRecord:
        return self.vdcr

    @property
    def virtual_disk_record(self) -> VirtualDiskRecord:
        return self.vdr


class DDFPhysicalDisk(PhysicalDisk):
    def __init__(self, fh: BinaryIO, sector_size: int = DEFAULT_SECTOR_SIZE):
        self.sector_size = sector_size

        fh.seek(-sector_size, io.SEEK_END)
        self.anchor = c_ddf.DDF_Header(fh)
        if self.anchor.Signature != c_ddf.DDF_HEADER_SIGNATURE:
            raise DDFError(
                "Invalid DDF anchor header. "
                f"Signature: {self.anchor.Signature:#010x}, expected {c_ddf.DDF_HEADER_SIGNATURE:#x}"
            )

        self.primary_header_offset = self.anchor.Primary_Header_LBA * sector_size
        self.secondary_header_offset = None

        fh.seek(self.primary_header_offset)
        self.primary_header = c_ddf.DDF_Header(fh)
        self.secondary_header = None
        if self.anchor.Secondary_Header_LBA != 0xFFFFFFFFFFFFFFFF:
            self.secondary_header_offset = self.anchor.Secondary_Header_LBA * sector_size
            fh.seek(self.secondary_header_offset)
            self.secondary_header = c_ddf.DDF_Header(fh)

        self.active_header_offset = self.primary_header_offset

        header_offset = self.active_header_offset
        fh.seek(header_offset + (self.anchor.Controller_Data_Section * sector_size))
        self.controller_data = ControllerData(fh)

        physical_disk_records_offset = header_offset + (self.anchor.Physical_Disk_Records_Section * sector_size)
        self.physical_disk_records = _read_physical_disk_records(fh, physical_disk_records_offset)

        virtual_disk_records_offset = header_offset + (self.anchor.Virtual_Disk_Records_Section * sector_size)
        self.virtual_disk_records = _read_virtual_disk_records(fh, virtual_disk_records_offset)

        configuration_records_offset = header_offset + (self.anchor.Configuration_Records_Section * sector_size)
        self.virtual_disk_configuration_records = _read_virtual_disk_configuration_records(
            fh,
            configuration_records_offset,
            self.anchor.Configuration_Records_Section_Length // self.anchor.Configuration_Record_Length,
            self.anchor.Configuration_Record_Length * sector_size,
            self.anchor.Max_Primary_Element_Entries,
        )

        fh.seek(header_offset + (self.anchor.Physical_Disk_Data_Section * sector_size))
        self.physical_disk_data = PhysicalDiskData(fh)

        self.guid = self.physical_disk_data.guid
        self.reference = self.physical_disk_data.reference

        my_pdr = next((pdr for pdr in self.physical_disk_records if pdr.reference == self.reference), None)
        if not my_pdr:
            raise DDFError(f"Physical disk does not have a physical disk record: {self.reference:#010x}")

        self.type = my_pdr.type
        self.state = my_pdr.state
        self.path_information = my_pdr.path_information
        self.block_size = my_pdr.block_size if my_pdr.block_size != 0xFFFF else self.sector_size

        super().__init__(fh, 0, my_pdr.size * self.block_size)

    def __repr__(self) -> str:
        return f"<DDFPhysicalDisk guid={self.guid} reference={self.reference:#08x} size={self.size}>"


class ControllerData:
    def __init__(self, fh: BinaryIO):
        self.header = _read_section_header(fh, c_ddf.Controller_Data, c_ddf.DDF_CONTROLLER_DATA_SIGNATURE)
        self.guid = self.header.Controller_GUID
        self.type = self.header.Controller_Type

    def __repr__(self) -> str:
        return f"<ControllerData guid={self.guid} type={self.type}>"


class PhysicalDiskData:
    def __init__(self, fh: BinaryIO):
        self.header = _read_section_header(fh, c_ddf.Physical_Disk_Data, c_ddf.DDF_PDD_SIGNATURE)

        self.guid = self.header.PD_GUID
        self.reference = self.header.PD_Reference

    def __repr__(self) -> str:
        return f"<PhysicalDiskData guid={self.guid} reference={self.reference:#x}>"


class PhysicalDiskRecord:
    def __init__(self, fh: BinaryIO):
        self.header = c_ddf.Physical_Disk_Entry(fh)

        self.guid = self.header.PD_GUID
        self.reference = self.header.PD_Reference
        self.type = self.header.PD_Type
        self.state = self.header.PD_State
        self.size = self.header.Configured_Size
        self.path_information = self.header.Path_Information
        self.block_size = self.header.Block_Size

    def __repr__(self) -> str:
        return f"<PhysicalDiskRecord guid={self.guid} reference={self.reference:#x}>"


class VirtualDiskRecord:
    def __init__(self, fh: BinaryIO):
        self.header = c_ddf.Virtual_Disk_Entry(fh)

        self.guid = self.header.VD_GUID
        self.number = self.header.VD_Number
        self.type = self.header.VD_Type
        self.state = self.header.VD_State
        self.init_state = self.header.Init_State
        name = self.header.VD_Name.split(b"\x00")[0]
        self.name = name.decode("utf-8") if self.type & 0x02 else name.decode()

    def __repr__(self) -> str:
        return f"<VirtualDiskRecord guid={self.guid} number={self.number} type={self.type:#x} name={self.name!r}>"


class VirtualDiskConfigurationRecord:
    def __init__(self, fh: BinaryIO, num_entries: int):
        self.header = c_ddf.VD_Configuration_Record(fh)

        self.guid = self.header.VD_GUID
        self.guid_timestamp = ts.from_unix(DECADE + int.from_bytes(self.guid[16:20], "little"))
        self.timestamp = ts.from_unix(DECADE + self.header.Timestamp)
        self.sequence_number = self.header.Sequence_Number
        self.primary_element_count = self.header.Primary_Element_Count
        self.strip_size = self.header.Strip_Size
        self.primary_raid_level = self.header.Primary_RAID_Level
        self.raid_level_qualifier = self.header.RAID_Level_Qualifier
        self.secondary_element_count = self.header.Secondary_Element_Count
        self.secondary_element_seq = self.header.Secondary_Element_Seq
        self.secondary_raid_level = self.header.Secondary_RAID_Level
        self.size = self.header.VD_Size
        self.block_size = self.header.Block_Size
        self.rotate_parity_count = self.header.Rotate_Parity_Count

        self.physical_disk_sequence = c_ddf.uint32[num_entries](fh)[: self.primary_element_count]
        self.starting_block = c_ddf.uint64[num_entries](fh)[: self.primary_element_count]

    def __repr__(self) -> str:
        return f"<VirtualDiskConfigurationRecord guid={self.guid}>"


def _read_section_header(fh: BinaryIO, structure: Structure, signature: int) -> Instance:
    obj = structure(fh)
    if obj.Signature != signature:
        raise DDFError(f"Invalid {structure.name} header. Signature: {obj.Signature:#010x}, expected {signature:#x}")
    return obj


def _read_physical_disk_records(fh: BinaryIO, offset: int) -> list[PhysicalDiskRecord]:
    fh.seek(offset)
    header = _read_section_header(fh, c_ddf.Physical_Disk_Records, c_ddf.DDF_PDR_SIGNATURE)
    return [PhysicalDiskRecord(fh) for _ in range(header.Populated_PDEs)]


def _read_virtual_disk_records(fh: BinaryIO, offset: int) -> list[VirtualDiskRecord]:
    fh.seek(offset)
    header = _read_section_header(fh, c_ddf.Virtual_Disk_Records, c_ddf.DDF_VD_RECORD_SIGNATURE)
    return [VirtualDiskRecord(fh) for _ in range(header.Populated_VDEs)]


def _read_virtual_disk_configuration_records(
    fh: BinaryIO, offset: int, count: int, size: int, num_entries: int
) -> list[VirtualDiskConfigurationRecord]:
    result = []
    for _ in range(count):
        fh.seek(offset)
        obj = VirtualDiskConfigurationRecord(fh, num_entries)

        if obj.header.Signature != c_ddf.DDF_VDCR_SIGNATURE:
            continue

        result.append(obj)
        offset += size

    return result


def _convert_raid_layout(prl: int, rlq: int, srl: int, pec: int, sec: int) -> tuple[Level, Layout, int]:
    """Convert DDF RAID layout to a layout we can work with.

    Args:
        prl: The DDF Primary RAID Level.
        rlq: The DDF RAID Level Qualifier.
        srl: The DDF Secondary RAID Level.
        pec: The DDF Primary Element Count
        sec: The DDF Secondary Element Count.
    """
    level = None
    layout = None
    num_disks = pec

    if sec > 1:
        if prl != c_ddf.DDF_VDCR_RAID1 or srl not in (c_ddf.DDF_VDCR_2STRIPED, c_ddf.DDF_VDCR_2SPANNED):
            raise ValueError(f"Unsupported secondary RAID level: {srl}")

        if num_disks == 2 and rlq == c_ddf.DDF_VDCR_RAID1_SIMPLE:
            layout = 0x102  # 2 near copies, 1 far copy
        elif num_disks == 3 and rlq == c_ddf.DDF_VDCR_RAID1_MULTI:
            layout = 0x103  # 3 near copies, 1 far copy

        num_disks *= sec
        level = Level.RAID10

    else:
        if prl == c_ddf.DDF_VDCR_CONCAT:
            level = Level.LINEAR

        elif prl == c_ddf.DDF_VDCR_RAID0:
            if rlq != c_ddf.DDF_VDCR_RAID0_SIMPLE:
                raise ValueError(f"Unsupported DDF RAID0 layout: ({prl}, {rlq})")

            level = Level.RAID0

        elif prl == c_ddf.DDF_VDCR_RAID1:
            if not (
                (rlq == c_ddf.DDF_VDCR_RAID1_SIMPLE and num_disks == 2)
                or (rlq == c_ddf.DDF_VDCR_RAID1_MULTI and num_disks == 3)
            ):
                raise ValueError(f"Unsupported DDF RAID1 layout: ({prl}, {rlq})")

            level = Level.RAID1

        elif prl == c_ddf.DDF_VDCR_RAID1E:
            if rlq == c_ddf.DDF_VDCR_RAID1E_ADJACENT:
                layout = 0x102
            elif rlq == c_ddf.DDF_VDCR_RAID1E_OFFSET:
                layout = 0x201
            else:
                raise ValueError(f"Unsupported DDF RAID1E layout: ({prl}, {rlq})")

            level = Level.RAID10

        elif prl == c_ddf.DDF_VDCR_RAID4:
            if rlq != c_ddf.DDF_VDCR_RAID4_N:
                raise ValueError(f"Unsupported DDF RAID4 layout: ({prl}, {rlq})")

            level = Level.RAID4

        elif prl == c_ddf.DDF_VDCR_RAID5:
            if rlq == c_ddf.DDF_VDCR_RAID5_N_RESTART:
                layout = Layout.LEFT_ASYMMETRIC
            elif rlq == c_ddf.DDF_VDCR_RAID5_0_RESTART:
                layout = Layout.RIGHT_ASYMMETRIC
            elif rlq == c_ddf.DDF_VDCR_RAID5_N_CONTINUE:
                layout = Layout.LEFT_SYMMETRIC
            else:
                raise ValueError(f"Unsupported DDF RAID5 layout: ({prl}, {rlq})")

            level = Level.RAID5

        elif prl == c_ddf.DDF_VDCR_RAID6:
            if rlq == c_ddf.DDF_VDCR_RAID5_N_RESTART:
                layout = Layout.ROTATING_N_RESTART
            elif rlq == c_ddf.DDF_VDCR_RAID6_0_RESTART:
                layout = Layout.ROTATING_ZERO_RESTART
            elif rlq == c_ddf.DDF_VDCR_RAID5_N_CONTINUE:
                layout = Layout.ROTATING_N_CONTINUE
            else:
                raise ValueError(f"Unsupported DDF RAID6 layout: ({prl}, {rlq})")

            level = Level.RAID6
        else:
            raise ValueError(f"Unsupported DDF RAID layout: ({prl}, {rlq})")

    return level, layout, num_disks
