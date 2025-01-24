# http://www.ntfs.com/ldm.htm
#
# This file is still a WIP

from __future__ import annotations

import logging
import os

from dissect.cstruct import cstruct

log = logging.getLogger(__name__)
log.setLevel(os.getenv("DISSECT_LOG_LDM", "CRITICAL"))

ldm_def = """
enum TOCREGION_FLAGS : uint16 {
    NOT_EXIST   = 0x01,
    NEW         = 0x02,
    DELETE      = 0x04,
    DISABLED    = 0x08
};

enum VMDB_STATE : uint16 {
    CONSISTENT  = 0x01,
    CREATION    = 0x02,
    DELETION    = 0x03
};

enum VBLK_STATE : uint16 {
    CONSISTENT  = 0x01,
    DELETION    = 0x02,
    INACTIVE    = 0x03
};

enum VBLK_RECORD_TYPE : uint16 {
    COMPONENT   = 0x32,
    PARTITION   = 0x33,
    DISK3       = 0x34,
    DISK_GROUP3 = 0x35,
    DISK4       = 0x44,
    DISK_GROUP4 = 0x45,
    VOLUME      = 0x51
};

struct PRIVHEAD {
    char        magic[8];
    uint32      checksum;
    uint16      version_major;
    uint16      version_minor;
    uint64      last_update;
    uint64      update_sequence_number;
    uint64      primary_private_header_offset;
    uint64      secondary_private_header_offset;
    char        disk_id[64];
    char        host_id[64];
    char        disk_group_id[64];
    char        disk_group_name[31];
    uint32      sector_size;
    uint32      flags;
    uint16      public_region_slice_number;
    uint16      private_region_slice_number;
    uint64      public_region_offset;
    uint64      public_region_size;
    uint64      primary_toc_offset;
    uint64      secondary_toc_offset;
    uint32      num_configs;
    uint32      num_logs;
    uint64      config_size;
    uint64      log_size;
    char        disk_signature;
    char        disk_set_id[16];
    char        disk_set_id_repeat[16];
};

struct TOCREGION {
    char            name[8];
    TOCREGION_FLAGS flags;
    uint64          offset;
    uint64          size;
    uint16          unk;
    uint16          copy_number;
    char            zeroes[4];
};

struct TOCBLOCK {
    char        magic[8];
    uint32      sequence_number1;
    char        zeroes1[4];
    uint32      sequence_number2;
    char        zeroes2[16];
    TOCREGION   config;
    TOCREGION   log;
};

struct VMDB {
    char        magic[4];
    uint32      last_vblk_sequence;
    uint32      vblk_size;
    uint32      first_vblk;
    VMDB_STATE  update_status;
    uint16      version_major;
    uint16      version_minor;
    char        disk_group_name[31];
    char        disk_group_guid[64];
    uint64      commit_sequence;
    uint64      pending_sequence;
    uint32      num_committed_volume_vblk;
    uint32      num_committed_component_vblk;
    uint32      num_committed_partition_vblk;
    uint32      num_committed_disk_vblk;
    uint32      unused0;
    uint32      unused1;
    uint32      unused2;
    uint32      num_pending_volume_vblk;
    uint32      num_pending_component_vblk;
    uint32      num_pending_partition_vblk;
    uint32      num_pending_disk_vblk;
    uint32      unused3;
    uint32      unused4;
    uint32      unused5;
    uint64      last_access_time;
};

struct VBLK_HEADER {
    char        magic[4];
    uint32      sequence_number;
    uint32      group_number;
    uint16      record_number;
    uint16      num_records;
};

struct VBLK_COMPONENT {
    uint16      update_status;
    uint16      record_type;
    uint32      data_length;
    uint8       object_id_len;
    char        object_id[objectid_len];
    uint8       name_len;
    char        name[name_len];
    uint8       volume_state_len;
    char        volume_state[volume_state_len];
    uint8       component_type;
    char        zeroes0[4];
    uint8       num_children_len;
    char        num_children[num_children_len];
    uint64      log_commit_id;
    char        zeroes1[8];
    uint8       parent_id_len;
    char        parent_id[parent_id_len];
    char        zeroes2;
    uint8       strip_size_len;
    char        strip_size[strip_size_len];
    uint8       num_columns_len;
    char        num_columns[num_columns_len];
};

struct VBLK_PARTITION {
    uint16      update_status;
    uint16      record_type;
    uint32      data_length;
    uint8       object_id_len;
    char        object_id[objectid_len];
    uint8       name_len;
    char        name[name_len];
    char        zeroes[4];
    uint64      log_commit_id;
    uint64      start;
    uint64      volume_offset;
    uint8       size_len;
    char        size[size_len];
    uint8       parent_id_len;
    char        parent_id[parent_id_len];
    uint8       disk_object_id_len;
    char        disk_object_id[disk_object_id_len];
    uint8       component_part_index_len;
    char        component_part_index[component_part_index_len];
};

struct VBLK_DISK3 {
    uint16      update_status;
    uint16      record_type;
    uint32      data_length;
    uint8       object_id_len;
    char        object_id[objectid_len];
    uint8       name_len;
    char        name[name_len];
    uint8       disk_id_len;
    char        disk_id[disk_id_len];
    uint8       alternate_name_len;
    char        alternate_name[alternate_name_len];
    char        zeroes[4];
    uint64      log_commit_id;
};

struct VBLK_DISK4 {
    uint16      update_status;
    uint16      record_type;
    uint32      data_length;
    uint8       object_id_len;
    char        object_id[objectid_len];
    uint8       name_len;
    char        name[name_len];
    char        disk_id1[16];
    char        disk_id2[16];
    char        zeroes[3];
    uint16      id;
    uint64      log_commit_id;
};

struct VBLK_DISK_GROUP3 {
    uint16      update_status;
    uint16      record_type;
    uint32      data_length;
    uint8       object_id_len;
    char        object_id[objectid_len];
    uint8       name_len;
    char        name[name_len];
    uint8       disk_group_id_len;
    char        disk_group_id[disk_id_len];
    char        zeroes[4];
    uint64      log_commit_id;
    // 0xffffffff
    // 0xffffffff
};

struct VBLK_DISK_GROUP4 {
    uint16      update_status;
    uint16      record_type;
    uint32      data_length;
    uint8       object_id_len;
    char        object_id[objectid_len];
    uint8       name_len;
    char        name[name_len];
    char        disk_group_id[16];
    char        disk_set_id[16];
    char        zeroes[4];
    uint64      log_commit_id;
    // 0xffffffff
    // 0xffffffff
};

struct VBLK_VOLUME {

};

struct KLOG {

};
"""

c_ldm = cstruct(endian=">").load(ldm_def)
