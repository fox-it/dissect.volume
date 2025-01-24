from __future__ import annotations

from dissect.cstruct import cstruct

dm_def = """
/* DM BTree */
enum node_flags {
    INTERNAL_NODE = 1,
    LEAF_NODE = 1 << 1
};

/*
 * Every btree node begins with this structure.  Make sure it's a multiple
 * of 8-bytes in size, otherwise the 64bit keys will be mis-aligned.
 */
struct node_header {
    uint32  csum;
    uint32  flags;
    uint64  blocknr;                /* Block this node is supposed to live in. */

    uint32  nr_entries;
    uint32  max_entries;
    uint32  value_size;
    uint32  padding;
};

struct btree_node {
    struct node_header header;
    // uint64 keys[];
};

/* Thin */
#define THIN_SUPERBLOCK_MAGIC 27022010
#define THIN_SUPERBLOCK_LOCATION 0
#define THIN_VERSION 2
#define SECTOR_TO_BLOCK_SHIFT 3

/* This should be plenty */
#define SPACE_MAP_ROOT_SIZE 128

/*
 * Little endian on-disk superblock and device details.
 */
struct thin_disk_superblock {
    uint32  csum;                   /* Checksum of superblock except for this field. */
    uint32  flags;
    uint64  blocknr;                /* This block number, dm_block_t. */

    uint8   uuid[16];
    uint64  magic;
    uint32  version;
    uint32  time;

    uint64  trans_id;

    /*
     * Root held by userspace transactions.
     */
    uint64  held_root;

    uint8   data_space_map_root[SPACE_MAP_ROOT_SIZE];
    uint8   metadata_space_map_root[SPACE_MAP_ROOT_SIZE];

    /*
     * 2-level btree mapping (dev_id, (dev block, time)) -> data block
     */
    uint64  data_mapping_root;

    /*
     * Device detail root mapping dev_id -> device_details
     */
    uint64  device_details_root;

    uint32  data_block_size;        /* In 512-byte sectors. */

    uint32  metadata_block_size;    /* In 512-byte sectors. */
    uint64  metadata_nr_blocks;

    uint32  compat_flags;
    uint32  compat_ro_flags;
    uint32  incompat_flags;
};

struct disk_device_details {
    uint64 mapped_blocks;
    uint64 transaction_id;          /* When created. */
    uint32 creation_time;
    uint32 snapshotted_time;
};
"""

c_dm = cstruct().load(dm_def)

SECTOR_SIZE = 512
