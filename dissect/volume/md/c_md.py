from dissect.cstruct import cstruct

md_def = """
#define MD_RESERVED_BYTES               (64 * 1024)
#define MD_RESERVED_SECTORS             (MD_RESERVED_BYTES / 512)

/*
 * RAID superblock.
 *
 * The RAID superblock maintains some statistics on each RAID configuration.
 * Each real device in the RAID set contains it near the end of the device.
 * Some of the ideas are copied from the ext2fs implementation.
 *
 * We currently use 4096 bytes as follows:
 *
 *  word offset     function
 *
 *     0  -    31   Constant generic RAID device information.
 *    32  -    63   Generic state information.
 *    64  -   127   Personality specific information.
 *   128  -   511   12 32-words descriptors of the disks in the raid set.
 *   512  -   911   Reserved.
 *   912  -  1023   Disk specific descriptor.
 */

#define MD_SB_GENERIC_CONSTANT_WORDS    32
#define MD_SB_GENERIC_STATE_WORDS       32
#define MD_SB_GENERIC_WORDS             (MD_SB_GENERIC_CONSTANT_WORDS + MD_SB_GENERIC_STATE_WORDS)
#define MD_SB_PERSONALITY_WORDS         64
#define MD_SB_DESCRIPTOR_WORDS          32
#define MD_SB_DISKS                     27
#define MD_SB_DISKS_WORDS               (MD_SB_DISKS * MD_SB_DESCRIPTOR_WORDS)
#define MD_SB_RESERVED_WORDS            (1024 - MD_SB_GENERIC_WORDS - MD_SB_PERSONALITY_WORDS - MD_SB_DISKS_WORDS - MD_SB_DESCRIPTOR_WORDS)

/*
 * Device "operational" state bits
 */
#define MD_DISK_FAULTY                  0           /* disk is faulty / operational */
#define MD_DISK_ACTIVE                  1           /* disk is running or spare disk */
#define MD_DISK_SYNC                    2           /* disk is in sync with the raid set */
#define MD_DISK_REMOVED                 3           /* disk is in sync with the raid set */
#define MD_DISK_CLUSTER_ADD             4           /* Initiate a disk add across the cluster
                                                     * For clustered enviroments only.
                                                     */
#define MD_DISK_CANDIDATE               5           /* disk is added as spare (local) until confirmed
                                                     * For clustered enviroments only.
                                                     */
#define MD_DISK_FAILFAST                10          /* Send REQ_FAILFAST if there are multiple
                                                     * devices available - and don't try to
                                                     * correct read errors.
                                                     */

#define MD_DISK_WRITEMOSTLY             9           /* disk is "write-mostly" is RAID1 config.
                                                     * read requests will only be sent here in
                                                     * dire need
                                                     */
#define MD_DISK_JOURNAL                 18          /* disk is used as the write journal in RAID-5/6 */

#define MD_DISK_ROLE_SPARE              0xffff
#define MD_DISK_ROLE_FAULTY             0xfffe
#define MD_DISK_ROLE_JOURNAL            0xfffd
#define MD_DISK_ROLE_MAX                0xff00      /* max value of regular disk role */

typedef struct mdp_device_descriptor_s {
    uint32  number;                                 /* 0 Device number in the entire set */
    uint32  major;                                  /* 1 Device major number */
    uint32  minor;                                  /* 2 Device minor number */
    uint32  raid_disk;                              /* 3 The role of the device in the raid set */
    uint32  state;                                  /* 4 Operational state */
    uint32  reserved[MD_SB_DESCRIPTOR_WORDS - 5];
} mdp_disk_t;

#define MD_SB_MAGIC                     0xa92b4efc

typedef struct mdp_superblock_s {
    /*
     * Constant generic information
     */
    uint32  md_magic;                               /*  0 MD identifier */
    uint32  major_version;                          /*  1 major version to which the set conforms */
    uint32  minor_version;                          /*  2 minor version ... */
    uint32  patch_version;                          /*  3 patchlevel version ... */
    uint32  gvalid_words;                           /*  4 Number of used words in this section */
    char    set_uuid0[4];                           /*  5 Raid set identifier */
    uint32  ctime;                                  /*  6 Creation time */
    uint32  level;                                  /*  7 Raid personality */
    uint32  size;                                   /*  8 Apparent size of each individual disk */
    uint32  nr_disks;                               /*  9 total disks in the raid set */
    uint32  raid_disks;                             /* 10 disks in a fully functional raid set */
    uint32  md_minor;                               /* 11 preferred MD minor device number */
    uint32  not_persistent;                         /* 12 does it have a persistent superblock */
    char    set_uuid1[4];                           /* 13 Raid set identifier #2 */
    char    set_uuid2[4];                           /* 14 Raid set identifier #3 */
    char    set_uuid3[4];                           /* 15 Raid set identifier #4 */
    uint32  gstate_creserved[MD_SB_GENERIC_CONSTANT_WORDS - 16];

    /*
     * Generic state information
     */
    uint32  utime;                                  /*  0 Superblock update time */
    uint32  state;                                  /*  1 State bits (clean, ...) */
    uint32  active_disks;                           /*  2 Number of currently active disks */
    uint32  working_disks;                          /*  3 Number of working disks */
    uint32  failed_disks;                           /*  4 Number of failed disks */
    uint32  spare_disks;                            /*  5 Number of spare disks */
    uint32  sb_csum;                                /*  6 checksum of the whole superblock */
    uint32  events_lo;                              /*  7 low-order of superblock update count */
    uint32  events_hi;                              /*  8 high-order of superblock update count */
    uint32  cp_events_lo;                           /*  9 low-order of checkpoint update count */
    uint32  cp_events_hi;                           /* 10 high-order of checkpoint update count */
    uint32  recovery_cp;                            /* 11 recovery checkpoint sector count */
    /* There are only valid for minor_version > 90 */
    uint64  reshape_position;                       /* 12,13 next address in array-space for reshape */
    uint32  new_level;                              /* 14 new level we are reshaping to */
    uint32  delta_disks;                            /* 15 change in number of raid_disks */
    uint32  new_layout;                             /* 16 new layout */
    uint32  new_chunk;                              /* 17 new chunk size (bytes) */
    uint32  gstate_sreserved[MD_SB_GENERIC_STATE_WORDS - 18];

    /*
     * Personality information
     */
    uint32  layout;                                 /*  0 the array's physical layout */
    uint32  chunk_size;                             /*  1 chunk size in bytes */
    uint32  root_pv;                                /*  2 LV root PV */
    uint32  root_block;                             /*  3 LV root block */
    uint32  pstate_reserved[MD_SB_PERSONALITY_WORDS - 4];

    /*
     * Disks information
     */
    mdp_disk_t  disks[MD_SB_DISKS];

    /*
     * Reserved
     */
    uint32  reserved[MD_SB_RESERVED_WORDS];

    /*
     * Active descriptor
     */
    mdp_disk_t  this_disk;

} mdp_super_t;

#define WriteMostly1                    1           /* mask for writemostly flag in above */
#define FailFast1                       2           /* Should avoid retries and fixups and just fail */

/*
 * The version-1 superblock :
 * All numeric fields are little-endian.
 *
 * total size: 256 bytes plus 2 per device.
 *  1K allows 384 devices.
 */
struct mdp_superblock_1 {
    /* constant array information - 128 bytes */
    uint32  magic;                                  /* MD_SB_MAGIC: 0xa92b4efc - little endian */
    uint32  major_version;                          /* 1 */
    uint32  feature_map;                            /* bit 0 set if 'bitmap_offset' is meaningful */
    uint32  pad0;                                   /* always set to 0 when writing */

    char    set_uuid[16];                           /* user-space generated. */
    char    set_name[32];                           /* set and interpreted by user-space */

    uint64  ctime;                                  /* lo 40 bits are seconds, top 24 are microseconds or 0*/
    int32   level;                                  /* -4 (multipath), -1 (linear), 0,1,4,5 */
    uint32  layout;                                 /* only for raid5 and raid10 currently */
    uint64  size;                                   /* used size of component devices, in 512byte sectors */

    uint32  chunksize;                              /* in 512byte sectors */
    uint32  raid_disks;
    union {
        uint32  bitmap_offset;                      /* sectors after start of superblock that bitmap starts
                                                     * NOTE: signed, so bitmap can be before superblock
                                                     * only meaningful of feature_map[0] is set.
                                                     */

        /* only meaningful when feature_map[MD_FEATURE_PPL] is set */
        struct {
            uint16  offset;                         /* sectors from start of superblock that ppl starts (signed) */
            uint16  size;                           /* ppl size in sectors */
        } ppl;
    };

    /* These are only valid with feature bit '4' */
    uint32  new_level;                              /* new level we are reshaping to */
    uint64  reshape_position;                       /* next address in array-space for reshape */
    uint32  delta_disks;                            /* change in number of raid_disks */
    uint32  new_layout;                             /* new layout */
    uint32  new_chunk;                              /* new chunk size (512byte sectors) */
    uint32  new_offset;                             /* signed number to add to data_offset in new
                                                     * layout. 0 == no-change. This can be
                                                     * different on each device in the array.
                                                     */

    /* constant this-device information - 64 bytes */
    uint64  data_offset;                            /* sector start of data, often 0 */
    uint64  data_size;                              /* sectors in this device that can be used for data */
    uint64  super_offset;                           /* sector start of this superblock */
    union {
        uint64  recovery_offset;                    /* sectors before this offset (from data_offset) have been recovered */
        uint64  journal_tail;                       /* journal tail of journal device (from data_offset) */
    };
    uint32  dev_number;                             /* permanent identifier of this device - not role in raid */
    uint32  cnt_corrected_read;                     /* number of read errors that were corrected by re-writing */
    char    device_uuid[16];                        /* user-space setable, ignored by kernel */
    uint8   devflags;                               /* per-device flags. Only two defined... */

    /* Bad block log.  If there are any bad blocks the feature flag is set.
     * If offset and size are non-zero, that space is reserved and available
     */
    uint8   bblog_shift;                            /* shift from sectors to block size */
    uint16  bblog_size;                             /* number of sectors reserved for list */
    int32   bblog_offset;                           /* sector offset from superblock to bblog, signed - not unsigned */

    /* array state information - 64 bytes */
    uint64  utime;                                  /* 40 bits second, 24 bits microseconds */
    uint64  events;                                 /* incremented when superblock updated */
    uint64  resync_offset;                          /* data before this offset (from data_offset) known to be in sync */
    uint32  sb_csum;                                /* checksum up to devs[max_dev] */
    uint32  max_dev;                                /* size of devs[] array to consider */
    char    pad3[64-32];                            /* set to 0 when writing */

    /* device state information. Indexed by dev_number.
     * 2 bytes per device
     * Note there are no per-device state flags. State information is rolled
     * into the 'roles' value.  If a device is spare or faulty, then it doesn't
     * have a meaningful role.
     */
    uint16  dev_roles[max_dev];                     /* role in array, or 0xffff for a spare, or 0xfffe for faulty */
};

/* non-obvious values for 'level' */
#define LEVEL_MULTIPATH                 (-4)
#define LEVEL_LINEAR                    (-1)
#define LEVEL_FAULTY                    (-5)

/*
 * Our supported algorithms
 */
#define ALGORITHM_LEFT_ASYMMETRIC       0           /* Rotating Parity N with Data Restart */
#define ALGORITHM_RIGHT_ASYMMETRIC      1           /* Rotating Parity 0 with Data Restart */
#define ALGORITHM_LEFT_SYMMETRIC        2           /* Rotating Parity N with Data Continuation */
#define ALGORITHM_RIGHT_SYMMETRIC       3           /* Rotating Parity 0 with Data Continuation */

/* Define non-rotating (raid4) algorithms.  These allow
 * conversion of raid4 to raid5.
 */
#define ALGORITHM_PARITY_0              4           /* P or P,Q are initial devices */
#define ALGORITHM_PARITY_N              5           /* P or P,Q are final devices. */

/* DDF RAID6 layouts differ from md/raid6 layouts in two ways.
 * Firstly, the exact positioning of the parity block is slightly
 * different between the 'LEFT_*' modes of md and the "_N_*" modes
 * of DDF.
 * Secondly, or order of datablocks over which the Q syndrome is computed
 * is different.
 * Consequently we have different layouts for DDF/raid6 than md/raid6.
 * These layouts are from the DDFv1.2 spec.
 * Interestingly DDFv1.2-Errata-A does not specify N_CONTINUE but
 * leaves RLQ=3 as 'Vendor Specific'
 */

#define ALGORITHM_ROTATING_ZERO_RESTART 8           /* DDF PRL=6 RLQ=1 */
#define ALGORITHM_ROTATING_N_RESTART    9           /* DDF PRL=6 RLQ=2 */
#define ALGORITHM_ROTATING_N_CONTINUE   10          /* DDF PRL=6 RLQ=3 */

/* For every RAID5 algorithm we define a RAID6 algorithm
 * with exactly the same layout for data and parity, and
 * with the Q block always on the last device (N-1).
 * This allows trivial conversion from RAID5 to RAID6
 */
#define ALGORITHM_LEFT_ASYMMETRIC_6     16
#define ALGORITHM_RIGHT_ASYMMETRIC_6    17
#define ALGORITHM_LEFT_SYMMETRIC_6      18
#define ALGORITHM_RIGHT_SYMMETRIC_6     19
#define ALGORITHM_PARITY_0_6            20
#define ALGORITHM_PARITY_N_6            ALGORITHM_PARITY_N
"""  # noqa: E501

c_md = cstruct()
c_md.load(md_def)

SECTOR_SIZE = 512
