from __future__ import annotations

from dissect.cstruct import cstruct

# Structures are copied from:
# https://github.com/freebsd/freebsd-src/blob/f21a6a6a8fc59393173d9a537ed8cebbdbd6343c/sys/geom/vinum/geom_vinum_var.h

vinum_def = """
struct timeval {
    uint64 sec;
    uint64 usec;
};

typedef uint64 off_t;

/*
 * Slice header
 *
 * Vinum drives start with this structure:
 *
 *                                             Sector
 * |--------------------------------------|
 * |   PDP-11 memorial boot block         |      0
 * |--------------------------------------|
 * |   Disk label, maybe                  |      1
 * |--------------------------------------|
 * |   Slice definition  (vinum_hdr)      |      8
 * |--------------------------------------|
 * |                                      |
 * |   Configuration info, first copy     |      9
 * |                                      |
 * |--------------------------------------|
 * |                                      |
 * |   Configuration info, second copy    |      9 + size of config
 * |                                      |
 * |--------------------------------------|
 *

/* Sizes and offsets of our information. */
#define GV_HDR_OFFSET       4096    /* Offset of vinum header. */
#define GV_HDR_LEN          512     /* Size of vinum header. */
#define GV_CFG_OFFSET       4608    /* Offset of first config copy. */
#define GV_CFG_LEN          65536   /* Size of config copy. */

/* This is where the actual data starts. */
#define GV_DATA_START       (GV_CFG_LEN * 2 + GV_CFG_OFFSET)
/* #define GV_DATA_START       (GV_CFG_LEN * 2 + GV_HDR_LEN) */

#define GV_MAXDRIVENAME     32      /* Maximum length of a device name. */

/*
 * hostname is 256 bytes long, but we don't need to shlep multiple copies in
 * vinum.  We use the host name just to identify this system, and 32 bytes
 * should be ample for that purpose.
 */

#define GV_HOSTNAME_LEN     32
struct gv_label {
    char            sysname[GV_HOSTNAME_LEN];   /* System name at creation time. */
    char            name[GV_MAXDRIVENAME];      /* Our name of the drive. */
    struct timeval  date_of_birth;              /* The time it was created ... */
    struct timeval  last_update;                /* ... and the time of last update. */
    off_t           drive_size;                 /* Total size incl. headers. */
};

#define GV_OLD_MAGIC        0x494E2056494E4F00LL
#define GV_OLD_NOMAGIC      0x4E4F2056494E4F00LL
#define GV_MAGIC            0x56494E554D2D3100LL
#define GV_NOMAGIC          0x56494E554D2D2D00LL

/* The 'header' of each valid vinum drive. */
struct gv_hdr {
    uint64_t        magic;
    uint64_t        config_length;
    struct gv_label label;
} header;
"""

c_vinum = cstruct(endian=">").load(vinum_def)

# Not really needed as this size is hardcoded in the various GV_*_OFFSET and related values
SECTOR_SIZE = 512

MAGIC_ACTIVE = {c_vinum.GV_OLD_MAGIC, c_vinum.GV_MAGIC}
MAGIC_INACTIVE = {c_vinum.GV_OLD_NOMAGIC, c_vinum.GV_NOMAGIC}
