from dissect.cstruct import cstruct

lvm_def = """
struct label_header {
    char    id[8];
    uint64  sector;
    uint32  crc;
    uint32  offset;
    char    type[8];
};

struct pv_header {
    char    pv_uuid[32];
    uint64  device_size;
};

struct disk_locn {
    uint64  offset;         /* Offset in bytes to start sector */
    uint64  size;           /* Bytes */
};

// Metadata area header
struct mda_header {
    uint32  checksum;       /* Checksum of rest of mda_header */
    char    magic[16];      /* To aid scans for metadata */
    uint32  version;
    uint64  start;          /* Absolute start byte of mda_header */
    uint64  size;           /* Size of metadata area */
};

struct raw_locn {
    uint64  offset;         /* Offset in bytes to start sector */
    uint64  size;           /* Bytes */
    uint32  checksum;
    uint32  flags;
};

#define RAW_LOCN_IGNORED    0x00000001
"""

c_lvm = cstruct().load(lvm_def)

SECTOR_SIZE = 512

LABEL_SCAN_SECTORS = 4

STATUS_FLAG_ALLOCATABLE = "ALLOCATABLE"  # pv only
STATUS_FLAG_RESIZEABLE = "RESIZEABLE"  # vg only
STATUS_FLAG_READ = "READ"
STATUS_FLAG_VISIBLE = "VISIBLE"  # lv only
STATUS_FLAG_WRITE = "WRITE"
