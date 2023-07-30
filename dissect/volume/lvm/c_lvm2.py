from dissect.cstruct import cstruct

lvm_def = """
struct label_header {
    char    signature[8];
    uint64  sector_number;
    uint32  checksum;
    uint32  data_offset;
    char    type_indicator[8];
};

struct pv_header {
    char    identifier[32];
    uint64  volume_size;
};

struct data_area_descriptor {
    uint64  offset;
    uint64  size;
};

// Metadata area header
struct mda_header {
    uint32  checksum;
    char    signature[16];
    uint32  version;
    uint64  offset;         // Metadata area offset
    uint64  size;           // Metadata area size
};

struct raw_locn {
    uint64  offset;         // Data area offset
    uint64  size;           // Data area size
    uint32  checksum;
    uint32  flags;
};

#define RAW_LOCN_IGNORED    0x00000001
"""

c_lvm = cstruct()
c_lvm.load(lvm_def)

SECTOR_SIZE = 512

STATUS_FLAG_ALLOCATABLE = "ALLOCATABLE"  # pv only
STATUS_FLAG_RESIZEABLE = "RESIZEABLE"  # vg only
STATUS_FLAG_READ = "READ"
STATUS_FLAG_VISIBLE = "VISIBLE"  # lv only
STATUS_FLAG_WRITE = "WRITE"
