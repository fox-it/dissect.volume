import io
from typing import BinaryIO, Iterator
from uuid import UUID

from dissect import cstruct

from dissect.volume.disk.partition import Partition
from dissect.volume.exceptions import DiskError

bsd_def = """
/* The disk magic number */
#define BSD_MAGIC           0x82564557

#define BSD_NPARTS_MIN      8
#define BSD_NPARTS_MAX      20

#define DISKMAGIC64         0xc4464c59
#define MAXPARTITIONS64     16
#define RESPARTITIONS64     32

/* Size of bootblock area in sector-size neutral bytes */
#define BSD_BOOTBLOCK_SIZE  8192

/* partition containing whole disk */
#define BSD_PART_RAW        2

/* partition normally containing swap */
#define BSD_PART_SWAP       1

/* Drive-type specific data size (in number of 32-bit inegrals) */
#define BSD_NDRIVEDATA      5

/* Number of spare 32-bit integrals following drive-type data */
#define BSD_NSPARE          5

struct partition {                              /* the partition table */
    uint32_t    p_size;                         /* number of sectors in partition */
    uint32_t    p_offset;                       /* starting sector */
    uint32_t    p_fsize;                        /* filesystem basic fragment size */
    uint8_t     p_fstype;                       /* filesystem type, see below */
    uint8_t     p_frag;                         /* filesystem fragments per block */
    uint16_t    p_cpg;                          /* filesystem cylinders per group */
};

struct partition64 {                            /* the partition table */
    uint64_t    p_boffset;                      /* slice relative offset, in bytes */
    uint64_t    p_bsize;                        /* size of partition, in bytes */
    uint8_t     p_fstype;
    uint8_t     p_unused01;                     /* reserved, must be 0 */
    uint8_t     p_unused02;                     /* reserved, must be 0 */
    uint8_t     p_unused03;                     /* reserved, must be 0 */
    uint32_t    p_unused04;                     /* reserved, must be 0 */
    uint32_t    p_unused05;                     /* reserved, must be 0 */
    uint32_t    p_unused06;                     /* reserved, must be 0 */
    char        p_type_uuid[16];                /* mount type as UUID */
    char        p_stor_uuid[16];                /* unique uuid for storage */
};

struct disklabel {
    uint32_t    d_magic;                        /* the magic number */
    uint16_t    d_type;                         /* drive type */
    uint16_t    d_subtype;                      /* controller/d_type specific */
    char        d_typename[16];                 /* type name, e.g. "eagle" */

    char        d_packname[16];                 /* pack identifier */

    /* disk geometry: */
    uint32_t    d_secsize;                      /* # of bytes per sector */
    uint32_t    d_nsectors;                     /* # of data sectors per track */
    uint32_t    d_ntracks;                      /* # of tracks per cylinder */
    uint32_t    d_ncylinders;                   /* # of data cylinders per unit */
    uint32_t    d_secpercyl;                    /* # of data sectors per cylinder */
    uint32_t    d_secperunit;                   /* # of data sectors per unit */

    /*
     * Spares (bad sector replacements) below are not counted in
     * d_nsectors or d_secpercyl.  Spare sectors are assumed to
     * be physical sectors which occupy space at the end of each
     * track and/or cylinder.
     */
    uint16_t    d_sparespertrack;               /* # of spare sectors per track */
    uint16_t    d_sparespercyl;                 /* # of spare sectors per cylinder */
    /*
     * Alternate cylinders include maintenance, replacement, configuration
     * description areas, etc.
     */
    uint32_t    d_acylinders;                   /* # of alt. cylinders per unit */

    /* hardware characteristics: */
    /*
     * d_interleave, d_trackskew and d_cylskew describe perturbations
     * in the media format used to compensate for a slow controller.
     * Interleave is physical sector interleave, set up by the
     * formatter or controller when formatting.  When interleaving is
     * in use, logically adjacent sectors are not physically
     * contiguous, but instead are separated by some number of
     * sectors.  It is specified as the ratio of physical sectors
     * traversed per logical sector.  Thus an interleave of 1:1
     * implies contiguous layout, while 2:1 implies that logical
     * sector 0 is separated by one sector from logical sector 1.
     * d_trackskew is the offset of sector 0 on track N relative to
     * sector 0 on track N-1 on the same cylinder.  Finally, d_cylskew
     * is the offset of sector 0 on cylinder N relative to sector 0
     * on cylinder N-1.
     */
    uint16_t    d_rpm;                          /* rotational speed */
    uint16_t    d_interleave;                   /* hardware sector interleave */
    uint16_t    d_trackskew;                    /* sector 0 skew, per track */
    uint16_t    d_cylskew;                      /* sector 0 skew, per cylinder */
    uint32_t    d_headswitch;                   /* head switch time, usec */
    uint32_t    d_trkseek;                      /* track-to-track seek, usec */
    uint32_t    d_flags;                        /* generic flags */
    uint32_t    d_drivedata[BSD_NDRIVEDATA];    /* drive-type specific data */
    uint32_t    d_spare[BSD_NSPARE];            /* reserved for future use */
    uint32_t    d_magic2;                       /* the magic number (again) */
    uint16_t    d_checksum;                     /* xor of data incl. partitions */

    /* filesystem and partition information: */
    uint16_t    d_npartitions;                  /* number of partitions in following */
    uint32_t    d_bbsize;                       /* size of boot area at sn0, bytes */
    uint32_t    d_sbsize;                       /* max size of fs superblock, bytes */
    // partition   d_partitions[BSD_NPARTS_MIN];   /* actually may be more */
};

struct disklabel64 {
    uint32_t    d_magic;                        /* the magic number */
    uint32_t    d_crc;                          /* crc32() d_magic through last part */
    uint32_t    d_align;                        /* partition alignment requirement */
    uint32_t    d_npartitions;                  /* number of partitions */
    char        d_stor_uuid[16];                /* unique uuid for label */

    uint64_t    d_total_size;                   /* total size incl everything (bytes) */
    uint64_t    d_bbase;                        /* boot area base offset (bytes) */
                                                /* boot area is pbase - bbase */
    uint64_t    d_pbase;                        /* first allocatable offset (bytes) */
    uint64_t    d_pstop;                        /* last allocatable offset+1 (bytes) */
    uint64_t    d_abase;                        /* location of backup copy if not 0 */

    char        d_packname[64];
    char        d_reserved[64];

    /*
     * Note: offsets are relative to the base of the slice, NOT to
     * d_pbase.  Unlike 32 bit disklabels the on-disk format for
     * a 64 bit disklabel remains slice-relative.
     *
     * An uninitialized partition has a p_boffset and p_bsize of 0.
     *
     * If p_fstype is not supported for a live partition it is set
     * to FS_OTHER.  This is typically the case when the filesystem
     * is identified by its uuid.
     */
    //partition64 d_partitions[MAXPARTITIONS64]; /* actually may be more */
};

/* d_type values: */
#define DTYPE_SMD           1                   /* SMD, XSMD; VAX hp/up */
#define DTYPE_MSCP          2                   /* MSCP */
#define DTYPE_DEC           3                   /* other DEC (rk, rl) */
#define DTYPE_SCSI          4                   /* SCSI */
#define DTYPE_ESDI          5                   /* ESDI interface */
#define DTYPE_ST506         6                   /* ST506 etc. */
#define DTYPE_HPIB          7                   /* CS/80 on HP-IB */
#define DTYPE_HPFL          8                   /* HP Fiber-link */
#define DTYPE_FLOPPY        10                  /* floppy */
#define DTYPE_CCD           11                  /* concatenated disk */
#define DTYPE_VINUM         12                  /* vinum volume */
#define DTYPE_DOC2K         13                  /* Msys DiskOnChip */
#define DTYPE_RAID          14                  /* CMU RAIDFrame */
#define DTYPE_JFS2          16                  /* IBM JFS 2 */

/*
 * Filesystem type and version.
 * Used to interpret other filesystem-specific
 * per-partition information.
 */
#define FS_UNUSED           0                   /* unused */
#define FS_SWAP             1                   /* swap */
#define FS_V6               2                   /* Sixth Edition */
#define FS_V7               3                   /* Seventh Edition */
#define FS_SYSV             4                   /* System V */
#define FS_V71K             5                   /* V7 with 1K blocks (4.1, 2.9) */
#define FS_V8               6                   /* Eighth Edition, 4K blocks */
#define FS_BSDFFS           7                   /* 4.2BSD fast filesystem */
#define FS_MSDOS            8                   /* MSDOS filesystem */
#define FS_BSDLFS           9                   /* 4.4BSD log-structured filesystem */
#define FS_OTHER            10                  /* in use, but unknown/unsupported */
#define FS_HPFS             11                  /* OS/2 high-performance filesystem */
#define FS_ISO9660          12                  /* ISO 9660, normally CD-ROM */
#define FS_BOOT             13                  /* partition contains bootstrap */
#define FS_VINUM            14                  /* Vinum drive */
#define FS_RAID             15                  /* RAIDFrame drive */
#define FS_FILECORE         16                  /* Acorn Filecore Filing System */
#define FS_EXT2FS           17                  /* ext2fs */
#define FS_NTFS             18                  /* Windows/NT file system */
#define FS_CCD              20                  /* concatenated disk component */
#define FS_JFS2             21                  /* IBM JFS2 */
#define FS_HAMMER           22                  /* DragonFlyBSD Hammer FS */
#define FS_HAMMER2          23                  /* DragonFlyBSD Hammer2 FS */
#define FS_UDF              24                  /* UDF */
#define FS_EFS              26                  /* SGI's Extent File system */
#define FS_ZFS              27                  /* Sun's ZFS */
#define FS_NANDFS           30                  /* FreeBSD nandfs (NiLFS derived) */

/*
 * flags shared by various drives:
 */
#define D_REMOVABLE         0x01                /* removable media */
#define D_ECC               0x02                /* supports ECC */
#define D_BADSECT           0x04                /* supports bad sector forw. */
#define D_RAMDISK           0x08                /* disk emulator */
#define D_CHAIN             0x10                /* can do back-back transfers */
"""

c_bsd = cstruct.cstruct()
c_bsd.load(bsd_def)

DTYPE_NAMES = {
    0: "unknown",
    c_bsd.DTYPE_SMD: "SMD",
    c_bsd.DTYPE_MSCP: "MSCP",
    c_bsd.DTYPE_DEC: "old DEC",
    c_bsd.DTYPE_SCSI: "SCSI",
    c_bsd.DTYPE_ESDI: "ESDI",
    c_bsd.DTYPE_ST506: "ST506",
    c_bsd.DTYPE_HPIB: "HP-IB",
    c_bsd.DTYPE_HPFL: "HP-FL",
    9: "type 9",
    c_bsd.DTYPE_FLOPPY: "floppy",
    c_bsd.DTYPE_CCD: "CCD",
    c_bsd.DTYPE_VINUM: "Vinum",
    c_bsd.DTYPE_DOC2K: "DOC2K",
    c_bsd.DTYPE_RAID: "Raid",
    c_bsd.DTYPE_JFS2: "jfs",
}

FS_NAMES = {
    c_bsd.FS_UNUSED: "unused",
    c_bsd.FS_SWAP: "swap",
    c_bsd.FS_V6: "Version 6",
    c_bsd.FS_V7: "Version 7",
    c_bsd.FS_SYSV: "System V",
    c_bsd.FS_V71K: "4.1BSD",
    c_bsd.FS_V8: "Eighth Edition",
    c_bsd.FS_BSDFFS: "4.2BSD",
    c_bsd.FS_MSDOS: "MSDOS",
    c_bsd.FS_BSDLFS: "4.4LFS",
    c_bsd.FS_OTHER: "unknown",
    c_bsd.FS_HPFS: "HPFS",
    c_bsd.FS_ISO9660: "ISO9660",
    c_bsd.FS_BOOT: "boot",
    c_bsd.FS_VINUM: "vinum",
    c_bsd.FS_RAID: "raid",
    c_bsd.FS_FILECORE: "Filecore",
    c_bsd.FS_EXT2FS: "EXT2FS",
    c_bsd.FS_NTFS: "NTFS",
    c_bsd.FS_CCD: "ccd",
    c_bsd.FS_JFS2: "jfs",
    c_bsd.FS_HAMMER: "HAMMER",
    c_bsd.FS_HAMMER2: "HAMMER2",
    c_bsd.FS_UDF: "UDF",
    c_bsd.FS_EFS: "EFS",
    c_bsd.FS_ZFS: "ZFS",
    c_bsd.FS_NANDFS: "nandfs",
}


class BSD:
    """BSD disklabel."""

    TYPES = [
        # MBR FreeBSD
        0xA5,
        # MBR OpenBSD
        0xA6,
        # MBR NetBSD
        0xA9,
        # MBR DragonFlyBSD
        0x6C,
        # GPT DragonFlyBSD disklabel32
        UUID("9D087404-1CA5-11DC-8817-01301BB8A9F5").bytes_le,
        # GPT DragonFlyBSD disklabel64
        UUID("3D48CE54-1D16-11DC-8696-01301BB8A9F5").bytes_le,
        # GPT FreeBSD disklabel
        UUID("516E7CB4-6ECF-11D6-8FF8-00022D09712B").bytes_le,
    ]

    def __init__(self, fh: BinaryIO, sector_size: int = 512):
        self.fh = fh
        self.sector_size = sector_size

        # BSD disklabel is usually part of another partition scheme such as MBR or GPT.
        # The actual BSD disklabel info always starts at the second sector.
        offset = fh.seek(sector_size, io.SEEK_CUR)
        data = fh.read(512)
        magic = c_bsd.uint32(data)

        if magic == c_bsd.BSD_MAGIC:
            # disklabel
            self.disklabel = c_bsd.disklabel(data)
            self.type = 32

            if (self.disklabel.d_magic, self.disklabel.d_magic2) != (c_bsd.BSD_MAGIC, c_bsd.BSD_MAGIC):
                raise DiskError(
                    f"Invalid BSD disklabel magic, expected {c_bsd.BSD_MAGIC:#x}, "
                    f"got ({self.disklabel.d_magic:#x}, {self.disklabel.d_magic2:#x})."
                )
        elif magic == c_bsd.DISKMAGIC64:
            # disklabel64
            self.disklabel = c_bsd.disklabel64(data)
            self.type = 64
        else:
            raise DiskError(
                f"Invalid BSD disklabel magic, expected {c_bsd.BSD_MAGIC:#x} or {c_bsd.DISKMAGIC64:#x}, "
                f"got {magic:#x}."
            )

        self._partitions_offset = offset + len(self.disklabel)
        self.partitions = list(self._partitions())

    def _partitions(self) -> Iterator[Partition]:
        if self.type == 32:
            # Get the table offset first
            self.fh.seek(self._partitions_offset + (c_bsd.BSD_PART_RAW * len(c_bsd.partition)))
            table_offset = c_bsd.partition(self.fh).p_offset * self.sector_size
        else:
            table_offset = 0

        self.fh.seek(self._partitions_offset)
        for i in range(self.disklabel.d_npartitions):
            if i == c_bsd.BSD_PART_RAW:
                # Skip internal partition
                continue

            if self.type == 32:
                partition = c_bsd.partition(self.fh)
                if partition.p_fstype == 0:
                    continue

                offset = (partition.p_offset * self.sector_size) - table_offset
                size = partition.p_size * self.sector_size
                guid = None
            elif self.type == 64:
                partition = c_bsd.partition64(self.fh)
                if (partition.p_boffset == 0 and partition.p_bsize) or partition.p_fstype == 0:
                    continue

                offset = partition.p_boffset
                size = partition.p_bsize
                guid = partition.p_stor_uuid

            yield Partition(
                disk=self,
                number=i + 1,  # partitions are 1-indexed
                offset=offset,
                size=size,
                vtype=partition.p_fstype,
                name=None,
                guid=guid,
                vtype_str=FS_NAMES.get(partition.p_fstype, "?"),
                raw=partition,
            )
