# Reference: https://www.snia.org/sites/default/files/SNIA_DDF_Technical_Position_v2.0.pdf

from dissect.cstruct import cstruct

ddf_def = """
#define DDF_HEADER_SIGNATURE            0xDE11DE11
#define DDF_HEADER_CLOSED               0x00
#define DDF_HEADER_OPENED_MASK          0x0F
#define DDF_HEADER_OPEN_ANCHOR          0xFF
#define DDF_HEADER_ANCHOR               0x00
#define DDF_HEADER_PRIMARY              0x01
#define DDF_HEADER_SECONDARY            0x02

struct DDF_Header {
    uint32  Signature;
    uint32  CRC;
    char    DDF_Header_GUID[24];
    char    DDF_rev[8];
    uint32  Sequence_Number;
    uint32  TimeStamp;
    uint8   Open_Flag;
    uint8   Foreign_Flag;
    uint8   Disk_Grouping;
    char    Reserved1[13];
    uint8   Header_Ext[32];
    uint64  Primary_Header_LBA;
    uint64  Secondary_Header_LBA;
    uint8   Header_Type;
    char    Reserved2[3];
    uint32  Workspace_Length;
    uint64  Workspace_LBA;
    uint16  Max_PD_Entries;
    uint16  Max_VD_Entries;
    uint16  Max_Partitions;
    uint16  Configuration_Record_Length;
    uint16  Max_Primary_Element_Entries;
    uint32  Max_Mapped_Block_Entries;                       /* DDF 2.0 */
    char    Reserved3[50];
    uint32  Controller_Data_Section;
    uint32  Controller_Data_Section_Length;
    uint32  Physical_Disk_Records_Section;
    uint32  Physical_Disk_Records_Section_Length;
    uint32  Virtual_Disk_Records_Section;
    uint32  Virtual_Disk_Records_Section_Length;
    uint32  Configuration_Records_Section;
    uint32  Configuration_Records_Section_Length;
    uint32  Physical_Disk_Data_Section;
    uint32  Physical_Disk_Data_Section_Length;
    uint32  BBM_Log_Section;
    uint32  BBM_Log_Section_Length;
    uint32  Diagnostic_Space_Section;
    uint32  Diagnostic_Space_Section_Length;
    uint32  Vendor_Specific_Logs_Section;
    uint32  Vendor_Specific_Logs_Section_Length;
    char    Reserved4[256];
};

#define DDF_CONTROLLER_DATA_SIGNATURE   0xAD111111

struct Controller_Data {
    uint32  Signature;
    uint32  CRC;
    char    Controller_GUID[24];
    char    Controller_Type[8];
    char    Product_ID[16];
    char    Reserved[8];
    char    Vendor_Unique_Controller_Data[448];
};

#define DDF_PDR_SIGNATURE               0x22222222

#define DDF_PDE_GUID_FORCE              (1 << 0)
#define DDF_PDE_PARTICIPATING           (1 << 1)
#define DDF_PDE_GLOBAL_SPARE            (1 << 2)
#define DDF_PDE_CONFIG_SPARE            (1 << 3)
#define DDF_PDE_FOREIGN                 (1 << 4)
#define DDF_PDE_LEGACY                  (1 << 5)
#define DDF_PDE_TYPE_MASK               (0x0f << 12)
#define DDF_PDE_UNKNOWN                 (0x00 << 12)
#define DDF_PDE_SCSI                    (0x01 << 12)
#define DDF_PDE_SAS                     (0x02 << 12)
#define DDF_PDE_SATA                    (0x03 << 12)
#define DDF_PDE_FC                      (0x04 << 12)

#define DDF_PDE_ONLINE                  (1 << 0)
#define DDF_PDE_FAILED                  (1 << 1)
#define DDF_PDE_REBUILD                 (1 << 2)
#define DDF_PDE_TRANSITION              (1 << 3)
#define DDF_PDE_PFA                     (1 << 4)
#define DDF_PDE_UNRECOVERED             (1 << 5)
#define DDF_PDE_MISSING                 (1 << 6)

struct Physical_Disk_Entry {
    char    PD_GUID[24];
    uint32  PD_Reference;
    uint16  PD_Type;
    uint16  PD_State;
    uint64  Configured_Size;
    char    Path_Information[18];
    uint16  Block_Size;                                     /* DDF 2.0 */
    char    Reserved[4];
};

struct Physical_Disk_Records {
    uint32  Signature;
    uint32  CRC;
    uint16  Populated_PDEs;
    uint16  Max_PDE_Supported;
    char    Reserved[52];
    // Variable data
    // Physical_Disk_Entry Physical_Disk_Entries[Populated_PDEs];
};

#define DDF_VD_RECORD_SIGNATURE         0xDDDDDDDD

#define DDF_VDE_SHARED                  (1 << 0)
#define DDF_VDE_ENFORCE_GROUP           (1 << 1)
#define DDF_VDE_UNICODE_NAME            (1 << 2)
#define DDF_VDE_OWNER_ID_VALID          (1 << 3)

#define DDF_VDE_OPTIMAL                 0x00
#define DDF_VDE_DEGRADED                0x01
#define DDF_VDE_DELETED                 0x02
#define DDF_VDE_MISSING                 0x03
#define DDF_VDE_FAILED                  0x04
#define DDF_VDE_PARTIAL                 0x05
#define DDF_VDE_OFFLINE                 0x06
#define DDF_VDE_STATE_MASK              0x07
#define DDF_VDE_MORPH                   (1 << 3)
#define DDF_VDE_DIRTY                   (1 << 4)

#define DDF_VDE_UNINTIALIZED            0x00
#define DDF_VDE_INIT_QUICK              0x01
#define DDF_VDE_INIT_FULL               0x02
#define DDF_VDE_INIT_MASK               0x03
#define DDF_VDE_UACCESS_RW              0x00
#define DDF_VDE_UACCESS_RO              0x80
#define DDF_VDE_UACCESS_BLOCKED         0xc0
#define DDF_VDE_UACCESS_MASK            0xc0

struct Virtual_Disk_Entry {
    char    VD_GUID[24];
    uint16  VD_Number;
    char    Reserved1[2];
    uint32  VD_Type;
    uint8   VD_State;
    uint8   Init_State;
    uint8   Partially_Optimal_Drive_Failures_Remaining;     /* DDF 2.0 */
    char    Reserved2[13];
    char    VD_Name[16];
};

struct Virtual_Disk_Records {
    uint32  Signature;
    uint32  CRC;
    uint16  Populated_VDEs;
    uint16  Max_VDE_Supported;
    char    Reserved[52];
    // Variable data
    // Virtual_Disk_Entry  Virtual_Disk_Entries[Populated_VDEs];
};

#define DDF_VDCR_SIGNATURE              0xEEEEEEEE

/* Primary Raid Level (PRL) */
#define DDF_VDCR_RAID0                  0x00
#define DDF_VDCR_RAID1                  0x01
#define DDF_VDCR_RAID3                  0x03
#define DDF_VDCR_RAID4                  0x04
#define DDF_VDCR_RAID5                  0x05
#define DDF_VDCR_RAID6                  0x06
#define DDF_VDCR_RAID1E                 0x11
#define DDF_VDCR_SINGLE                 0x0F
#define DDF_VDCR_CONCAT                 0x1F
#define DDF_VDCR_RAID5E                 0x15
#define DDF_VDCR_RAID5EE                0x25

/* Raid Level Qualifier (RLQ) */
#define DDF_VDCR_RAID0_SIMPLE           0x00
#define DDF_VDCR_RAID1_SIMPLE           0x00
#define DDF_VDCR_RAID1_MULTI            0x01
#define DDF_VDCR_RAID3_0                0x00
#define DDF_VDCR_RAID3_N                0x01
#define DDF_VDCR_RAID4_0                0x00
#define DDF_VDCR_RAID4_N                0x01
#define DDF_VDCR_RAID5_0_RESTART        0x00
#define DDF_VDCR_RAID6_0_RESTART        0x01
#define DDF_VDCR_RAID5_N_RESTART        0x02
#define DDF_VDCR_RAID5_N_CONTINUE       0x03

#define DDF_VDCR_RAID1E_ADJACENT        0x00
#define DDF_VDCR_RAID1E_OFFSET          0x01

/* Secondary RAID Level (SRL) */
#define DDF_VDCR_2STRIPED               0x00
#define DDF_VDCR_2MIRRORED              0x01
#define DDF_VDCR_2CONCAT                0x02
#define DDF_VDCR_2SPANNED               0x03

#define DDF_VDCR_CACHE_WB               (1 << 0)
#define DDF_VDCR_CACHE_WB_ADAPTIVE      (1 << 1)
#define DDF_VDCR_CACHE_RA               (1 << 2)
#define DDF_VDCR_CACHE_RA_ADAPTIVE      (1 << 3)
#define DDF_VDCR_CACHE_WCACHE_NOBATTERY (1 << 4)
#define DDF_VDCR_CACHE_WCACHE_ALLOW     (1 << 5)
#define DDF_VDCR_CACHE_RCACHE_ALLOW     (1 << 6)
#define DDF_VDCR_CACHE_VENDOR           (1 << 7)

struct VD_Configuration_Record {
    uint32  Signature;
    uint32  CRC;
    char    VD_GUID[24];
    uint32  Timestamp;
    uint32  Sequence_Number;
    char    Reserved1[24];
    uint16  Primary_Element_Count;
    uint8   Strip_Size;
    uint8   Primary_RAID_Level;
    uint8   RAID_Level_Qualifier;
    uint8   Secondary_Element_Count;
    uint8   Secondary_Element_Seq;
    uint8   Secondary_RAID_Level;
    uint64  Block_Count;
    uint64  VD_Size;
    uint16  Block_Size;                                     /* DDF 2.0 */
    uint8   Rotate_Parity_Count;                            /* DDF 2.0 */
    char    Reserved2[5];
    char    Associated_Spares[32];
    uint64  Cache_Policies_And_Parameters;
    uint8   BG_Rate;
    char    Reserved3[3];
    uint8   MDF_Parity_Disks;                               /* DDF 2.0 */
    uint16  MDF_Parity_Generator_Polynomial;                /* DDF 2.0 */
    char    Reserved4[1];
    uint8   MDF_Constant_Generation_Method;                 /* DDF 2.0 */
    char    Reserved5[47];
    char    Reserved6[192];
    char    V0[32];
    char    V1[32];
    char    V2[16];
    char    V3[16];
    char    Vendor_Specific_Scratch_Space[32];
    // Variable data
    // uint32  Physical_Disk_Sequence[0];
    // uint64  Starting_Block[0];
};

#define DDF_VUCR_SIGNATURE              0x88888888

struct VU_Configuration_Record {
    uint32  Signature;
    uint32  CRC;
    char    VD_GUID[24];
};

#define DDF_SA_SIGNATURE                0x55555555

struct Spare_Assignment_Entry {
    char    VD_GUID[24];
    uint16  Secondary_Element;
    char    Reserved[6];
};

#define DDF_SAR_TYPE_DEDICATED          (1 << 0)
#define DDF_SAR_TYPE_REVERTIBLE         (1 << 1)
#define DDF_SAR_TYPE_ACTIVE             (1 << 2)
#define DDF_SAR_TYPE_ENCL_AFFINITY      (1 << 3)

struct Spare_Assignment_Record {
    uint32  Signature;
    uint32  CRC;
    uint32  Timestamp;
    char    Reserved1[7];
    uint8   Spare_Type;
    uint16  Populated_SAEs;
    uint16  Max_SAE_Supported;
    char    Reserved2[8];
    Spare_Assignment_Entry  Spare_Assignment_Entries[Populated_SAEs];
};

#define DDF_PDD_SIGNATURE               0x33333333

#define DDF_PDD_FORCED_REF              0x01
#define DDF_PDD_FORCED_GUID             0x01

struct Physical_Disk_Data {
    uint32  Signature;
    uint32  CRC;
    char    PD_GUID[24];
    uint32  PD_Reference;
    uint8   Forced_Ref_Flag;
    uint8   Forced_PD_GUID_Flag;
    char    Vendor_Specific_Scratch_Space[32];
    char    Reserved[442];
};

#define DDF_BBML_SIGNATURE              0xABADB10C

struct Mapped_Block_Entry {
    uint64  Defective_Block_Start;
    uint32  Spare_Block_Offset;
    uint16  Remapped_Marked_Count;
    char    Reserved[2];
};

struct Bad_Block_Management_Log {
    uint32  Signature;
    uint32  CRC;
    uint32  Entry_Count;
    uint32  Reserved_Spare_Block_Count;
    char    Reserved[8];
    uint64  First_Spare_LBA;
    Mapped_Block_Entry  Mapped_Block_Entries[Entry_Count];
};

#define DDF_VENDOR_LOG_SIGNATURE        0x01DBEEF0

struct Vendor_Specific_Log {
    uint32  Signature;
    uint32  CRC;
    char    Log_Owner[8];
    char    Reserved[16];
};
"""
c_ddf = cstruct(endian=">").load(ddf_def)
