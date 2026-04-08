#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly TESTS_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly OUT_DIR="${TESTS_ROOT}/_data/lvm"

log()  { printf '[INFO] %s\n' "$*" >&2; }
warn() { printf '[WARN] %s\n' "$*" >&2; }
error()  { printf '[ERROR] %s\n' "$*" >&2; }
have() { command -v "$1" >/dev/null 2>&1; }

require_tools() {
    local -a tools=(pvcreate mkfs.ext4 dd xxd)
    local missing=0

    for t in "${tools[@]}"; do
        if ! have "$t"; then
            error "Missing required tool: $t"
            missing=1
        fi
    done

    if (( missing != 0 )); then
        error "One or more required tools are missing. Aborting."
        exit 1
    fi
}

function _create_lvm_inconsistent_sizes {
  local filename="$1"
  local uid=$(id -u)
  dd if=/dev/zero of="$filename" bs=1M count=8 >&/dev/null
  log "Created block device $filename"

  local lo=$(sudo losetup --partscan --show --find "$filename")

  log "Created loopback device $lo"
  sudo pvcreate "$lo" >/dev/null
  sudo vgcreate vghelp "$lo" >/dev/null
  sudo lvcreate -l 100%FREE -n lv vghelp >/dev/null
  sudo mkfs.ext4 /dev/vghelp/lv >&/dev/null

  log "Created lvm2 structure"


  sudo mount /dev/vghelp/lv /mnt/tmp-lvm --mkdir
  sudo chmod -R o+rw /mnt/tmp-lvm
  fallocate -l "2448KiB" /mnt/tmp-lvm/large-file
  echo "A small file at the end of the disk" > /mnt/tmp-lvm/small-file

  log "Created files on disk"

  log "Cleanup lvm devices"
  sudo umount /mnt/tmp-lvm
  sudo lvchange -an vghelp/lv
  sudo vgchange -an vghelp >/dev/null
  sudo losetup -d "$lo"


  log "Update size of the pv header"
  # Update the size at this offset
  printf "00000240: 0000 3000" | xxd -r - "$filename"

  # Compress the resulting file
  gzip "$filename"
}


main() {
    require_tools

    mkdir -p "${OUT_DIR}"

    _create_lvm_inconsistent_sizes "${OUT_DIR}/lvm-inconsistent-sizes.bin"

    log "All test cases generated under: ${OUT_DIR}"
}

main
