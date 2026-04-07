#!/usr/bin/env bash

set -e

function _create_lvm_inconsistent_sizes {
  local filename="$1"
  dd if=/dev/zero of="$filename" bs=1M count=8

  local lo=$(sudo losetup --partscan --show --find "$filename")
  local uid=$(id -u)

  sudo pvcreate "$lo"
  sudo vgcreate vghelp "$lo"
  sudo lvcreate -l 100%FREE -n lv vghelp
  sudo mkfs.ext4 /dev/vghelp/lv
  sudo mount /dev/vghelp/lv /mnt/tmp-lvm --mkdir
  sudo chmod -R o+rw /mnt/tmp-lvm
  fallocate -l "2448KiB" /mnt/tmp-lvm/large-file
  echo "A small file at the end of the disk" > /mnt/tmp-lvm/small-file

  sudo umount /mnt/tmp-lvm
  sudo lvchange -an vghelp/lv
  sudo vgchange -an vghelp
  sudo losetup -d "$lo"
  # Updae the size at this offset
  printf "00000240: 0000 3000" | xxd -r - "$filename"
  gzip "$filename"
}


_create_lvm_inconsistent_sizes "./lvm/lvm-inconsistent-sizes.bin"

