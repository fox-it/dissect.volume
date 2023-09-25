from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import BinaryIO, Optional, Union

from dissect.util import ts

from dissect.volume.exceptions import LVM2Error
from dissect.volume.lvm.metadata import VolumeGroup
from dissect.volume.lvm.physical import LVM2Device

log = logging.getLogger(__name__)
log.setLevel(os.getenv("DISSECT_LOG_LVM", "CRITICAL"))


class LVM2:
    """Logical Volume Manager"""

    def __init__(self, fh: Union[list[Union[BinaryIO, LVM2Device]], Union[BinaryIO, LVM2Device]]):
        self.fh = [fh] if not isinstance(fh, list) else fh
        if not self.fh:
            raise ValueError("At least one file-like object is required")

        devices = [LVM2Device(fh) if not isinstance(fh, LVM2Device) else fh for fh in self.fh]
        self.devices = {device.id: device for device in devices}

        self.metadata = devices[0].metadata
        self.contents: str = self.metadata["contents"]
        self.version: int = self.metadata["version"]
        self.description: Optional[str] = self.metadata.get("description")
        self.creation_host: Optional[str] = self.metadata.get("creation_host")
        self.creation_time: Optional[datetime] = None
        if creation_time := self.metadata.get("creation_time"):
            self.creation_time: Optional[datetime] = ts.from_unix(creation_time)

        vg = [VolumeGroup.from_dict(value, name=key) for key, value in self.metadata.items() if isinstance(value, dict)]
        if len(vg) != 1:
            raise LVM2Error(f"Found multiple volume groups, expected only one: {vg}")
        self.volume_group = vg[0]
        self.volume_group.attach(self.devices)

    def __repr__(self) -> str:
        return f"<LVM2 vg={self.vg}>"

    @property
    def vg(self) -> VolumeGroup:
        return self.volume_group
