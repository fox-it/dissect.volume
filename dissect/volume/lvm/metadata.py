import ast
import re
from collections import OrderedDict

from dissect.volume.exceptions import LVM2Error


class Metadata:
    def __init__(self, volume_group, global_parameters=None, raw=None):
        if global_parameters is None:
            global_parameters = {}
        self.volume_group = volume_group
        self.globals = global_parameters
        self.raw = raw

    def __getattr__(self, k):
        if k in self.globals:
            return self.globals[k]

        return object.__getattribute__(self, k)

    def __repr__(self):
        return f"<Metadata vg={self.volume_group} globals={self.globals}>"

    @property
    def vg(self):
        return self.volume_group

    @classmethod
    def parse(cls, metadata):
        root = OrderedDict()
        curr = root
        parents = {}
        global_params = OrderedDict()

        s = re.sub(r"(#[^\"]+?)$", "", metadata, flags=re.M)

        it = iter(s.split("\n"))
        for line in it:
            line = line.strip()
            if not line or line[0] == "#":
                continue

            if line[-1] == "{":
                name = line[:-1].strip()

                child = OrderedDict()
                parent = curr
                parents[id(child)] = parent
                parent[name] = child
                curr = child
                continue

            if line[-1] == "}":
                curr = parents[id(curr)]
                continue

            k, v = keyvaluepair(line, it)
            if curr is root:
                global_params[k] = v
            else:
                curr[k] = v

        if len(root) > 1:
            raise LVM2Error("Too many volume groups in metadata")

        vg_name = list(root.keys())[0]
        vg_dict = root[vg_name]

        return cls(VolumeGroupMeta.from_dict(vg_name, vg_dict), global_params, metadata)


class VolumeGroupMeta:
    def __init__(self, name, attrs, physical_volumes, logical_volumes):
        self.name = name
        self.attrs = attrs
        self.physical_volumes = physical_volumes
        self.logical_volumes = logical_volumes

    def __getattr__(self, k):
        if k in self.attrs:
            return self.attrs[k]

        return object.__getattribute__(self, k)

    def __repr__(self):
        return f"<VolumeGroupMeta name={self.name} pv={self.physical_volumes} lv={self.logical_volumes}>"

    @property
    def pv(self):
        return self.physical_volumes

    @property
    def lv(self):
        return self.logical_volumes

    @classmethod
    def from_dict(cls, name, vg_dict):
        pv = [PhysicalVolumeMeta.from_dict(k, v) for k, v in vg_dict["physical_volumes"].items()]
        lv = [LogicalVolumeMeta.from_dict(k, v) for k, v in vg_dict["logical_volumes"].items()]
        attrs = {k: v for k, v in vg_dict.items() if k not in ["physical_volumes", "logical_volumes"]}

        return cls(name, attrs, pv, lv)


class LogicalVolumeMeta:
    def __init__(self, name, segments, attrs):
        self.name = name
        self.segments = segments
        self.attrs = attrs

    def __getattr__(self, k):
        if k in self.attrs:
            return self.attrs[k]

        return object.__getattribute__(self, k)

    def __repr__(self):
        return f"<LogicalVolumeMeta name={self.name}>"

    @classmethod
    def from_dict(cls, name, lv_dict):
        segments = []

        for k, v in lv_dict.items():
            if k.startswith("segment") and k != "segment_count":
                if v.get("type") == "snapshot":
                    continue
                segments.append(SegmentMeta.from_dict(k, v))

        return cls(name, segments, lv_dict)


class PhysicalVolumeMeta:
    def __init__(self, name, attrs):
        self.name = name
        self.attrs = attrs

    def __getattr__(self, k):
        if k in self.attrs:
            return self.attrs[k]

        return object.__getattribute__(self, k)

    def __repr__(self):
        return f"<PhysicalVolumeMeta name={self.name} id={self.id}>"

    @classmethod
    def from_dict(cls, name, pv_dict):
        return cls(name, pv_dict)


class SegmentMeta:
    def __init__(self, name, stripes, attrs):
        self.name = name
        self.stripes = stripes
        self.attrs = attrs

    def __getattr__(self, k):
        if k in self.attrs:
            return self.attrs[k]

        return object.__getattribute__(self, k)

    def __repr__(self):
        return f"<SegmentMeta name={self.name} stripes={self.stripes}>"

    @classmethod
    def from_dict(cls, name, segment_dict):
        stripes = []
        stripe_meta = segment_dict["stripes"]
        for i in range(0, len(stripe_meta), 2):
            stripes.append(StripeMeta(*stripe_meta[i : i + 2]))

        return cls(name, stripes, segment_dict)


class StripeMeta:
    def __init__(self, physical_volume_name, extent_offset):
        self.physical_volume_name = physical_volume_name
        self.extent_offset = extent_offset

    def __repr__(self):
        return f"<StripeMeta pv={self.physical_volume_name} offset={self.extent_offset}>"


def keyvaluepair(s, it):
    k, v = s.strip().split("=", 1)
    k = k.strip()
    v = v.strip()

    if v[0] == "[":
        if v[-1] != "]":
            values = [v]
            for line_iter in it:
                values.append(line_iter)
                if line_iter[-1] == "]":
                    break
            v = "".join(values)

    v = ast.literal_eval(v)

    return k, v
