from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import TypedDict

from dissect.volume.vinum.c_vinum import c_vinum

log = logging.getLogger(__name__)
log.setLevel(os.getenv("DISSECT_LOG_VINUM", "CRITICAL"))


@dataclass
class Volume:
    """The representation of a Vinum Volume.

    A Vinum Volume defines a single RAID set. One or more Vinum Plexes can be
    part of a Volume.
    """

    timestamp: datetime
    name: bytes
    state: VolumeState | None = None


@dataclass
class Plex:
    """The representation of a Vinum Plex.

    A Vinum Plex can be thought of as one of the individual disks in a mirrored
    array. One or more Vinum SDs can be part of a Plex. The Plex defines the
    type of RAID in which these SDs are organized.
    """

    timestamp: datetime
    name: bytes | None = None
    org: PlexOrg | None = None
    stripesize: int | None = None
    volume: bytes | None = None
    state: PlexState | None = None


@dataclass
class SD:
    """The representation of a Vinum SD.

    A Vinum SD contains information about the actual physical disk and points
    to the device of this disk.
    """

    timestamp: datetime
    drive: bytes
    name: bytes | None = None
    # length is the size in bytes of the data section on disk, so without any
    # vinum headers etc.
    length: int | None = None
    # driveoffset is the start of the data section on disk in bytes.
    driveoffset: int | None = None
    plex: bytes | None = None
    # plexoffset is the offset of the data section of this disk within the plex in
    # bytes, e.g.: the first disk always starts at offset 0, if the size of its
    # data section (SD.length) is 1024b then the plexoffset for the second disk
    # will be 1024.
    plexoffset: int | None = None
    state: SDState | None = None


class ParseError(Exception):
    pass


class BytesDefaultEnum(bytes, Enum):
    @classmethod
    def _missing_(cls, value):
        return cls._default


class VolumeState(BytesDefaultEnum):
    DOWN = auto()
    UP = b"up"

    _default = DOWN


class PlexState(BytesDefaultEnum):
    DOWN = auto()
    UP = b"up"
    INITIALIZING = b"initializing"
    DEGRADED = b"degraded"
    GROWABLE = b"growable"

    _default = DOWN


class PlexOrg(BytesDefaultEnum):
    DISORG = auto()
    CONCAT = b"concat"
    STRIPED = b"striped"
    RAID5 = b"raid5"

    _default = DISORG


class SDState(BytesDefaultEnum):
    DOWN = auto()
    UP = b"up"
    INITIALIZING = b"initializing"
    DEGRADED = b"degraded"
    GROWABLE = b"growable"

    _default = DOWN


def _parse_size(size: bytes) -> int:
    # Only the first byte after the numerals (and optional minus sign) should
    # be considered.
    postfix = size.lstrip(b"-0123456789")
    if postfix:
        numeral = size[: -len(postfix)]
    else:
        numeral = size
    unit = postfix[:1]

    try:
        size = int(numeral)
    except ValueError:
        # If there are no numerals (numeral is empty or the minus sign), the
        # size should be parsed as 0.
        size = 0
    else:
        if unit:
            # Invalid unites should be ignored and size is returned as is.
            if unit in (b"b", b"B", b"s", b"S"):
                size = size * 512  # Yes also for b/B
            elif unit in (b"k", b"K"):
                size = size * 1024
            elif unit in (b"m", b"M"):
                size = size * 1024 * 1024
            elif unit in (b"g", b"G"):
                size = size * 1024 * 1024 * 1024

    return size


def _parse_volume_config(config_time: datetime, tokens: list[bytes]) -> Volume | None:
    volume = None
    name = None
    state = None

    tokens = iter(tokens)
    token = next(tokens, None)
    try:
        while token is not None:
            if token == b"state":
                state = VolumeState(next(tokens))
            else:
                name = token
            token = next(tokens, None)
    except StopIteration:
        log.debug("No value for token %r, ignoring volume config", token)
    else:
        if name is None:
            log.debug("No name found for volume, ignoring volume config")
        else:
            volume = Volume(
                timestamp=config_time,
                name=name,
                state=state,
            )

    return volume


def _parse_plex_config(config_time: datetime, tokens: list[bytes]) -> Plex | None:
    plex = None
    name = None
    org = None
    stripesize = None
    volume = None
    state = None

    tokens = iter(tokens)
    token = next(tokens, None)
    try:
        while token is not None:
            if token == b"name":
                name = next(tokens)
            elif token == b"org":
                org = PlexOrg(next(tokens))
                if org == PlexOrg.RAID5 or org == PlexOrg.STRIPED:
                    stripesize = _parse_size(next(tokens))
                    # the kernel parser only checks on == 0, but < 0 also seems unreasonable
                    if stripesize <= 0:
                        raise ParseError(f"Invalid stripesize: {stripesize}")
            elif token == b"vol" or token == b"volume":
                volume = next(tokens)
            elif token == b"state":
                state = PlexState(next(tokens))
            else:
                raise ParseError(f"Unknown token {token}")

            token = next(tokens, None)

    except (StopIteration, ParseError) as err:
        if isinstance(err, StopIteration):
            log.debug("No value for token %r, ignoring plex config", token)
        else:
            log.debug("%s, ignoring plex config", err)

    else:
        plex = Plex(
            timestamp=config_time,
            name=name,
            org=org,
            stripesize=stripesize,
            volume=volume,
            state=state,
        )

    return plex


def _parse_sd_config(config_time: datetime, tokens: list[bytes]) -> SD | None:
    sd = None
    name = None
    drive = None
    length = None
    driveoffset = None
    plex = None
    plexoffset = None
    state = None

    tokens = iter(tokens)
    token = next(tokens, None)
    try:
        while token is not None:
            if token == b"name":
                name = next(tokens)
            elif token == b"drive":
                drive = next(tokens)
            elif token == b"len" or token == b"length":
                length = _parse_size(next(tokens))
                if length < 0:
                    length = -1
            elif token == b"driveoffset":
                driveoffset = _parse_size(next(tokens))
                if driveoffset != 0 and driveoffset < c_vinum.GV_DATA_START:
                    raise ParseError(f"Invalid driveoffset: {driveoffset}")
            elif token == b"plex":
                plex = next(tokens)
            elif token == b"plexoffset":
                plexoffset = _parse_size(next(tokens))
                if plexoffset < 0:
                    raise ParseError(f"Invalid plexoffset: {plexoffset}")
            elif token == b"state":
                state = SDState(next(tokens))
            else:
                raise ParseError(f"Unknown token {token}")

            token = next(tokens, None)

    except (StopIteration, ParseError) as err:
        if isinstance(err, StopIteration):
            log.debug("No value for token %r, ignoring sd config", token)
        else:
            log.debug("%s, ignoring sd config", err)

    else:
        if drive is None:
            log.debug("No drive found for sd, ignoring sd config")
        else:
            sd = SD(
                timestamp=config_time,
                name=name,
                drive=drive,
                length=length,
                driveoffset=driveoffset,
                plex=plex,
                plexoffset=plexoffset,
                state=state,
            )

    return sd


def get_char(line: bytes, idx: int) -> bytes:
    """Return a single byte bytestring at index ``idx`` in ``line``.

    If the index is outside of the bounaries of ``line``, an empty bytestring
    will be returned.
    """
    char = b""
    if idx >= 0 and idx < len(line):
        char = line[idx : idx + 1]  # this makes sure we get a single byte bytestring
    return char


class TokenizeError(Exception):
    pass


def tokenize(line: bytes) -> iter[bytes]:
    """Yield individual tokens from a vinum config line.

    This token parser is constructed to be equivalent to the token parser used in the
    FreeBSD kernel code. There are a few caveats though:

    - it expects lines to be pre-splitted on newline and null-byte characters
    - it does not attempt to parse quoted tokens, as the code in the kernel parser is
      buggy and will always lead to an error condition (it will mimick the error condition
      though).
    """
    whitespace = {b" ", b"\t"}
    quotes = {b'"', b"'"}
    comment = {b"#"}
    eol = {b""}
    end_of_list = eol.union(comment)
    end_of_token = whitespace.union(eol)

    token = b""
    idx = 0
    while True:
        char = get_char(line, idx)

        while char in whitespace:
            # Remove leading whitespace up to the next token or end_of_list condition
            idx += 1
            char = get_char(line, idx)

        if char in end_of_list:
            # We are at the end of the token list (a comment or end of line).
            break

        if char in quotes:
            # Encountering a quoted token will always lead to an error
            # condition in the (Free)BSD vinum kernel code. This is a bug in
            # that code, which we mimick here.
            raise TokenizeError(f"Found quoted token at index {idx}")

        while char not in end_of_token:
            # Add characters to the token until we encounter a stop condition.
            # Note that comment and quote characters are allowed in a token as
            # long as they are not preceded by whitespace.
            token += char
            idx += 1
            char = get_char(line, idx)

        if token:
            yield token
            token = b""

        idx += 1


class VinumConfigs(TypedDict):
    volumes: list[Volume]
    plexes: list[Plex]
    sds: list[SD]


RE_CONFIG_EOL = re.compile(b"[\x00\n]")


TOKEN_CONFIG_MAP = {
    b"volume": "volumes",
    b"plex": "plexes",
    b"sd": "sds",
}


def parse_vinum_config(config_time: datetime, config: bytes) -> VinumConfigs:
    """Parse the on-disk vinum configuration.

    Parsing forgiveness and strictness is implemented in the same way as in the vinum kernel code:

    Lines with an unknown configuration "type" (not b"volume", b"plex" or b"sd"), are ignored.

    Lines that fail to parse due to:
      - no name present
      - no value present for a token
      - unknown token name
      - a tokenization error

    will fail that line and the subsequent lines (rest of the config) to not being parsed.
    """
    config_data: VinumConfigs = {
        "volumes": [],
        "plexes": [],
        "sds": [],
    }

    for line in RE_CONFIG_EOL.split(config):
        try:
            tokens = tokenize(line)
            token = next(tokens, None)
            if token is None:
                # We encountered a line without tokens (empty, just whitespace or # comments)
                continue
            if token == b"volume":
                parsed_config = _parse_volume_config(config_time, tokens)
            elif token == b"plex":
                parsed_config = _parse_plex_config(config_time, tokens)
            elif token == b"sd":
                parsed_config = _parse_sd_config(config_time, tokens)
            else:
                parsed_config = None
                log.debug("Unknown config type in line: %r, ignoring config line", line)

            if parsed_config:
                config_type = TOKEN_CONFIG_MAP[token]
                config_data[config_type].append(parsed_config)
            else:
                log.debug("Invalid config line %r", line)
                log.debug("Ignoring this line and the rest of the config data")
                break
        except TokenizeError as err:
            log.debug("Invalid config line %r: %s", line, err)
            log.debug("Ignoring this line and the rest of the config data")
            break

    return config_data
