from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from dissect.volume.vinum.config import (
    SD,
    Plex,
    PlexOrg,
    PlexState,
    SDState,
    TokenizeError,
    VinumConfigs,
    Volume,
    VolumeState,
    _parse_plex_config,
    _parse_sd_config,
    _parse_size,
    _parse_volume_config,
    get_char,
    log,
    parse_vinum_config,
    tokenize,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

CONF_TS = datetime.min.replace(tzinfo=timezone.utc)


def test_volume_state() -> None:
    assert VolumeState(b"up") == VolumeState.UP
    assert VolumeState(b"down") == VolumeState.DOWN
    assert VolumeState(b"foo") == VolumeState.DOWN


def test_plex_state() -> None:
    assert PlexState(b"up") == PlexState.UP
    assert PlexState(b"initializing") == PlexState.INITIALIZING
    assert PlexState(b"degraded") == PlexState.DEGRADED
    assert PlexState(b"growable") == PlexState.GROWABLE
    assert PlexState(b"down") == PlexState.DOWN
    assert PlexState(b"foo") == PlexState.DOWN


def test_plex_org() -> None:
    assert PlexOrg(b"concat") == PlexOrg.CONCAT
    assert PlexOrg(b"striped") == PlexOrg.STRIPED
    assert PlexOrg(b"raid5") == PlexOrg.RAID5
    assert PlexOrg(b"?") == PlexOrg.DISORG
    assert PlexOrg(b"foo") == PlexOrg.DISORG


def test_sd_state() -> None:
    assert SDState(b"up") == SDState.UP
    assert SDState(b"initializing") == SDState.INITIALIZING
    assert SDState(b"degraded") == SDState.DEGRADED
    assert SDState(b"growable") == SDState.GROWABLE
    assert SDState(b"down") == SDState.DOWN
    assert SDState(b"foo") == SDState.DOWN


@pytest.mark.parametrize(
    ("bytestr", "size"),
    [
        (b"123", 123),
        (b"123foo", 123),
        (b"123b", 123 * 512),
        (b"123bfoo", 123 * 512),
        (b"123B", 123 * 512),
        (b"123Bfoo", 123 * 512),
        (b"123s", 123 * 512),
        (b"123sfoo", 123 * 512),
        (b"123S", 123 * 512),
        (b"123Sfoo", 123 * 512),
        (b"123k", 123 * 1024),
        (b"123kfoo", 123 * 1024),
        (b"123K", 123 * 1024),
        (b"123Kfoo", 123 * 1024),
        (b"123m", 123 * 1024 * 1024),
        (b"123mfoo", 123 * 1024 * 1024),
        (b"123M", 123 * 1024 * 1024),
        (b"123Mfoo", 123 * 1024 * 1024),
        (b"123g", 123 * 1024 * 1024 * 1024),
        (b"123gfoo", 123 * 1024 * 1024 * 1024),
        (b"123G", 123 * 1024 * 1024 * 1024),
        (b"123Gfoo", 123 * 1024 * 1024 * 1024),
        (b" 123", 0),
        (b"foo", 0),
        (b"", 0),
    ],
)
def test__parse_size(bytestr: bytes, size: int) -> None:
    assert _parse_size(bytestr) == size


@pytest.mark.parametrize(
    ("tokens", "result", "logline"),
    [
        ([b"myname"], Volume(timestamp=CONF_TS, name=b"myname"), ""),
        ([b"myname", b"state", b"up"], Volume(timestamp=CONF_TS, name=b"myname", state=VolumeState(b"up")), ""),
        ([b"state", b"up", b"myname"], Volume(timestamp=CONF_TS, name=b"myname", state=VolumeState(b"up")), ""),
        ([b"myname", b"state"], None, "No value for token b'state', ignoring volume config"),
        ([b"state", b"up"], None, "No name found for volume, ignoring volume config"),
    ],
)
def test__parse_volume_config(
    caplog: pytest.LogCaptureFixture,
    tokens: list[bytes],
    result: Volume | None,
    logline: str,
) -> None:
    caplog.set_level(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    volume = _parse_volume_config(CONF_TS, iter(tokens))

    assert volume == result
    assert logline in caplog.text


@pytest.mark.parametrize(
    ("tokens", "result", "logline"),
    [
        ([b"name", b"myname"], Plex(timestamp=CONF_TS, name=b"myname"), ""),
        ([b"name", b"myname", b"state", b"up"], Plex(timestamp=CONF_TS, name=b"myname", state=PlexState(b"up")), ""),
        ([b"state", b"up", b"name", b"myname"], Plex(timestamp=CONF_TS, name=b"myname", state=PlexState(b"up")), ""),
        (
            [b"name", b"myname", b"org", b"concat", b"vol", b"myvol", b"state", b"up"],
            Plex(
                timestamp=CONF_TS,
                name=b"myname",
                org=PlexOrg(b"concat"),
                volume=b"myvol",
                state=PlexState(b"up"),
            ),
            "",
        ),
        (
            [b"name", b"myname", b"org", b"concat", b"volume", b"myvol", b"state", b"up"],
            Plex(
                timestamp=CONF_TS,
                name=b"myname",
                org=PlexOrg(b"concat"),
                volume=b"myvol",
                state=PlexState(b"up"),
            ),
            "",
        ),
        (
            [b"name", b"myname", b"org", b"raid5", b"123", b"volume", b"myvol", b"state", b"up"],
            Plex(
                timestamp=CONF_TS,
                name=b"myname",
                org=PlexOrg(b"raid5"),
                stripesize=123,
                volume=b"myvol",
                state=PlexState(b"up"),
            ),
            "",
        ),
        (
            [b"name", b"myname", b"org", b"striped", b"123", b"volume", b"myvol", b"state", b"up"],
            Plex(
                timestamp=CONF_TS,
                name=b"myname",
                org=PlexOrg(b"striped"),
                stripesize=123,
                volume=b"myvol",
                state=PlexState(b"up"),
            ),
            "",
        ),
        (
            [b"name", b"myname", b"org", b"striped", b"0"],
            None,
            "Invalid stripesize: 0, ignoring plex config",
        ),
        (
            [b"name", b"myname", b"org", b"striped", b"-123"],
            None,
            "Invalid stripesize: -123, ignoring plex config",
        ),
        ([b"name", b"myname", b"foo"], None, "Unknown token b'foo', ignoring plex config"),
        ([b"name", b"myname", b"state"], None, "No value for token b'state', ignoring plex config"),
    ],
)
def test__parse_plex_config(
    caplog: pytest.LogCaptureFixture,
    tokens: list[bytes],
    result: Volume | None,
    logline: str,
) -> None:
    caplog.set_level(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    plex = _parse_plex_config(CONF_TS, iter(tokens))

    assert plex == result
    assert logline in caplog.text


@pytest.mark.parametrize(
    ("tokens", "result", "logline"),
    [
        ([b"name", b"myname", b"drive", b"mydrive"], SD(timestamp=CONF_TS, drive=b"mydrive", name=b"myname"), ""),
        (
            [b"name", b"myname", b"drive", b"mydrive", b"state", b"up"],
            SD(timestamp=CONF_TS, drive=b"mydrive", name=b"myname", state=SDState(b"up")),
            "",
        ),
        (
            [b"drive", b"mydrive", b"state", b"up", b"name", b"myname"],
            SD(timestamp=CONF_TS, drive=b"mydrive", name=b"myname", state=SDState(b"up")),
            "",
        ),
        (
            [
                b"name",
                b"myname",
                b"drive",
                b"mydrive",
                b"len",
                b"123",
                b"driveoffset",
                b"135680",
                b"plex",
                b"myplex",
                b"plexoffset",
                b"123",
                b"state",
                b"up",
            ],
            SD(
                timestamp=CONF_TS,
                name=b"myname",
                drive=b"mydrive",
                length=123,
                driveoffset=135680,
                plex=b"myplex",
                plexoffset=123,
                state=SDState(b"up"),
            ),
            "",
        ),
        (
            [
                b"name",
                b"myname",
                b"drive",
                b"mydrive",
                b"length",
                b"123",
                b"driveoffset",
                b"135680",
                b"plex",
                b"myplex",
                b"plexoffset",
                b"123",
                b"state",
                b"up",
            ],
            SD(
                timestamp=CONF_TS,
                name=b"myname",
                drive=b"mydrive",
                length=123,
                driveoffset=135680,
                plex=b"myplex",
                plexoffset=123,
                state=SDState(b"up"),
            ),
            "",
        ),
        (
            [b"drive", b"mydrive", b"length", b"-123"],
            SD(
                timestamp=CONF_TS,
                drive=b"mydrive",
                length=-1,
            ),
            "",
        ),
        (
            [b"drive", b"mydrive", b"driveoffset", b"0"],
            SD(
                timestamp=CONF_TS,
                drive=b"mydrive",
                driveoffset=0,
            ),
            "",
        ),
        (
            [b"drive", b"mydrive", b"driveoffset", b"123"],
            None,
            "Invalid driveoffset: 123, ignoring sd config",
        ),
        (
            [b"drive", b"mydrive", b"plexoffset", b"-123"],
            None,
            "Invalid plexoffset: -123, ignoring sd config",
        ),
        ([b"drive", b"mydrive", b"foo"], None, "Unknown token b'foo', ignoring sd config"),
        ([b"drive", b"mydrive", b"state"], None, "No value for token b'state', ignoring sd config"),
        ([b"state", b"up"], None, "No drive found for sd, ignoring sd config"),
    ],
)
def test__parse_sd_config(
    caplog: pytest.LogCaptureFixture, tokens: list[bytes], result: Volume | None, logline: str
) -> None:
    caplog.set_level(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    sd = _parse_sd_config(CONF_TS, iter(tokens))

    assert sd == result
    assert logline in caplog.text


@pytest.mark.parametrize(
    ("bytestr", "idx", "result"),
    [
        (b"abc", -1, b""),
        (b"abc", 0, b"a"),
        (b"abc", 1, b"b"),
        (b"abc", 2, b"c"),
        (b"abc", 3, b""),
    ],
)
def test_get_char(bytestr: bytes, idx: int, result: bytes) -> None:
    assert get_char(bytestr, idx) == result


@pytest.mark.parametrize(
    ("line", "tokens"),
    [
        (b"foo bar", [b"foo", b"bar"]),
        (b"foo\tbar", [b"foo", b"bar"]),
        (b" \t \t foo bar", [b"foo", b"bar"]),
        (b"foo bar \t \t ", [b"foo", b"bar"]),
        (b"foo bar#bla", [b"foo", b"bar#bla"]),
        (b"foo bar #bla", [b"foo", b"bar"]),
        (b"foo bar'", [b"foo", b"bar'"]),
        (b'foo bar"', [b"foo", b'bar"']),
    ],
)
def test_tokenize(line: bytes, tokens: list[bytes]) -> None:
    assert list(tokenize(line)) == tokens


@pytest.mark.parametrize(
    ("line", "idx"),
    [
        (b"foo 'bar", 4),
        (b'foo  "bar', 5),
    ],
)
def test_tokenize_raises(line: bytes, idx: int) -> None:
    with pytest.raises(TokenizeError, match=f"Found quoted token at index {idx}"):
        list(tokenize(line))


def gen_vps(vps_cls: Volume | Plex | SD, arg_name: str, count: int = 0) -> iter[Volume | Plex | SD]:
    vps_name = vps_cls.__name__.lower()

    def vps_iter() -> Iterator[Volume | Plex | SD]:
        idx = 0
        done = False

        kwargs = {}
        while not done:
            kwargs[arg_name] = f"{vps_name}{idx}".encode()
            yield vps_cls(timestamp=CONF_TS, **kwargs)
            idx += 1

            if idx == count:
                done = True

    return vps_iter()


@pytest.mark.parametrize(
    ("config", "expected_config", "expected_logs"),
    [
        (
            b"volume\nplex\nsd",
            {
                "volumes": list(gen_vps(Volume, "name", 1)),
                "plexes": list(gen_vps(Plex, "name", 1)),
                "sds": list(gen_vps(SD, "drive", 1)),
            },
            None,
        ),
        (
            b"volume\x00plex\x00sd",
            {
                "volumes": list(gen_vps(Volume, "name", 1)),
                "plexes": list(gen_vps(Plex, "name", 1)),
                "sds": list(gen_vps(SD, "drive", 1)),
            },
            None,
        ),
        (
            b"volume\nplex\x00sd\n",
            {
                "volumes": list(gen_vps(Volume, "name", 1)),
                "plexes": list(gen_vps(Plex, "name", 1)),
                "sds": list(gen_vps(SD, "drive", 1)),
            },
            None,
        ),
        (
            b"volume\nvolume\x00plex\nplex\x00\nplex\x00\x00sd\00sd\x00\n\n\x00sd\n\x00\n",
            {
                "volumes": list(gen_vps(Volume, "name", 2)),
                "plexes": list(gen_vps(Plex, "name", 3)),
                "sds": list(gen_vps(SD, "drive", 3)),
            },
            None,
        ),
        (
            b"volume\x00foo\nplex\x00sd",
            {
                "volumes": list(gen_vps(Volume, "name", 1)),
                "plexes": [],
                "sds": [],
            },
            "Invalid config line b'foo'",
        ),
    ],
)
def test_parse_vinum_config(
    caplog: pytest.LogCaptureFixture,
    config: bytes,
    expected_config: VinumConfigs,
    expected_logs: None | str,
) -> None:
    caplog.set_level(logging.DEBUG)
    log.setLevel(logging.DEBUG)

    volumes = gen_vps(Volume, "name")
    plexes = gen_vps(Plex, "name")
    sds = gen_vps(SD, "drive")

    with (
        patch("dissect.volume.vinum.config._parse_volume_config", autospec=True, side_effect=volumes),
        patch("dissect.volume.vinum.config._parse_plex_config", autospec=True, side_effect=plexes),
        patch("dissect.volume.vinum.config._parse_sd_config", autospec=True, side_effect=sds),
    ):
        config_data = parse_vinum_config(CONF_TS, config)

    assert config_data == expected_config
    if expected_logs is not None:
        assert expected_logs in caplog.text


def test_parse_vinum_config_token_error(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    log.setLevel(logging.DEBUG)

    with patch("dissect.volume.vinum.config.tokenize", autospec=True, side_effect=TokenizeError("Oops!")):
        parse_vinum_config(CONF_TS, b"b0rk\nb1rk\nb3rk")

    assert "Invalid config line b'b0rk': Oops!" in caplog.text
