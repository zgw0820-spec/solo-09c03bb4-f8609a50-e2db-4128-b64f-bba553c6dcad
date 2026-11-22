"""Unit tests for source validation and per-version constraints."""

from __future__ import annotations

import pytest

from app.table import SubscriptionTable


def table(mapping, lo=16000, hi=16999):
    return SubscriptionTable.build(mapping, port_min=lo, port_max=hi)


def test_groups_ports_by_source():
    t = table({16001: "127.0.0.11", 16002: "127.0.0.11", 16003: "127.0.0.12"})
    assert t.ports_for("127.0.0.11") == (16001, 16002)
    assert t.ports_for("127.0.0.12") == (16003,)
    assert t.ports_for("127.0.0.13") == ()
    assert t.sources() == frozenset({"127.0.0.11", "127.0.0.12"})


def test_rejects_more_than_two_sources_per_version():
    with pytest.raises(ValueError, match="at most 2"):
        table(
            {
                16001: "127.0.0.11",
                16002: "127.0.0.12",
                16003: "127.0.0.13",
            }
        )


def test_two_sources_are_allowed():
    t = table({16001: "127.0.0.11", 16002: "127.0.0.12"})
    assert t.sources() == frozenset({"127.0.0.11", "127.0.0.12"})


def test_rejects_port_outside_classroom_range():
    with pytest.raises(ValueError, match="outside the configured range"):
        table({15000: "127.0.0.11"})


def test_rejects_invalid_source():
    with pytest.raises(ValueError, match="invalid IPv4"):
        table({16001: "not-an-ip"})
    with pytest.raises(ValueError, match="multicast"):
        table({16001: "239.1.1.1"})
    with pytest.raises(ValueError):
        table({16001: "0.0.0.0"})


def test_requires_at_least_one_classroom():
    with pytest.raises(ValueError, match="at least one classroom"):
        table({})


def test_table_is_immutable_snapshot():
    t = table({16001: "127.0.0.11"})
    with pytest.raises(TypeError):
        t.entries[16002] = "127.0.0.12"  # type: ignore[index]
    with pytest.raises(TypeError):
        t.by_source["127.0.0.11"] = ()  # type: ignore[index]


def test_equality_is_by_content():
    a = table({16001: "127.0.0.11", 16002: "127.0.0.12"})
    b = table({16002: "127.0.0.12", 16001: "127.0.0.11"})
    assert a == b
    assert a.as_dict() == b.as_dict()
