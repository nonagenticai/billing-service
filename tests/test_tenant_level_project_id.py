"""``tenancy.tenant_level_project_id`` -- owner ruling 2026-10-07.

Project-less apps (core_app, worker_management) send this as X-Project-Id.
The fixed vector below is the cross-repo contract: callers assert the SAME
literal, so a drift in either implementation is caught on its own side.
"""

from __future__ import annotations

import uuid

import pytest

from tenancy import tenant_level_project_id

VECTOR_IN = "0195f48b-d000-7000-8000-000000000100"
VECTOR_OUT = "00100000-0000-0008-0007-000db84f5910"


def test_fixed_vector() -> None:
    assert tenant_level_project_id(VECTOR_IN) == VECTOR_OUT


def test_fixed_vector_is_the_reversed_hex() -> None:
    assert VECTOR_OUT.replace("-", "") == VECTOR_IN.replace("-", "")[::-1]


def test_round_trip_is_an_involution() -> None:
    assert tenant_level_project_id(VECTOR_OUT) == VECTOR_IN
    for _ in range(50):
        t = str(uuid.uuid4())
        out = tenant_level_project_id(t)
        assert out is not None
        assert out != t
        assert tenant_level_project_id(out) == t


def test_non_canonical_input_is_canonicalised_first() -> None:
    assert tenant_level_project_id(VECTOR_IN.upper()) == VECTOR_OUT
    assert tenant_level_project_id("{" + VECTOR_IN + "}") == VECTOR_OUT
    assert tenant_level_project_id(VECTOR_IN.replace("-", "")) == VECTOR_OUT
    assert tenant_level_project_id(uuid.UUID(VECTOR_IN)) == VECTOR_OUT


@pytest.mark.parametrize("bad", [None, "", "not-a-uuid", "0195f48b", 42])
def test_non_uuid_input_is_none_and_never_raises(bad) -> None:
    assert tenant_level_project_id(bad) is None
