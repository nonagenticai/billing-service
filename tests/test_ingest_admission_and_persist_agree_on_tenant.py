"""Admission and persistence must agree about who a cost event belongs to.

THE CONTRACT, stated once
-------------------------
Two independent pieces of code read the SAME wire field ``tenant_id``:

* ``validate_cost_event`` (``services/_cost_schema``) decides ADMISSION. A value
  it cannot attribute is refused up front and dead-lettered, precisely so that
  unbillable spend is quarantined instead of silently sunk.
* ``_normalize_tenant_id`` (``services/_parsers``) decides PERSISTENCE, and its
  return drives real money: ``cost_flusher`` keys the ``token_usage`` rollup and
  ``debit_budget`` off it.

The contract between them is a biconditional, and it is the only thing that makes
``cost_flusher``'s dropped ``if tid == UNATTRIBUTED_TENANT_ID: continue`` guard
safe — that removal rests on the stated invariant "what reaches here always
resolves to a REAL tenant uuid":

    validate_cost_event admits a value  <=>  _normalize_tenant_id resolves it to
                                             a real (non-nil) tenant

THE DEFECT THIS PINS
--------------------
The two disagreed, silently and in the expensive direction. Admission strips
(``str(raw_tenant).strip()``, and ``canonical_tenant_id`` strips again);
persistence did not. ``looks_like_credential`` returns True for ANY " ", "\t" or
"\n" anywhere in the value, so a padded-but-valid tenant took the leaked-token
branch::

    tenant_id = " 019de842-…"   ->  admitted as a real tenant
                                ->  persisted as 00000000-…  (default-anon)

A real customer's spend was therefore booked to the nil tenant: not invoiced, not
enforced against their budget, and invisible in the DLQ because it was never
dead-lettered. Billing is append-only with no refund or void path, so every such
row is permanently unbillable.

WHY THIS IS WRITTEN AS A PROPERTY, NOT A CASE LIST
--------------------------------------------------
The bug was not a missing branch in either function — each was internally
consistent. It was an AGREEMENT failure between them. Asserting the biconditional
over a corpus catches the next divergence whichever side moves, which a table of
expected outputs pinned to one implementation would not.
"""

from __future__ import annotations

import pytest

from services._cost_schema import validate_cost_event
from services._parsers import _normalize_tenant_id, original_tenant_label
from tenancy import NIL_TENANT_ID

#: The licences registry as measured 2026-08-25, hard-coded for the same reason
#: ``test_tenant_id_resolution_boundary`` hard-codes it: importing the map under
#: test would make this agree with any future wrong edit to that map.
N8N_DEV_UUID = "019de842-c92a-7e37-8fdd-eb0d8c5396f9"

#: Everything except ``tenant_id``, kept minimal and valid so the only thing that
#: can move a verdict is the tenant field itself.
_BASE_EVENT = {
    "provider": "deepseek",
    "service_key": "inference-router",  # pragma: allowlist secret
    "model": "deepseek-v4-pro",
    "cost_usd": "1.23",
}

#: Whitespace shapes a real emitter produces: a shell-sourced env var with a
#: trailing newline, a header copied with a leading space, a CRLF-terminated
#: value read off the wire.
_PADDINGS = [" {}", "{} ", "{}\n", "{}\r\n", "\t{}", "  {}  "]

_ATTRIBUTABLE_VALUES = [N8N_DEV_UUID, N8N_DEV_UUID.upper(), "n8n-dev"]


def _padded_corpus() -> list[str]:
    return [pad.format(v) for v in _ATTRIBUTABLE_VALUES for pad in _PADDINGS]


@pytest.mark.parametrize("raw", _padded_corpus())
def test_admission_and_persistence_agree_on_every_padded_tenant(raw: str) -> None:
    """The biconditional, over padded spellings of attributable tenants.

    This is the assertion that was false before the fix: every one of these was
    ADMITTED by the validator and PERSISTED to the nil tenant.
    """
    admitted = validate_cost_event(dict(_BASE_EVENT, tenant_id=raw)) is None
    tenant_id, is_non_uuid = _normalize_tenant_id(raw)
    resolved_real = not is_non_uuid

    assert admitted == resolved_real, (
        f"admission/persistence disagree for {raw!r}: "
        f"validate_cost_event admitted={admitted} but _normalize_tenant_id "
        f"resolved={tenant_id} (is_non_uuid={is_non_uuid}). An admitted event "
        f"MUST resolve to a real tenant — cost_flusher bills off this value."
    )


@pytest.mark.parametrize("raw", _padded_corpus())
def test_a_padded_tenant_bills_the_same_tenant_as_its_bare_spelling(raw: str) -> None:
    """Padding is not identity. It must not change WHO is billed."""
    tenant_id, is_non_uuid = _normalize_tenant_id(raw)
    assert tenant_id == N8N_DEV_UUID, (
        f"{raw!r} billed {tenant_id}, not {N8N_DEV_UUID} — a real tenant's spend "
        f"was attributed elsewhere (nil sink = permanently unbillable)."
    )
    assert is_non_uuid is False
    assert tenant_id != NIL_TENANT_ID


@pytest.mark.parametrize("raw", _padded_corpus())
def test_the_display_label_drops_the_padding_too(raw: str) -> None:
    """``tenant_name`` is a dashboard column; it must not carry stray padding
    and must not be redacted as a bogus 'credential'."""
    label = original_tenant_label(raw)
    assert label == raw.strip()
    assert "redacted" not in label


# ---------------------------------------------------------------------------
# The strip must not blunt the leak guard it sits in front of.
# ---------------------------------------------------------------------------

#: All synthetic placeholders: a well-known HS256 header with a ``{"sub": "1"}``
#: payload and an alphabetical signature, and ``user:password`` base64. No real
#: credential is present here.
_CREDENTIAL_SHAPES = [
    "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.abcdefghij",  # pragma: allowlist secret
    "  Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.abcdefg\n",  # pragma: allowlist secret
    "basic dXNlcjpwYXNzd29yZA==",
    "token abc123def456",
    "x" * 49,
]


@pytest.mark.parametrize("raw", _CREDENTIAL_SHAPES)
def test_a_leaked_credential_is_still_refused_and_still_sinks(raw: str) -> None:
    """Stripping the OUTSIDE cannot rescue a value whose INSIDE is a secret.

    A real header still trips on its ``bearer``/``basic ``/``token `` prefix, on
    its interior whitespace, or on the >48-char bound. Four such rows were
    measured in production, so this is the shape that must keep failing closed.
    """
    assert validate_cost_event(dict(_BASE_EVENT, tenant_id=raw)) is not None
    tenant_id, is_non_uuid = _normalize_tenant_id(raw)
    assert tenant_id == NIL_TENANT_ID
    assert is_non_uuid is True


@pytest.mark.parametrize("raw", ["", "   ", "\n", "\t", None])
def test_an_absent_tenant_still_sinks_and_is_labelled_unknown(raw: str | None) -> None:
    """Whitespace-only is ABSENT, not a credential — it must sink, and its label
    must be the honest ``"unknown"`` rather than a redaction of nothing."""
    assert validate_cost_event(dict(_BASE_EVENT, tenant_id=raw)) is not None
    tenant_id, is_non_uuid = _normalize_tenant_id(raw)
    assert tenant_id == NIL_TENANT_ID
    assert is_non_uuid is True
    assert original_tenant_label(raw) == "unknown"
