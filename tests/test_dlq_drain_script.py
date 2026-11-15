"""Guards on ``scripts/dlq_drain.py`` — the reason gate and the vacuity exit.

WHY A SCRIPT GETS A TEST FILE. This tool deletes rows and writes an immutable
audit reason, and its predecessor shipped two defects that a successful-looking
run concealed: it read the discard counts one nesting level too high (so a
1,007-row delete would print ``discarded: 0``) and it drove off a stale id dump
instead of the live stream. The two behaviours pinned here are the ones that
would silently regress into exactly that class of failure again.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "dlq_drain.py"


def _load():
    spec = importlib.util.spec_from_file_location("dlq_drain", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["dlq_drain"] = module
    spec.loader.exec_module(module)
    return module


drain = _load()


@pytest.mark.unit
class TestReasonGate:
    """``--reason`` is required and has no default, and this is why.

    The 36 entries discarded on 2026-08-28 carry an audit reason inherited
    verbatim from a plan — "producer fixed (2138 attempts in 6h, 0 new
    dead-letters)" — that had already been refuted by the very entries it was
    used to discard, which arrived AFTER the window it cites. Audit rows are
    immutable, so the permanent record still carries a justification the
    operator's own measurement disproved. A default reason is how that happens.
    """

    def test_a_real_reason_passes(self) -> None:
        drain._check_reason(
            "nil_tenant_id with cost_usd=0 on free local providers; replay "
            "re-rejects identically, measured 2026-08-29."
        )

    def test_a_short_reason_is_refused(self) -> None:
        with pytest.raises(SystemExit) as exc:
            drain._check_reason("draining the queue")
        assert exc.value.code == 1

    @pytest.mark.parametrize(
        "reason",
        [
            "TODO write the real justification for this destructive sweep here",
            "tbd — undrainable nil tenant entries, will explain properly later on",
            "FIXME this is the standard reason we always use for these sweeps ok",
        ],
    )
    def test_a_placeholder_is_refused_even_when_long_enough(self, reason: str) -> None:
        """A placeholder that clears the length floor is still a default.

        Without this, the length gate teaches the next operator to pad rather
        than to think — the same failure the gate exists to prevent.
        """
        assert len(reason.strip()) >= drain.MIN_REASON_CHARS
        with pytest.raises(SystemExit) as exc:
            drain._check_reason(reason)
        assert exc.value.code == 1


@pytest.mark.unit
class TestEnvelopeUnwrap:
    """``_call`` must return the envelope's ``data``, and must refuse a body without one.

    The predecessor read ``res["discarded"]`` off the envelope. The route
    declares ``response_model=Envelope[DlqDiscardResult]``, so that key does not
    exist there and every count printed zero — after a successful delete. A
    missing ``data`` key is therefore not a shrug, it is the signature of the
    exact bug, and it must stop the run rather than be read as "nothing
    happened".
    """

    def test_unwraps_data(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(drain.urllib.request, "urlopen", _fake_urlopen({"data": {"count": 7}}))
        assert drain._call("GET", "http://x/api", "tok") == {"count": 7}

    def test_a_body_with_no_data_key_exits_nonzero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(drain.urllib.request, "urlopen", _fake_urlopen({"discarded": []}))
        with pytest.raises(SystemExit) as exc:
            drain._call("GET", "http://x/api", "tok")
        assert exc.value.code == 1


def _fake_urlopen(payload: dict):
    import io
    import json

    def _open(_request, timeout=None):  # noqa: ARG001
        return io.StringIO(json.dumps(payload))

    return _open


@pytest.mark.unit
class TestTruncationIsNotSuccess:
    """A drain that stopped short must not exit 0.

    THE DEFECT, found by an adversarial pass on the first draft.
    ``/dlq/inspect`` is ``XRANGE(stream, count<=500)`` — always the OLDEST page,
    with NO cursor. So once refused entries fill one page (>=500 with
    ``cost_usd > 0``, which is exactly the population that accumulates), the next
    pass sees only ids already in ``refused_ids``, ``ids`` is empty, the loop
    breaks, and every entry BEHIND that page is never inspected. Hitting
    ``--max-passes`` truncates the same way. Both printed a cheerful
    ``REMAINING count=5000`` and returned **0**.

    That is the exact defect class this tool exists to prevent, reproduced inside
    the tool: there was an exit for a vacuous run and none for a truncated one.
    """

    def test_the_gate_is_armed(self) -> None:
        """A named constant, so the gate is greppable and cannot be lost silently."""
        assert drain.CONFIRM_REMAINING_IS_AN_ERROR is True

    def test_remaining_entries_under_confirm_is_exit_4(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        rc = _run_main(
            monkeypatch,
            argv=["--reason", _REASON, "--confirm"],
            pages=[_page(["a-1"]), _page([])],
            discard={"examined": 1, "eligible": ["a-1"], "discarded": ["a-1"], "refused": []},
            final=_page(["b-1", "b-2"]),
        )
        out = capsys.readouterr().out
        assert rc == 4, out
        assert "TRUNCATED" in out

    def test_a_dry_run_leaving_entries_is_still_exit_0(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """The disproof arm. A dry run discards nothing, so entries remaining is
        its CORRECT outcome — gating on it would make every dry run look failed."""
        rc = _run_main(
            monkeypatch,
            argv=["--reason", _REASON],
            pages=[_page(["a-1"])],
            discard={"examined": 1, "eligible": ["a-1"], "discarded": [], "refused": []},
            final=_page(["a-1"]),
        )
        assert rc == 0, capsys.readouterr().out

    def test_a_fully_drained_confirm_is_exit_0(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        rc = _run_main(
            monkeypatch,
            argv=["--reason", _REASON, "--confirm"],
            pages=[_page(["a-1"]), _page([])],
            discard={"examined": 1, "eligible": ["a-1"], "discarded": ["a-1"], "refused": []},
            final=_page([]),
        )
        assert rc == 0, capsys.readouterr().out


@pytest.mark.unit
class TestPerIdFailureIsNotAnInvariantBreach:
    """`eligible + refused <= examined`, and a shortfall must not abort the sweep.

    The first draft asserted EQUALITY and exited 1 on any gap, reporting "the
    buckets are not a partition". That is a misdiagnosis: `examined` is bumped
    before the per-id work and dlq_ops' per-id `except` swallows and continues by
    design, so one Redis hiccup leaves an id in neither bucket — and would have
    aborted an otherwise-good 1,007-entry sweep.
    """

    def test_a_shortfall_is_reported_and_the_sweep_continues(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        rc = _run_main(
            monkeypatch,
            argv=["--reason", _REASON, "--confirm"],
            pages=[_page(["a-1", "a-2"]), _page([])],
            # 2 examined, only 1 accounted for: one id failed inside dlq_ops.
            discard={"examined": 2, "eligible": ["a-1"], "discarded": ["a-1"], "refused": []},
            final=_page([]),
        )
        out = capsys.readouterr().out
        assert rc == 0, out
        assert "in neither bucket" in out
        assert "'failed': 1" in out

    def test_over_counting_is_still_a_hard_stop(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """An id in TWO buckets is a real defect and must still abort."""
        with pytest.raises(SystemExit) as exc:
            _run_main(
                monkeypatch,
                argv=["--reason", _REASON, "--confirm"],
                pages=[_page(["a-1"])],
                discard={
                    "examined": 1,
                    "eligible": ["a-1"],
                    "discarded": ["a-1"],
                    "refused": [{"id": "a-1", "reason": "x"}],
                },
                final=_page([]),
            )
        assert exc.value.code == 1
        assert "INVARIANT BROKEN" in capsys.readouterr().out


_REASON = "nil_tenant_id with cost_usd=0 on free local providers, measured 2026-08-29"


def _page(ids: list[str]) -> dict:
    return {"entries": [{"id": i} for i in ids], "count": len(ids), "has_more": False}


def _run_main(monkeypatch, *, argv, pages, discard, final) -> int:
    """Drive drain.main() with the network and kubectl stubbed out.

    Stubs `_mint_token` and `_call` only — the loop, the guards and every exit
    path under test are the real ones.
    """
    monkeypatch.setattr(drain, "_mint_token", lambda _u: "tok")
    seq = list(pages)

    def fake_call(method, url, token, body=None):
        if method == "GET":
            return seq.pop(0) if seq else final
        return discard

    monkeypatch.setattr(drain, "_call", fake_call)
    monkeypatch.setattr(sys, "argv", ["dlq_drain.py", *argv])
    return drain.main()
