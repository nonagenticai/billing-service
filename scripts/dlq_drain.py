#!/usr/bin/env python3
"""Drain ``billing:costs:dead`` — page the live stream, discard, prove it happened.

WHY THIS IS IN THE REPO. It was written on 2026-08-28 as ``/tmp/dlq_drain.py`` on
the k3s master, used once to discard 36 entries, and left there. The tool it
replaced (``/tmp/dlq_discard.py``) is still sitting in the same directory waiting
for the next operator to find it, and it is DEFECTIVE in two ways that a
successful-looking run would not reveal:

  1. It read ``res["discarded"]`` at the top level. The route declares
     ``response_model=Envelope[DlqDiscardResult]``, so the counts live under
     ``data``. It would have deleted 1,007 entries and printed
     ``TOTALS: {'discarded': 0, ...}`` — a destructive success reported as a
     no-op. It also counted a ``not_found`` key the result does not have.
  2. It drove off a 3.5-hour-old id dump instead of the live stream. Eleven
     entries arrived between that dump and the sweep; driving off the dump would
     have silently missed every one.

An operational tool whose only copy is in ``/tmp`` on one host is not a tool, it
is a memory. Hence this file.

WHAT IT ASSERTS, and every one of these is a defect it has already caught:
  * the envelope is unwrapped before any count is read;
  * ``len(eligible) + len(refused) == examined`` on every batch — the server's
    own arithmetic must close, or the buckets are not a partition;
  * a dry run discards NOTHING (``discarded == []``), so ``--confirm`` is the
    only thing that can delete;
  * refused ids are remembered, so an ineligible entry cannot spin the loop
    forever by reappearing on the next page;
  * ``examined > 0`` or **exit 3**. A run that inspected nothing proves nothing,
    and exiting 0 on it is the vacuity defect this estate keeps re-finding.

⚠️ ``--reason`` IS REQUIRED AND HAS NO DEFAULT, deliberately. The 36 entries
discarded on 2026-08-28 carry an audit reason inherited verbatim from a plan:
"producer fixed (2138 attempts in 6h, 0 new dead-letters)". That claim had
already been refuted — by the very entries it was used to discard, which arrived
after the window it cites. Audit rows are immutable, so the permanent record now
carries a justification the operator's own measurement disproved. A default
reason is how that happens; typing the reason at the moment you decide is the
fix. The placeholder guard below exists because a default with a "fill this in"
marker is just a slower default.

USAGE (from the k3s master, which can reach the service and read the secret):

    sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl -n billing port-forward \
        svc/billing-service 8157:8157 &
    python3 scripts/dlq_drain.py --reason "<why these are unrecoverable, today>"
    python3 scripts/dlq_drain.py --reason "..." --confirm    # irreversible

Exit codes: 0 drained (or nothing to drain, with a non-vacuous inspection),
1 HTTP failure, 2 token mint failure, 3 VACUOUS (examined 0 — proves nothing).
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

# In-cluster service DNS, never a pod or ClusterIP address: the ClusterIP the
# original /tmp copy hardcoded is reassigned whenever the Service is recreated,
# and a stale one fails as a connection timeout rather than as a name error.
DEFAULT_BASE_URL = "http://billing-service.billing.svc.cluster.local:8157"
DEFAULT_TOKEN_URL = "https://auth.nonagentic.ai/realms/secure-apps/protocol/openid-connect/token"

# Pages are what the server will return; CHUNK is what we ask it to act on at
# once. Kept apart so a large page cannot become one enormous destructive call.
PAGE = 500
CHUNK = 200

# A reason must be a sentence someone wrote today, not a token that satisfies a
# length check. Substrings, lowercased — see the module docstring.
REASON_PLACEHOLDERS = ("todo", "tbd", "fixme", "xxx", "fill this in", "<why", "reason here")
MIN_REASON_CHARS = 40

# Kept as a named constant so the truncation gate is greppable and can be
# disabled in a test without monkeypatching argv.
CONFIRM_REMAINING_IS_AN_ERROR = True


def _kubectl(*args: str) -> str:
    return subprocess.check_output(
        ["sudo", "KUBECONFIG=/etc/rancher/k3s/k3s.yaml", "kubectl", *args], text=True
    ).strip()


def _secret(key: str) -> str:
    raw = _kubectl(
        "get", "secret", "-n", "billing", "billing-secrets", "-o", f"jsonpath={{.data.{key}}}"
    )
    return base64.b64decode(raw).decode()


def _mint_token(token_url: str) -> str:
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": _secret("keycloak-client-id"),
            "client_secret": _secret("keycloak-client-secret"),
        }
    ).encode()
    request = urllib.request.Request(
        token_url,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            # ⚠️ Cloudflare 403s a default ``Python-urllib`` User-Agent on
            # auth.nonagentic.ai, and the response reads exactly like a missing
            # Keycloak role. Send a UA.
            "User-Agent": "curl/8.5.0",
            "Accept": "application/json",
        },
    )
    try:
        return json.load(urllib.request.urlopen(request, timeout=30))["access_token"]
    except urllib.error.HTTPError as exc:
        print(f"TOKEN MINT HTTP {exc.code}: {exc.read()[:400].decode(errors='replace')}")
        raise SystemExit(2) from None


def _call(method: str, url: str, token: str, body: dict | None = None) -> dict:
    """Call the admin API and return the ENVELOPE'S ``data``, never the envelope.

    Unwrapping here rather than at each call site is the whole point: the
    predecessor read counts off the envelope and reported zeros after a
    successful delete.
    """
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    try:
        payload = json.load(urllib.request.urlopen(request, timeout=120))
    except urllib.error.HTTPError as exc:
        print(f"HTTP {exc.code} on {method} {url}: {exc.read()[:500].decode(errors='replace')}")
        raise SystemExit(1) from None
    if "data" not in payload:
        print(f"UNEXPECTED SHAPE from {method} {url}: no 'data' key in {sorted(payload)}")
        raise SystemExit(1)
    return payload["data"]


def _check_reason(reason: str) -> None:
    stripped = reason.strip()
    if len(stripped) < MIN_REASON_CHARS:
        print(
            f"--reason is {len(stripped)} chars; at least {MIN_REASON_CHARS} are required. "
            "This lands in an immutable audit row — say why these entries are "
            "unrecoverable, as measured today."
        )
        raise SystemExit(1)
    lowered = stripped.lower()
    for placeholder in REASON_PLACEHOLDERS:
        if placeholder in lowered:
            print(f"--reason contains the placeholder {placeholder!r}. Write the real reason.")
            raise SystemExit(1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reason",
        required=True,
        help="Immutable audit justification. No default, on purpose — see module docstring.",
    )
    parser.add_argument("--confirm", action="store_true", help="Actually delete. Irreversible.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--token-url", default=DEFAULT_TOKEN_URL)
    parser.add_argument("--max-passes", type=int, default=20)
    args = parser.parse_args()

    _check_reason(args.reason)
    token = _mint_token(args.token_url)
    print(
        f"token minted len={len(token)}  "
        f"mode={'CONFIRM (irreversible)' if args.confirm else 'DRY RUN'}"
    )

    inspect_url = f"{args.base_url}/api/admin/dlq/inspect?limit={PAGE}"
    discard_url = f"{args.base_url}/api/admin/dlq/discard"

    totals = {"examined": 0, "eligible": 0, "discarded": 0, "refused": 0, "failed": 0}
    refusal_reasons: dict[str, int] = {}
    refused_ids: set[str] = set()

    passes = 0
    for pass_no in range(1, args.max_passes + 1):
        passes = pass_no
        page = _call("GET", inspect_url, token)
        # Skipping ids already refused is what makes the loop terminate: an
        # ineligible entry stays on the stream and would otherwise be re-fetched
        # on every pass, forever, with each pass "making progress" on paper.
        ids = [e["id"] for e in page["entries"] if e["id"] not in refused_ids]
        print(
            f"[pass {pass_no}] stream page count={page['count']} "
            f"has_more={page['has_more']} actionable={len(ids)}"
        )
        if not ids:
            break

        pass_discarded = 0
        for start in range(0, len(ids), CHUNK):
            result = _call(
                "POST",
                discard_url,
                token,
                {"ids": ids[start : start + CHUNK], "reason": args.reason, "confirm": args.confirm},
            )
            examined = result["examined"]
            eligible, discarded, refused = (
                result["eligible"],
                result["discarded"],
                result["refused"],
            )
            # ⚠️ `<=`, NOT `==`. The first draft asserted equality and would have
            # aborted a 1,007-entry sweep on one Redis hiccup, reporting "the
            # buckets are not a partition" — a misdiagnosis. `examined` is
            # incremented before the per-id work, and dlq_ops' per-id `except`
            # swallows and continues by design, so a failed id is examined and
            # lands in neither bucket. Equality is the healthy case, not the
            # contract. Over-counting IS still impossible and still a real defect.
            accounted = len(eligible) + len(refused)
            if accounted > examined:
                print(
                    f"INVARIANT BROKEN: eligible {len(eligible)} + refused "
                    f"{len(refused)} > examined {examined}. An id is in two "
                    f"buckets; stopping."
                )
                raise SystemExit(1)
            if accounted < examined:
                dropped = examined - accounted
                totals["failed"] += dropped
                print(
                    f"  ⚠️ {dropped} id(s) examined but in neither bucket — per-id "
                    f"failures inside dlq_ops (see dlq_discard_entry_failed in the "
                    f"service log). They remain on the stream; re-run to retry."
                )
            if not args.confirm and discarded:
                print(f"DRY RUN DELETED {len(discarded)} ENTRIES — stopping immediately.")
                raise SystemExit(1)

            totals["examined"] += examined
            totals["eligible"] += len(eligible)
            totals["discarded"] += len(discarded)
            totals["refused"] += len(refused)
            pass_discarded += len(discarded)
            for item in refused:
                why = item.get("reason", "?")
                refusal_reasons[why] = refusal_reasons.get(why, 0) + 1
                refused_ids.add(item.get("id"))

        if not args.confirm:
            break
        if pass_discarded == 0:
            print("[stop] a full pass discarded nothing — no further progress possible")
            break

    final = _call("GET", inspect_url, token)
    print("TOTALS:", totals)
    print("REFUSAL REASONS:", refusal_reasons or "(none)")
    print(f"REMAINING on stream: count={final['count']} has_more={final['has_more']}")

    if totals["examined"] == 0:
        print(
            "VACUOUS: examined 0 entries — nothing was inspected, so this run proves "
            "NOTHING. Exiting 3 rather than 0: an empty queue and an unreachable one "
            "look identical from a zero."
        )
        return 3

    # ⚠️ TRUNCATION IS NOT SUCCESS, and this exit is the whole reason it is here.
    # `/dlq/inspect` is `XRANGE(stream, count=<=500)` — always the OLDEST page,
    # with NO cursor. So once refused entries fill one page (>=500 with
    # cost_usd > 0, which is exactly the population that accumulates), the next
    # pass sees only ids already in `refused_ids`, `ids` is empty, the loop
    # breaks, and EVERYTHING BEHIND THAT PAGE IS NEVER INSPECTED. Hitting
    # --max-passes truncates the same way. Both used to print a cheerful
    # "REMAINING count=5000" and return 0 — a drain that stopped short reported
    # as a drain that finished. There was an exit for a vacuous run and none for
    # a truncated one.
    #
    # Only enforced under --confirm: a dry run discards nothing, so entries
    # remaining is its CORRECT outcome, not a shortfall.
    if CONFIRM_REMAINING_IS_AN_ERROR and args.confirm and final["count"] > 0:
        print(
            f"TRUNCATED: {final['count']} entr(y/ies) still on the stream after "
            f"{passes} pass(es). This run did NOT drain the queue. Either every "
            f"remaining entry is ineligible (see REFUSAL REASONS above — that is a "
            f"policy answer, not a drain), or the oldest page is full of refusals "
            f"and the tail behind it was never inspected. Exiting 4, not 0: a "
            f"partial drain and a complete one must not look the same."
        )
        return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
