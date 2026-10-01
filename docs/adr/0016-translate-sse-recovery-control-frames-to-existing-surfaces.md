# ADR-0016: Translate the SSE recovery control frames to existing surfaces; defer precise per-channel recovery

**Status**: Accepted
**Date**: 2026-10-01

## Context

The platform added two SSE reconnect-recovery control frames (platform slice 3b,
`gateway.openapi` 1.0.3), delivered on the SSE stream as `event: message` with the type in
`data.type`, like the existing `gap` notification:

- `{"type":"no_replay","channels":[...]}` — cursor channels the server could not replay on
  reconnect (unauthorized, no Kafka mapping on a direct backend, or the replay errored).
- `{"type":"replay_truncated","replayed":N}` — the reconnect replay was cut short at the server's
  `WS_MAX_REPLAY_MESSAGES` cap; `N` records were delivered and a gap remains.

These are **SSE-only** (the server emits them only on the gRPC `Subscribe`/SSE path), so they live
in `gateway.openapi`, not the WS `client-ws.asyncapi.yaml` this SDK derives from. Today they are
absent from `decodeRegistry`, so `decodeFrame` returns them as `unknown` and `dispatch` surfaces a
`*UnknownEvent`.

**This SDK's SSE recovery is optimistic.** It relies on the server's opaque `Last-Event-ID` replay
and emits *no* client-side `*PossibleGap` on an SSE reconnect: the coalesced-`PossibleGap` mechanism
(ADR-0014/0015) snapshots the *granted* channel set at epoch death, but `grant()` is only called on
the WS `subscription_ack` path — SSE never populates it, so the snapshot unions nothing and
`emitPossibleGap()` (driven by WS `reconnect_ack`/`reconnect_error`) never fires on SSE. (Verified:
after three SSE reconnects over a cursor-bearing stream, `Stats().PossibleGaps == 0`.) Consequently,
before this change, a channel the server could not replay on an SSE reconnect was **silently lost**
— it surfaced only as a `*UnknownEvent` for the control frame, with no loss semantics.

Prior art (§XII): Centrifugo returns a `recovered` boolean per subscription (complete or failed,
never partial; `false` → use the history API); Ably sets `resumed=false` on reattach. Both collapse
the outcome into a binary "fully recovered vs. may-have-gap" signal — which is exactly what
`*PossibleGap` expresses here. The `replayed` count has no analogue in either (the actionable fact
is the truncation, not the magnitude).

## Decision

Recognize both frames in `dispatch`, intercepting them **before** the `*UnknownEvent` path (via
`handleSSEControlFrame`), so they neither surface as `*UnknownEvent` nor inflate the unknown-events
counter. They remain **out of `decodeRegistry`** — they are SSE (gateway.openapi) frames, not WS
AsyncAPI members, so the conformance test (`decodeRegistry ⇄ contract`) and the disposition table
stay untouched. Translate onto existing surfaces, both routed root-scoped (`delivery.send` with the
root context for both slots, like `surface`) because each is a permanently-unrecoverable, once-only
loss report — the cursor has advanced past the hole — so it must survive epoch teardown rather than
be discarded if the send parks during a drop:

- `no_replay` → forward one `*PossibleGap{Channels: …}` carrying the reported channels (decoded from
  the frame), counting `possibleGaps`. Because this SDK has no blanket, `no_replay` is the **only**
  signal those channels went unrecovered — surfacing it closes the pre-slice-3b silent-loss window.
- `replay_truncated` → forward a connection-level `*RecoveryInterruptedError{Kind:
  RecoveryKindReconnectReplay}` (empty `Channel`). This is the cross-SDK truncation-signal parity,
  carried on this SDK's existing interrupted-recovery surface. `RecoveryKindReconnectReplay` is a
  **new, distinct kind** so a caller can tell the server-reported connection-level SSE truncation
  apart from the existing channel-scoped, client-detected `RecoveryKindReplay` interrupt — chosen
  over `Resumed.Truncated` (`*Resumed` is never emitted on SSE; a field only populated in the failure
  case would be asymmetric). The server's `replayed` count is intentionally not carried (binary
  signal, per prior art).

The **precise** model (the server reports, on any reconnect-with-cursor, the full set of
requested-minus-recovered channels, plus a recovery-complete terminator) is **not** achievable
client-side today and is deferred to a platform-first arc (a future ADR supersedes this one). It is
blocked on two protocol gaps:

1. **No recovery-complete sentinel** on the SSE reconnect path — the client cannot await the absence
   of `no_replay` to conclude a channel was fully recovered.
2. **The quiet-channel hole** — `no_replay` is derived from the cursor; a channel subscribed but with
   no pos-bearing message before the drop has no cursor entry, so it gets neither replay nor
   `no_replay`, and the opaque cursor prevents the client computing requested-minus-cursor. So
   `no_replay` *narrows* this SDK's silent-loss window; it does not close it.

## Consequences

- Minimal, additive: two wire constants, one new recovery kind, one `handleSSEControlFrame` method,
  one decode helper, one intercept at the `unknown` branch. No change to `decodeRegistry`, the
  disposition table, the conformance test, or the vendored contract.
- SSE clients now receive a `*PossibleGap` for every server-reported unreplayable channel (previously
  a silent loss surfaced only as `*UnknownEvent`), and an explicit `*RecoveryInterruptedError` when
  the replay is truncated.
- **§XVIII cross-SDK note.** The `no_replay` surface differs across SDKs by each one's recovery
  model — specifically whether it already emits a blanket `possible_gap` on SSE reconnect that
  subsumes `no_replay`. An SDK with such a blanket recognizes `no_replay` without re-signaling (it
  would double-fire); an SDK without one (this SDK, and sukko-py) translates `no_replay` to a
  per-channel `possible_gap`. Each SDK's own companion ADR states its model; the shared invariant is
  **no silent recovery loss**. The deferred precision arc re-unifies the behavior.

## Alternatives rejected

- **Recognize `no_replay` but don't re-signal**: correct only for an SDK whose blanket already covers
  it. This SDK has no blanket (see Context), so dropping `no_replay` would be a silent recovery-loss
  regression — worse than the prior `*UnknownEvent` baseline.
- **Add the frames to `decodeRegistry`/the event union**: breaks the conformance test (they are not
  in the WS AsyncAPI) and forces golden-fixture + CHECKSUMS churn for non-WS-contract frames.
  Intercept-and-translate keeps the contract surface honest.
- **`Resumed.Truncated`** for `replay_truncated`: `*Resumed` is never emitted on SSE; a field only
  populated in the failure case is asymmetric.
- **Precise per-channel recovery now**: blocked on the two protocol gaps above; deferred.
