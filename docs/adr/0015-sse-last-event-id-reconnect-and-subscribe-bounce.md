# ADR-0015: SSE reconnect recovery via Last-Event-ID, and subscribe/unsubscribe as a lossless bounce

**Status**: Accepted
**Date**: 2026-09-30
**Ticket**: feat/go-sdk (Phase 11 SSE)

## Context

The platform's SSE transport now supports lossless reconnect recovery (platform
ADR-0030): a client that reconnects sending the `Last-Event-ID` request header — the
opaque `id:` value it last received — has the messages it missed replayed inside the
`Subscribe` stream, interleaved with live delivery, deduplicated by `mid` exactly as a
WebSocket reconnect is. This SDK currently fail-fast-stubs SSE
(`TransportSSE → ErrSSENotImplemented`); Phase 11 implements it.

SSE is **receive-only** and **connect-time-subscribed**: the channel set lives in the
`GET /sse?channels=…` URL, there is no live `subscribe` frame, and the gateway rejects an
empty set with `400`. ADR-0014 already governs the empty-desired-set edge (park on
reconnect, terminal on first connect); this ADR governs the resume cursor and how
`Subscribe`/`Unsubscribe` behave on a receive-only transport. sdk-js already implements
this (a connect-time-subscribe transport whose `subscribe()` drives the (re)open); §XVIII
requires this SDK to match it.

## Decision

1. **Resume cursor is transport-internal.** The `sseTransport` (built once, like the
   WebSocket transport) holds a mutex-guarded `lastEventID`. `sseConn.Read` captures the
   `id:` of each event at **dispatch** (the blank line — never at the `id:` line, so a
   truncated block cannot advance the cursor past an unreceived event); `Open` sends it as
   the `Last-Event-ID` header when non-empty. The cursor is **opaque** — echoed verbatim,
   never parsed. It rides across epochs on the persistent transport, so recovery needs no
   client-side plumbing (unlike WebSocket's `last_pos`, which the recovery owner threads).

2. **`Subscribe`/`Unsubscribe` on SSE is a lossless bounce.** They mutate the desired set;
   because a receive-only transport cannot send a subscription frame, the client bounces
   the epoch (closes the live conn; the supervisor's existing reconnect loop re-`Open`s,
   re-reading the desired set for the URL and riding the internal cursor). The gap across
   the bounce is replayed via `Last-Event-ID` — the same mechanism the fault matrix proved
   lossless. `connect()` with an empty set is a benign no-op (idle-until-subscribed);
   `subscribe()` drives the first open. Matches sdk-js.

3. **`CanPublish`/`CanSubscribeLive`/`CanRefreshInPlace` gate the send paths.** On SSE the
   subscribe serializer, the publish path, and the recovery owner's `reconnect{last_pos}`
   epoch-up probe are all gated off (recovery is transport-internal there); a live publish
   returns a typed capability error.

## Consequences

- **Easier**: SSE recovery is lossless with no client-side cursor plumbing; parity with
  sdk-js; the bounce reuses the proven reconnect+replay path.
- **Harder**: the subscribe serializer gains an SSE branch that bounces instead of sending
  a frame, and must coalesce rapid subscribe/unsubscribe bursts into one bounce and not
  count a deliberate bounce as a reconnect attempt (no backoff). `Subscriptions()` reports
  the connect-time desired set (no `subscription_ack` on SSE).
- **Companion platform fix (required for unsubscribe correctness)**: the gateway builds
  `SubscribeRequest.last_pos` from the decoded cursor without intersecting it against the
  permission-filtered `?channels=` set, and `authorizeLastPos` live-registers + replays
  genuinely-new cursor channels. On a WebSocket reconnect the client owns and prunes its
  `last_pos` map; the SSE client cannot (opaque cursor), so an unsubscribe→bounce would
  **resurrect** the removed channel. The gateway must intersect the decoded cursor keys
  with the requested channel set before building `last_pos` (defensible under §II
  regardless). Shipped as a separate sukko PR; the unsubscribe-bounce case depends on it.

## Alternatives rejected

- **Live `Subscribe`/`Unsubscribe` returns a typed capability error** (connect-time only):
  diverges from sdk-js's bounce (§XVIII) and is strictly less useful; the bounce is proven
  safe by the fault matrix, so there is no safety argument for the weaker contract.
- **Client-side cursor threading** (mirror the WebSocket `last_pos` recovery owner): the
  SSE cursor is opaque and per-connection; the transport already sees every `id:`, so
  holding it there is simpler and matches sdk-js. The recovery owner stays WebSocket-only.
- **Silent-deferred subscribe** (apply only on the next natural reconnect): a silent mode
  change (§XV) — the caller cannot tell whether a subscribe took effect.

## Cross-references

- ADR-0014 (empty-desired-set parks on reconnect) — the edge this builds on.
- Platform ADR-0030 (SSE reconnect replays through the Subscribe stream).
- sdk-py follows with the same contract; the Python `Client` supervisor threads the cursor
  through its per-epoch transport factory (a language-idiomatic difference — Python rebuilds
  the transport per epoch, Go and JS hold one).
