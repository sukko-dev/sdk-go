//go:build e2e

package e2e

import (
	"context"
	"net/url"
	"slices"
	"testing"
	"time"

	sukko "github.com/sukko-dev/sdk-go"
)

// TestReconnectReplay proves kafka gap→replay no-loss recovery: messages published while
// the subscriber's connection is down are replayed after it reconnects, and the anchor
// message is not re-delivered (the resume cursor is exclusive).
//
// The subscriber dials through a controllable TCP proxy; the publisher dials the gateway
// directly. RESTPublish derives its endpoint from the client URL, so a direct publisher
// keeps publishing while the subscriber's proxy is cut. cut() drops the subscriber and
// holds its reconnect; the publisher lands seq 2+3 into kafka meanwhile; restore() lets
// the subscriber reconnect and replay from its last position.
func TestReconnectReplay(t *testing.T) {
	requireLive(t)
	ctx := context.Background()
	ch := envChannel + "-replay"

	u, err := url.Parse(envWSURL)
	if err != nil {
		t.Fatalf("parse %s: %v", envWSURL, err)
	}
	proxy := newGateProxy(t, u.Host) // relay to the real gateway host:port
	defer proxy.close()

	// Subscriber through the proxy; a short backoff makes the first reconnect dial arrive
	// promptly (it then blocks on the held gate until restore).
	proxyURL := *u
	proxyURL.Host = proxy.addr()
	sub := newClient(t, proxyURL.String(), sukko.WithToken(envToken),
		sukko.WithBackoff(sukko.BackoffConfig{
			Initial: 200 * time.Millisecond, Max: time.Second, Multiplier: 2, Jitter: 0.2,
		}))
	col := collect(sub, ch)
	defer func() { _ = sub.Close(ctx); col.wait() }()

	// Publisher direct to the gateway — unaffected by the proxy cut.
	pub := newClient(t, envWSURL, sukko.WithToken(envToken))
	defer func() { _ = pub.Close(ctx) }()

	if err := sub.Connect(ctx); err != nil {
		t.Fatalf("subscriber Connect: %v", err)
	}
	if err := sub.Subscribe(ctx, []string{ch}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitUntil(t, 15*time.Second, "subscription grant",
		func() bool { return slices.Contains(sub.Subscriptions(), ch) })

	// seq 1 live — establishes the kafka position cursor the resume replays from.
	if _, err := pub.RESTPublish(ctx, ch, payload{Seq: 1}); err != nil {
		t.Fatalf("publish seq 1: %v", err)
	}
	waitUntil(t, liveTimeout, "seq 1 live delivery", func() bool { return hasSeq(col.snapshot(), 1) })
	for _, m := range col.snapshot() {
		if p, _ := sukko.DataAs[payload](m); p.Seq == 1 && m.Pos == "" {
			t.Fatal("anchor delivery has empty Pos — a kafka backend is required for replay")
		}
	}

	// Cut: drop the subscriber and hold its reconnect, then confirm it has left connected.
	proxy.cut()
	waitUntil(t, 10*time.Second, "subscriber to leave connected",
		func() bool { return sub.State() != sukko.StateConnected })

	// Publish seq 2+3 while the subscriber is offline (direct publisher, proxy still cut).
	if _, err := pub.RESTPublish(ctx, ch, payload{Seq: 2}); err != nil {
		t.Fatalf("publish seq 2: %v", err)
	}
	if _, err := pub.RESTPublish(ctx, ch, payload{Seq: 3}); err != nil {
		t.Fatalf("publish seq 3: %v", err)
	}

	// Restore: the subscriber reconnects and replays seq 2+3.
	proxy.restore()
	waitUntil(t, 30*time.Second, "replay of seq 2+3",
		func() bool { s := col.snapshot(); return hasSeq(s, 2) && hasSeq(s, 3) })

	if n := countSeq(col.snapshot(), 1); n != 1 {
		t.Errorf("anchor seq 1 delivered %d times — resume cursor not exclusive", n)
	}
}
