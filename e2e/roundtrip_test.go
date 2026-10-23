//go:build e2e

package e2e

import (
	"context"
	"slices"
	"testing"
	"time"

	sukko "github.com/sukko-dev/sdk-go"
)

// TestRoundTrip subscribes, then proves both publish paths deliver back to the same
// client: REST publish (available on every edition) and the fire-and-forget WebSocket
// Publish. The delivered Message carries a kafka position cursor. The WS leg exercises
// the client→gateway send path over the live socket — it would catch a transport that
// sent client frames on the wrong WebSocket opcode (the bug the Python SDK shipped).
func TestRoundTrip(t *testing.T) {
	requireLive(t)
	ctx := context.Background()

	c := newClient(t, envWSURL, sukko.WithToken(envToken))
	col := collect(c, envChannel)
	defer func() { _ = c.Close(ctx); col.wait() }()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Subscribe(ctx, []string{envChannel}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitUntil(t, 15*time.Second, "subscription grant",
		func() bool { return slices.Contains(c.Subscriptions(), envChannel) })

	// REST publish (no WS needed) → delivered back with a non-empty position cursor.
	if _, err := c.RESTPublish(ctx, envChannel, payload{Probe: "rest"}); err != nil {
		t.Fatalf("RESTPublish: %v", err)
	}
	waitUntil(t, liveTimeout, "rest probe delivery",
		func() bool { return hasProbe(col.snapshot(), "rest") })
	for _, m := range col.snapshot() {
		if p, _ := sukko.DataAs[payload](m); p.Probe == "rest" && m.Pos == "" {
			t.Errorf("kafka-backed delivery has empty Pos")
		}
	}

	// WebSocket Publish (fire-and-forget over the live connection) → delivered back.
	if err := c.Publish(ctx, envChannel, payload{Probe: "ws"}); err != nil {
		t.Fatalf("Publish (WS): %v", err)
	}
	waitUntil(t, liveTimeout, "ws probe delivery",
		func() bool { return hasProbe(col.snapshot(), "ws") })
}
