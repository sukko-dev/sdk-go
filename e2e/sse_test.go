//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	sukko "github.com/sukko-dev/sdk-go"
)

// TestSSESubscribeReceive proves the receive-only SSE transport live: a client on the SSE
// transport subscribes and receives a message published over WS (SSE cannot publish). SSE is
// Pro-gated (features.go SSETransport:Pro), so this skips unless the stack is Pro+ — on
// Community the SSE route is refused. The source-built grid runs it with a Pro license.
func TestSSESubscribeReceive(t *testing.T) {
	requireLive(t)
	if envEdition != "pro" && envEdition != "enterprise" {
		t.Skipf("SSE transport is Pro-gated — stack edition is %q", envEdition)
	}
	ctx := context.Background()
	ch := envChannel + "-sse"

	// SSE reads its connect-time channel set from the desired subscriptions, so Subscribe must
	// run BEFORE Connect (it is recorded, not sent); the SSE transport derives its HTTP origin
	// from the ws:// URL internally.
	sub := newClient(t, envWSURL, sukko.WithToken(envToken), sukko.WithTransport(sukko.TransportSSE))
	col := collect(sub, ch)
	defer func() { _ = sub.Close(ctx); col.wait() }()

	if err := sub.Subscribe(ctx, []string{ch}); err != nil {
		t.Fatalf("Subscribe (pre-connect): %v", err)
	}
	if err := sub.Connect(ctx); err != nil {
		t.Fatalf("Connect (SSE): %v", err)
	}
	waitUntil(t, 15*time.Second, "SSE stream connected",
		func() bool { return sub.State() == sukko.StateConnected })

	// Publisher over WS — SSE is receive-only.
	pub := newClient(t, envWSURL, sukko.WithToken(envToken))
	defer func() { _ = pub.Close(ctx) }()
	if err := pub.Connect(ctx); err != nil {
		t.Fatalf("publisher Connect: %v", err)
	}
	if _, err := pub.RESTPublish(ctx, ch, payload{Probe: "sse"}); err != nil {
		t.Fatalf("RESTPublish: %v", err)
	}
	waitUntil(t, liveTimeout, "sse probe delivery",
		func() bool { return hasProbe(col.snapshot(), "sse") })
}
