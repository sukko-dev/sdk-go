//go:build e2e

package e2e

import (
	"context"
	"slices"
	"testing"
	"time"

	sukko "github.com/sukko-dev/sdk-go"
)

// TestAPIKeyAuth proves the gateway accepts X-API-Key auth on the WebSocket path, not
// only JWTs: a key-authenticated client subscribes and receives. API keys are read-only
// (publish needs a JWT), so a JWT client does the publish. The channel suffix stays a
// single segment so the demo tenant's wildcard rules authorize it.
func TestAPIKeyAuth(t *testing.T) {
	requireLive(t)
	if envAPIKey == "" {
		t.Skip("api-key test needs SUKKO_IT_API_KEY (exported by e2e/run.sh)")
	}
	ctx := context.Background()
	ch := envChannel + "-apikey"

	sub := newClient(t, envWSURL, sukko.WithAPIKey(envAPIKey))
	col := collect(sub, ch)
	defer func() { _ = sub.Close(ctx); col.wait() }()

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

	if _, err := pub.RESTPublish(ctx, ch, payload{Probe: "apikey"}); err != nil {
		t.Fatalf("RESTPublish: %v", err)
	}
	waitUntil(t, liveTimeout, "apikey probe delivery",
		func() bool { return hasProbe(col.snapshot(), "apikey") })
}
