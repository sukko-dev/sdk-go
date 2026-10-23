//go:build e2e

// Package e2e is the sdk-go live Tier2 conformance suite (ADR-0040): it drives the real
// Sukko Go client against the LATEST RELEASED platform — the published GHCR images,
// verified by a committed prod-signed Community license, with the kafka backend. The
// stack is booted and provisioned by e2e/run.sh, which exports the SUKKO_IT_* env the
// helpers below read; absent that env every test skips, so a bare `go test -tags e2e`
// without a running stack is a graceful no-op rather than a failure.
//
// Build-tagged `e2e` so the heavy suite stays out of the normal `go test ./...` unit run
// and the fast CI lint/gate (mirrors the CLI sibling's e2e package).
package e2e

import (
	"cmp"
	"context"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	sukko "github.com/sukko-dev/sdk-go"
)

// Live stack coordinates, exported by e2e/run.sh. The subscribe and publish paths both
// target SUKKO_IT_CHANNEL; the token and API key are provisioned for the demo tenant.
var (
	envWSURL   = os.Getenv("SUKKO_IT_WS_URL")
	envToken   = os.Getenv("SUKKO_IT_TOKEN")
	envAPIKey  = os.Getenv("SUKKO_IT_API_KEY")
	envChannel = cmp.Or(os.Getenv("SUKKO_IT_CHANNEL"), "demo.itsmoke")
	envEdition = cmp.Or(os.Getenv("SUKKO_IT_EDITION"), "community")
)

// liveTimeout bounds a first-delivery wait. First delivery on a freshly-booted kafka
// topic (a consumer joining from offset 0) can take tens of seconds; a warm stack
// returns in well under a second.
const liveTimeout = 60 * time.Second

// requireLive skips a test when run.sh did not export the live coordinates, so the
// tagged suite degrades to a skip off a running stack rather than a failure.
func requireLive(t *testing.T) {
	t.Helper()
	if envWSURL == "" || envToken == "" {
		t.Skip("live e2e needs SUKKO_IT_WS_URL + SUKKO_IT_TOKEN (exported by e2e/run.sh)")
	}
}

// payload is the probe body published and decoded back via DataAs. Both fields are
// optional so one struct serves the round-trip/api-key tests (Probe) and the recovery
// test (Seq, always a positive value so omitempty never drops it).
type payload struct {
	Probe string `json:"probe,omitempty"`
	Seq   int    `json:"seq,omitempty"`
}

// newClient builds a live WS client for url with WithInsecureTransport (the live stack is
// ws:// on loopback). The caller connects and closes it.
func newClient(t *testing.T, url string, opts ...sukko.Option) *sukko.Client {
	t.Helper()
	base := []sukko.Option{sukko.WithInsecureTransport()}
	c, err := sukko.NewClient(context.Background(), url, append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewClient(%s): %v", url, err)
	}
	return c
}

// collector drains a client's Messages() channel into a slice, filtering to *Message on
// the given channel. It MUST be started before Connect: the delivery channel is bounded
// and an undrained client parks (back-pressure by design), so a late drain would stall
// the connection. Advisory events (PossibleGap, …) ride the same stream and are skipped.
// Call wait() after client.Close — the SDK closes Messages() in its terminal sequence, so
// the range ends and the goroutine joins.
type collector struct {
	mu   sync.Mutex
	msgs []*sukko.Message
	done chan struct{}
}

func collect(c *sukko.Client, channel string) *collector {
	col := &collector{done: make(chan struct{})}
	go func() {
		defer close(col.done)
		for ev := range c.Messages() {
			m, ok := ev.(*sukko.Message)
			if !ok || m.Channel != channel {
				continue
			}
			col.mu.Lock()
			col.msgs = append(col.msgs, m)
			col.mu.Unlock()
		}
	}()
	return col
}

func (col *collector) snapshot() []*sukko.Message {
	col.mu.Lock()
	defer col.mu.Unlock()
	return slices.Clone(col.msgs)
}

func (col *collector) wait() { <-col.done }

// waitUntil polls pred until true or the deadline elapses, failing the test on timeout.
// The SDK surfaces readiness as state (Subscriptions, State), not events, so polling is
// the race-free way to synchronize a publish against a subscription grant or a reconnect.
func waitUntil(t *testing.T, deadline time.Duration, what string, pred func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for !pred() {
		if time.Now().After(end) {
			t.Fatalf("timed out after %s waiting for %s", deadline, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// hasProbe reports whether any collected message decodes to the given Probe value.
func hasProbe(msgs []*sukko.Message, probe string) bool {
	for _, m := range msgs {
		if p, err := sukko.DataAs[payload](m); err == nil && p.Probe == probe {
			return true
		}
	}
	return false
}

// hasSeq reports whether any collected message decodes to the given Seq.
func hasSeq(msgs []*sukko.Message, seq int) bool {
	return countSeq(msgs, seq) > 0
}

// countSeq counts collected messages decoding to the given Seq — used to assert the
// resume cursor is exclusive (the anchor must not be replayed).
func countSeq(msgs []*sukko.Message, seq int) int {
	n := 0
	for _, m := range msgs {
		if p, err := sukko.DataAs[payload](m); err == nil && p.Seq == seq {
			n++
		}
	}
	return n
}
