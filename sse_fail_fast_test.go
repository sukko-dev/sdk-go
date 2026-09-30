package sukko

import (
	"context"
	"testing"
)

// SSE is now a supported transport (ADR-0015): WithTransport(TransportSSE) builds a client
// without error, and the transport reports receive-only capabilities (no publish, no live
// subscribe, no in-place refresh) so the client gates those paths locally.
func TestSSETransportAccepted(t *testing.T) {
	c, err := NewClient(context.Background(), "wss://example.test/ws", WithToken("jwt"), WithTransport(TransportSSE))
	if err != nil {
		t.Fatalf("NewClient(WithTransport(SSE)) = %v, want nil — SSE is supported", err)
	}
	defer func() { _ = c.Close(context.Background()) }()

	caps := c.Capabilities()
	if caps.CanPublish || caps.CanSubscribeLive || caps.CanRefreshInPlace {
		t.Errorf("SSE capabilities = %+v, want all false (receive-only)", caps)
	}
}
