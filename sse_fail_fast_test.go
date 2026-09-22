package sukko

import (
	"context"
	"errors"
	"testing"
)

// SSE is a declared TransportKind but not yet implemented. Selecting it MUST fail fast with
// ErrSSENotImplemented — never silently build a WebSocket transport (§XV: no silent fallback).
func TestSSETransportFailsFast(t *testing.T) {
	_, err := NewClient(context.Background(), "wss://example.test/ws", WithToken("jwt"), WithTransport(TransportSSE))
	if err == nil {
		t.Fatal("NewClient(WithTransport(SSE)) returned nil error — SSE must fail fast, not fall back to WebSocket")
	}
	if !errors.Is(err, ErrSSENotImplemented) {
		t.Errorf("error = %v, want ErrSSENotImplemented", err)
	}
}
