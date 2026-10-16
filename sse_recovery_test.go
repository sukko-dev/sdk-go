package sukko

import (
	"context"
	"slices"
	"testing"
	"time"
)

// The two SSE-only reconnect-recovery control frames (no_replay / replay_truncated,
// gateway.openapi 1.0.3, ADR-0016) ride the transport-agnostic dispatch path, so the fakeWS
// harness exercises the same decode→dispatch→surface route an SSE stream would. They are
// deliberately absent from decodeRegistry, so dispatch sees them as "unknown" and must intercept
// them before the UnknownEvent path.

func TestSSEReplayTruncatedSurfacesRecoveryInterrupted(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{onConnect: []string{
		`{"type":"replay_truncated","replayed":3}`,
	}})
	c := newTestClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close(context.Background()) }()

	timeout := time.After(3 * time.Second)
	for {
		select {
		case ev, ok := <-c.Messages():
			if !ok {
				t.Fatal("Messages() closed before a *RecoveryInterruptedError")
			}
			if _, isUnknown := ev.(*UnknownEvent); isUnknown {
				t.Fatal("replay_truncated surfaced as *UnknownEvent — interception failed")
			}
			if ri, isRI := ev.(*RecoveryInterruptedError); isRI {
				if ri.Kind != RecoveryKindReconnectReplay {
					t.Errorf("Kind = %q, want %q", ri.Kind, RecoveryKindReconnectReplay)
				}
				if ri.Channel != "" {
					t.Errorf("Channel = %q, want empty (connection-level)", ri.Channel)
				}
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for *RecoveryInterruptedError")
		}
	}
}

func TestSSENoReplaySurfacesPossibleGap(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{onConnect: []string{
		`{"type":"no_replay","channels":["acme.a","acme.b"]}`,
	}})
	c := newTestClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close(context.Background()) }()

	// This SDK's SSE recovery is optimistic — no blanket *PossibleGap on reconnect — so the server's
	// no_replay is the only signal these channels went unrecovered. It must surface a *PossibleGap
	// carrying them (not a *UnknownEvent, and not silence, which would be a silent recovery loss).
	timeout := time.After(3 * time.Second)
	for {
		select {
		case ev, ok := <-c.Messages():
			if !ok {
				t.Fatal("Messages() closed before a *PossibleGap")
			}
			if _, isUnknown := ev.(*UnknownEvent); isUnknown {
				t.Fatal("no_replay surfaced as *UnknownEvent — it must map to *PossibleGap")
			}
			if pg, isPG := ev.(*PossibleGap); isPG {
				if !slices.Equal(pg.Channels, []string{"acme.a", "acme.b"}) {
					t.Errorf("PossibleGap.Channels = %v, want [acme.a acme.b]", pg.Channels)
				}
				if n := c.Stats().PossibleGaps; n != 1 {
					t.Errorf("PossibleGaps = %d, want 1", n)
				}
				if n := c.Stats().UnknownEvents; n != 0 {
					t.Errorf("UnknownEvents = %d, want 0 (no_replay recognized, not unknown)", n)
				}
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for *PossibleGap")
		}
	}
}

// A no_replay frame carrying no channels (an edge the contract permits) surfaces nothing — no
// *PossibleGap, no *UnknownEvent — yet is still recognized, and the read-pump survives so the
// following live message is delivered.
func TestSSENoReplayEmptyChannelsSurfacesNothing(t *testing.T) {
	f := newFakeWS(t)
	f.script(epochScript{onConnect: []string{
		`{"type":"no_replay","channels":[]}`,
		`{"type":"message","channel":"acme.a","seq":1,"ts":1,"data":{"k":1}}`,
	}})
	c := newTestClient(t, f)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close(context.Background()) }()

	timeout := time.After(3 * time.Second)
	for {
		select {
		case ev, ok := <-c.Messages():
			if !ok {
				t.Fatal("Messages() closed before the trailing *Message")
			}
			switch ev.(type) {
			case *UnknownEvent:
				t.Fatal("empty no_replay surfaced as *UnknownEvent — it must be recognized")
			case *PossibleGap:
				t.Fatal("empty no_replay surfaced a *PossibleGap — nothing should be surfaced")
			case *Message:
				if n := c.Stats().PossibleGaps; n != 0 {
					t.Errorf("PossibleGaps = %d, want 0 (empty no_replay surfaces nothing)", n)
				}
				if n := c.Stats().UnknownEvents; n != 0 {
					t.Errorf("UnknownEvents = %d, want 0", n)
				}
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for the trailing *Message")
		}
	}
}
