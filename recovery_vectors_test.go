package sukko

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// Parity-vector binding for the recovery machine. Loads the vendored, checksum-pinned corpus
// (platform ADR-0023 / sdk-go ADR-0003) and replays each scenario through the real recoveryFSM,
// proving the language-neutral schema binds to this SDK's pure FSM and that its canonical actions
// match the contract's expected effects — the same corpus sukko-js and sukko-py replay.
//
// The FSM runs at the canonical recovery deadline/floor (DefaultRecoveryDeadline /
// DefaultReplayFloor = 10s), the values the vectors' `advance` timings assume — NOT the 1h
// deadline of newTestFSM, whose purpose is the opposite (keep timing out of the way). A single
// synthetic epoch stands in for the live connection; a virtual clock advances only on `advance`
// inputs, so timing-gated paths (the recovery deadline) are exercised deterministically with no
// real time (§VIII).

type vectorScenario struct {
	Name    string           `json:"name"`
	Machine string           `json:"machine"`
	Inputs  []map[string]any `json:"inputs"`
	Expect  []map[string]any `json:"expect"`
}

// runRecoveryVector drives the pure recoveryFSM and returns the canonical action list (snake_case
// tag + keys), matching the cross-SDK vector encoding. `advance` moves the virtual clock and then
// fires due() — the same call the real owner makes when its wake timer elapses — so a recovery
// deadline that has come due is observed exactly as in production. A `replay_message` bumps the
// recovery-frame counter the owner reads lock-free at its tick (delivery.replayFrames); the FSM
// has no per-frame handler.
func runRecoveryVector(t *testing.T, s vectorScenario) []map[string]any {
	t.Helper()
	f := newRecoveryFSM(DefaultReplayFloor, DefaultRecoveryDeadline)
	e := &epoch{}
	now := fsmBase
	// Per-channel recovery-frame counts — mirrors delivery.replayFrameCounts; the tick's
	// accessor reads them so silence-suspension is scoped per channel (platform ADR-0025).
	frames := map[string]int64{}
	tickNow := func() tick {
		return tick{now: now, current: e, replayFrames: func(ch string) int64 { return frames[ch] }}
	}

	var out []map[string]any
	canonReplays := func(acts []replayAction) {
		for _, a := range acts {
			out = append(out, map[string]any{"action": "send_replay", "channel": a.channel, "from_pos": a.fromPos})
		}
	}
	canonInterrupts := func(channels []string) {
		for _, ch := range channels {
			out = append(out, map[string]any{"action": "raise_recovery_interrupted", "channel": ch})
		}
	}

	for _, in := range s.Inputs {
		if ms, isAdvance := in["advance"]; isAdvance {
			now = now.Add(time.Duration(ms.(float64) * float64(time.Millisecond)))
			outcome := f.due(tickNow())
			canonReplays(outcome.replays)
			canonInterrupts(outcome.interrupts)
			continue
		}
		switch ev, _ := in["event"].(string); ev {
		case "gap":
			canonReplays(f.handleGap(in["channel"].(string), in["last_pos"].(string), e, tickNow()))
		case "replay_complete":
			canonReplays(f.handleReplayComplete(in["channel"].(string), e, tickNow()))
		case "replay_message":
			// A recovery frame arrived on this channel: recovery progress that re-arms its
			// silence deadline (platform ADR-0025). The owner never sees a per-frame event — it
			// reads this progress lock-free at its next due() tick, so here it is a counter bump.
			frames[in["channel"].(string)]++
		default:
			t.Fatalf("recovery vector: unhandled input event %v", in)
		}
	}
	return out
}

func TestRecoveryVectors(t *testing.T) {
	// Every vendored recovery vector replays through the real recoveryFSM and must produce the
	// scenario's canonical actions — adding a scenario is one JSON file (vendored), no test change.
	files, err := filepath.Glob(filepath.Join("testdata", "vectors", "recovery", "*.json"))
	if err != nil {
		t.Fatalf("glob vectors: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no recovery vectors found")
	}
	for _, path := range files {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read vector: %v", err)
			}
			var s vectorScenario
			if err := json.Unmarshal(raw, &s); err != nil {
				t.Fatalf("parse vector: %v", err)
			}
			if s.Machine != "recovery" {
				t.Fatalf("machine = %q, want recovery", s.Machine)
			}
			got := runRecoveryVector(t, s)
			if !reflect.DeepEqual(got, s.Expect) {
				t.Errorf("canonical actions mismatch\n got: %#v\nwant: %#v", got, s.Expect)
			}
		})
	}
}
