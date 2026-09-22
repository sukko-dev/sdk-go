package sukko

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Parity-vector binding for the recovery machine. Loads the vendored, checksum-pinned corpus
// (platform ADR-0023 / sdk-go ADR-0003) and replays each scenario through the real recoveryFSM,
// proving the language-neutral schema binds to this SDK's pure FSM and that its canonical actions
// match the contract's expected effects — the same corpus sukko-js and sukko-py replay.
//
// Reuses newTestFSM/tickAt/fsmBase from recovery_fsm_test.go (same package test build).

type vectorScenario struct {
	Name    string           `json:"name"`
	Machine string           `json:"machine"`
	Inputs  []map[string]any `json:"inputs"`
	Expect  []map[string]any `json:"expect"`
}

// runRecoveryVector drives the pure recoveryFSM with a single synthetic epoch and returns the
// canonical action list (snake_case tag + keys), matching the cross-SDK vector encoding.
func runRecoveryVector(t *testing.T, s vectorScenario) []map[string]any {
	t.Helper()
	f := newTestFSM()
	e := &epoch{}
	var out []map[string]any
	canon := func(acts []replayAction) {
		for _, a := range acts {
			out = append(out, map[string]any{"action": "send_replay", "channel": a.channel, "from_pos": a.fromPos})
		}
	}
	for _, in := range s.Inputs {
		if _, isAdvance := in["advance"]; isAdvance {
			t.Fatalf("recovery vector: 'advance' inputs are not yet bound in the go spike")
		}
		switch ev, _ := in["event"].(string); ev {
		case "gap":
			canon(f.handleGap(in["channel"].(string), in["last_pos"].(string), e, tickAt(fsmBase, e)))
		case "replay_complete":
			canon(f.handleReplayComplete(in["channel"].(string), e, tickAt(fsmBase, e)))
		case "replay_message":
			// The go FSM has no per-message handler (deadline reset is not modeled here); no action.
		default:
			t.Fatalf("recovery vector: unhandled input event %v", in)
		}
	}
	return out
}

func TestRecoveryVectorGapReplayBasic(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "vectors", "recovery", "gap-replay-basic.json"))
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
}
