package sukko

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const vectorsDir = "testdata/vectors"

// TestVendoredVectorsMatchChecksums is the go.sum-style guard the vendored contracts already have
// (conformance_test.go), applied to the parity-vector corpus: it fails when a vendored vector was
// edited without regenerating its checksum, or the reverse. Without it the checksum-pinning that
// platform ADR-0023 / sdk-go ADR-0003 rely on is unenforced — a stale or hand-edited vector would
// still round-trip its own binding and pass silently.
func TestVendoredVectorsMatchChecksums(t *testing.T) {
	recorded := readVectorChecksums(t)
	if len(recorded) == 0 {
		t.Fatal("vectors CHECKSUMS is empty")
	}

	for name, want := range recorded {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(vectorsDir, filepath.FromSlash(name)))
			if err != nil {
				t.Fatalf("CHECKSUMS names %s but it is not vendored: %v", name, err)
			}
			sum := sha256.Sum256(data)
			if got := hex.EncodeToString(sum[:]); got != want {
				t.Errorf("checksum mismatch for %s\n  have: %s\n  want: %s\n"+
					"Re-vendor the vector and regenerate CHECKSUMS together.", name, got, want)
			}
		})
	}

	// The reverse: a vector added without a CHECKSUMS entry would otherwise be unverified while
	// everything stays green.
	err := filepath.WalkDir(vectorsDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		rel, err := filepath.Rel(vectorsDir, path)
		if err != nil {
			return err
		}
		if _, ok := recorded[filepath.ToSlash(rel)]; !ok {
			t.Errorf("%s is vendored but has no CHECKSUMS entry", filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", vectorsDir, err)
	}
}

// readVectorChecksums parses the shasum-format testdata/vectors/CHECKSUMS.
func readVectorChecksums(t *testing.T) map[string]string {
	t.Helper()
	path := filepath.Join(vectorsDir, "CHECKSUMS")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sums := map[string]string{}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("%s line %d is not in shasum format: %q", path, i+1, line)
		}
		sums[strings.TrimPrefix(fields[1], "*")] = fields[0]
	}
	return sums
}
