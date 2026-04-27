package coordinator

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndLoadProducerSnapshot_RoundTrip(t *testing.T) {
	dir := t.TempDir()

	state := NewIdempotenceState()
	state.Record(7, 0, 0, 4, 100)
	state.Record(7, 0, 5, 9, 105)
	state.Record(42, 1, 0, 0, 200)

	if err := WriteProducerSnapshot(dir, 1000, state.Snapshot()); err != nil {
		t.Fatalf("write: %v", err)
	}

	base, entries, err := LoadLatestProducerSnapshot(dir, 1000)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if base != 1000 {
		t.Fatalf("base = %d, want 1000", base)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}

	restored := NewIdempotenceState()
	restored.Restore(entries)
	if cached, dup, _ := restored.Validate(7, 0, 0, 4); !dup || cached != 100 {
		t.Fatalf("PID=7 first batch dup-lookup failed: cached=%d dup=%v", cached, dup)
	}
	if cached, dup, _ := restored.Validate(7, 0, 5, 9); !dup || cached != 105 {
		t.Fatalf("PID=7 second batch dup-lookup failed: cached=%d dup=%v", cached, dup)
	}
}

func TestLoadLatestProducerSnapshot_PicksHighestBaseUnderCap(t *testing.T) {
	dir := t.TempDir()

	state := NewIdempotenceState()
	state.Record(1, 0, 0, 0, 0)

	for _, b := range []int64{0, 100, 500, 2000} {
		if err := WriteProducerSnapshot(dir, b, state.Snapshot()); err != nil {
			t.Fatalf("write base=%d: %v", b, err)
		}
	}

	base, _, err := LoadLatestProducerSnapshot(dir, 600)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if base != 500 {
		t.Fatalf("base = %d, want 500 (highest <= 600)", base)
	}
}

func TestLoadLatestProducerSnapshot_RejectsCorruptedFile(t *testing.T) {
	dir := t.TempDir()

	state := NewIdempotenceState()
	state.Record(1, 0, 0, 0, 50)

	if err := WriteProducerSnapshot(dir, 100, state.Snapshot()); err != nil {
		t.Fatalf("write: %v", err)
	}

	path := filepath.Join(dir, snapshotFileName(100))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	raw[len(raw)-1] ^= 0xFF
	if werr := os.WriteFile(path, raw, 0644); werr != nil {
		t.Fatalf("rewrite: %v", werr)
	}

	base, entries, err := LoadLatestProducerSnapshot(dir, 100)
	if err != nil {
		t.Fatalf("load returned err: %v", err)
	}
	if base != 0 || entries != nil {
		t.Fatalf("expected empty result on corrupt snapshot, got base=%d entries=%v", base, entries)
	}
}

func TestProducerStateManager_PrunesOldSnapshots(t *testing.T) {
	dir := t.TempDir()
	state := NewIdempotenceState()
	state.Record(1, 0, 0, 0, 0)

	psm := NewProducerStateManager(dir, state)

	for _, b := range []int64{100, 200, 300, 400} {
		if err := WriteProducerSnapshot(dir, b, state.Snapshot()); err != nil {
			t.Fatalf("seed write base=%d: %v", b, err)
		}
	}

	psm.OnSegmentRoll(500)

	bases, err := ListProducerSnapshotOffsets(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(bases) != psm.retainSnapshots {
		t.Fatalf("after prune got %d snapshots, want %d (retain) — %v", len(bases), psm.retainSnapshots, bases)
	}
	if bases[len(bases)-1] != 500 {
		t.Fatalf("newest = %d, want 500", bases[len(bases)-1])
	}
}
