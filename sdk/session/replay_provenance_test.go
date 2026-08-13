package session

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReplayProvenancePreflightCreatesPrivateFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "replay")
	path := filepath.Join(dir, strings.Repeat("a", 64)+".provenance.jsonl")
	sink := NewFileProvenanceSink(path, dir)
	if err := sink.Preflight(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		path string
		mode os.FileMode
	}{{dir, 0700}, {path, 0600}, {sink.LockPath(), 0600}} {
		assertReplayPrivatePath(t, check.path, check.mode)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("preflight appended data: %q", data)
	}
}

func TestReplayProvenanceConcurrentAppendsAreCompleteJSONLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	const count = 100
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := range count {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			// Independent sink instances model independent dtctl processes. They
			// derive one shared append-lock identity from the path.
			sink := NewFileProvenanceSink(path, dir)
			err := sink.Append(context.Background(), ReplayProvenanceRecord{
				SchemaVersion: ReplayProvenanceSchemaVersion,
				RecordedAt:    time.Date(2026, 8, 8, 10, 30, index, 0, time.UTC),
				Event:         "test-append",
				SessionID:     "00000000000000000000000000000000",
				Fields:        map[string]any{"index": index},
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	seen := make(map[int]bool)
	scanner := bufio.NewScanner(f)
	lines := 0
	for scanner.Scan() {
		lines++
		var record ReplayProvenanceRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("line %d is not complete JSON: %v", lines, err)
		}
		value, ok := record.Fields["index"].(float64)
		if !ok {
			t.Fatalf("line %d missing index: %+v", lines, record)
		}
		seen[int(value)] = true
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if lines != count || len(seen) != count {
		t.Fatalf("lines/unique indices = %d/%d, want %d", lines, len(seen), count)
	}
}

func TestReplayProvenanceCrossProcessAppends(t *testing.T) {
	if os.Getenv("DTCTL_REPLAY_PROVENANCE_HELPER") == "1" {
		path := os.Getenv("DTCTL_REPLAY_PROVENANCE_PATH")
		index, err := strconv.Atoi(os.Getenv("DTCTL_REPLAY_PROVENANCE_INDEX"))
		if err != nil {
			t.Fatal(err)
		}
		sink := NewFileProvenanceSink(path, filepath.Dir(path))
		for offset := range 10 {
			value := index*10 + offset
			if err := sink.Append(context.Background(), ReplayProvenanceRecord{
				SchemaVersion: ReplayProvenanceSchemaVersion,
				RecordedAt:    time.Date(2026, 8, 8, 10, 30, 0, value, time.UTC),
				Event:         "cross-process-test-append",
				Fields:        map[string]any{"index": value},
			}); err != nil {
				t.Fatal(err)
			}
		}
		return
	}

	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "cross-process.jsonl")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	const processes = 6
	commands := make([]*exec.Cmd, 0, processes)
	for index := range processes {
		command := exec.Command(executable, "-test.run=^TestReplayProvenanceCrossProcessAppends$")
		command.Env = replaySourceHelperEnv(os.Environ(), map[string]string{
			"DTCTL_REPLAY_PROVENANCE_HELPER": "1",
			"DTCTL_REPLAY_PROVENANCE_PATH":   path,
			"DTCTL_REPLAY_PROVENANCE_INDEX":  strconv.Itoa(index),
		})
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		commands = append(commands, command)
	}
	for _, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatalf("provenance helper: %v", err)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != processes*10 {
		t.Fatalf("cross-process line count = %d, want %d", len(lines), processes*10)
	}
	for index, line := range lines {
		if !json.Valid([]byte(line)) {
			t.Fatalf("cross-process line %d is partial JSON: %q", index+1, line)
		}
	}
}

func TestReplayProvenanceRejectsCorruptTrailingRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	if err := os.WriteFile(path, []byte(`{"schema_version":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	sink := NewFileProvenanceSink(path, dir)
	if err := sink.Preflight(context.Background()); err == nil || !strings.Contains(err.Error(), "partial trailing record") {
		t.Fatalf("preflight error = %v", err)
	}
	if err := sink.Append(context.Background(), ReplayProvenanceRecord{
		SchemaVersion: 1,
		RecordedAt:    time.Now(),
		Event:         "must-not-append",
	}); err == nil {
		t.Fatal("append accepted corrupt provenance")
	}
	data, _ := os.ReadFile(path)
	if string(data) != `{"schema_version":1}` {
		t.Fatalf("corrupt provenance was modified: %q", data)
	}
}

func TestReplayProvenanceRejectsSymlinkAndUnsafeParent(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "audit.jsonl")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := NewFileProvenanceSink(link, dir).Preflight(context.Background()); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink error = %v", err)
	}

	t.Run("unsafe parent", func(t *testing.T) {
		skipPOSIXModeAssertionsOnWindows(t)
		unsafe := filepath.Join(dir, "unsafe")
		if err := os.Mkdir(unsafe, 0755); err != nil {
			t.Fatal(err)
		}
		if err := NewFileProvenanceSink(filepath.Join(unsafe, "audit.jsonl"), dir).Preflight(context.Background()); err == nil || !strings.Contains(err.Error(), "not private") {
			t.Fatalf("unsafe parent error = %v", err)
		}
	})
}

func TestReplayProvenanceLockIsMandatory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	sink := NewFileProvenanceSink(path, dir)
	sink.lockTimeout = 40 * time.Millisecond
	sink.retryInterval = 5 * time.Millisecond
	if err := sink.Preflight(context.Background()); err != nil {
		t.Fatal(err)
	}
	unlock, err := acquireReplayFileLock(sink.LockPath(), time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if err := sink.Append(context.Background(), ReplayProvenanceRecord{
		SchemaVersion: 1,
		RecordedAt:    time.Now(),
		Event:         "blocked",
	}); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("append lock error = %v", err)
	}
}

func TestReplayProvenanceSharedOverrideUsesOneLockIdentity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.jsonl")
	a := NewFileProvenanceSink(path, dir)
	b := NewFileProvenanceSink(filepath.Clean(path), dir)
	if a.LockPath() != b.LockPath() {
		t.Fatalf("shared provenance paths use different locks: %s vs %s", a.LockPath(), b.LockPath())
	}
	if strings.Contains(filepath.Base(a.LockPath()), "shared") {
		t.Fatalf("lock filename exposes provenance identity: %s", a.LockPath())
	}
}

func TestReplayProvenanceRecordValidationAndCancellation(t *testing.T) {
	dir := t.TempDir()
	sink := NewFileProvenanceSink(filepath.Join(dir, "audit.jsonl"), dir)
	if err := sink.Append(context.Background(), ReplayProvenanceRecord{}); err == nil {
		t.Fatal("invalid record was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sink.Preflight(ctx); err == nil {
		t.Fatal("cancelled preflight was accepted")
	}
}
