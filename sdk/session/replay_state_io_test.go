package session

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReplayStatePrivateModesAndNoCredentialMaterial(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "replay")
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	state, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}

	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := dirInfo.Mode().Perm(); got != 0700 {
		t.Fatalf("replay dir mode = %04o, want 0700", got)
	}
	for _, path := range []string{store.StatePath(state.ContextKey), store.writerLockPath(req.Locator.ContextIdentityHash)} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0600 {
			t.Fatalf("%s mode = %04o, want 0600", path, got)
		}
	}
	data, err := os.ReadFile(store.StatePath(state.ContextKey))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"token_ref", "token-ref", "authorization", "dt0c01"} {
		if strings.Contains(strings.ToLower(string(data)), forbidden) {
			t.Fatalf("state contains forbidden credential material %q: %s", forbidden, data)
		}
	}
}

func TestReplayStateCorruptionAndSchemaVersionFailClosed(t *testing.T) {
	tests := []struct {
		name string
		edit func(t *testing.T, path string)
		want string
	}{
		{
			name: "malformed JSON",
			edit: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			},
			want: "corrupt",
		},
		{
			name: "unknown schema",
			edit: func(t *testing.T, path string) {
				t.Helper()
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var raw map[string]any
				if err := json.Unmarshal(data, &raw); err != nil {
					t.Fatal(err)
				}
				raw["schema_version"] = 99
				data, _ = json.Marshal(raw)
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			},
			want: "unsupported schema version 99",
		},
		{
			name: "unknown field",
			edit: func(t *testing.T, path string) {
				t.Helper()
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var raw map[string]any
				_ = json.Unmarshal(data, &raw)
				raw["unexpected"] = true
				data, _ = json.Marshal(raw)
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			},
			want: "unknown field",
		},
		{
			name: "configuration hash mismatch",
			edit: func(t *testing.T, path string) {
				t.Helper()
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var raw map[string]any
				_ = json.Unmarshal(data, &raw)
				raw["replay_config_hash"] = strings.Repeat("0", 64)
				data, _ = json.Marshal(raw)
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			},
			want: "replay_config_hash does not match",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
			store := NewReplayStateStore(dir, clock)
			req := replayTestRequest(t, dir, ReplayClockManual)
			state, err := store.Start(req)
			if err != nil {
				t.Fatal(err)
			}
			tt.edit(t, store.StatePath(state.ContextKey))
			if _, err := store.Status(req.Locator); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("status error = %v, want %q", err, tt.want)
			}
			// Explicit restart is the recovery path and never derives values from
			// the corrupt payload.
			req.Restart = true
			if _, err := store.Start(req); err != nil {
				t.Fatalf("restart did not recover corrupt state: %v", err)
			}
		})
	}
}

func TestReplayForeignUnreadableStateDoesNotBlockHealthyLifecycle(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	original, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	foreignReq := replayTestRequestForIdentity(
		t,
		dir,
		ReplayClockManual,
		"/synthetic/other-config.yaml",
		"other-context",
		"https://other.example.invalid",
	)
	foreign, err := store.Start(foreignReq)
	if err != nil {
		t.Fatal(err)
	}
	if foreign.ContextKey == original.ContextKey || foreign.ContextIdentityHash == original.ContextIdentityHash {
		t.Fatal("test setup did not create a distinct context key and identity")
	}
	foreignPath := store.StatePath(foreign.ContextKey)
	if err := os.WriteFile(foreignPath, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}

	status, err := store.Status(req.Locator)
	if err != nil || status.SessionID != original.SessionID {
		t.Fatalf("status was blocked by foreign state: state=%+v err=%v", status, err)
	}
	_, diagnostics, err := store.StatusWithDiagnostics(req.Locator)
	wantDiagnostic := filepath.Base(foreignPath)
	if err != nil || len(diagnostics.UnreadableStateFiles) != 1 || diagnostics.UnreadableStateFiles[0] != wantDiagnostic {
		t.Fatalf("diagnostics=%+v err=%v, want %q", diagnostics, err, wantDiagnostic)
	}
	advanced, err := store.Advance(req.Locator, time.Minute, req.ContextInputHash)
	if err != nil || advanced.Revision != original.Revision+1 {
		t.Fatalf("advance was blocked by foreign state: state=%+v err=%v", advanced, err)
	}
	stopped, err := store.Stop(req.Locator)
	if err != nil || stopped.Status != ReplayStatusStopped {
		t.Fatalf("stop was blocked by foreign state: state=%+v err=%v", stopped, err)
	}
	req.Restart = true
	restarted, err := store.Start(req)
	if err != nil || restarted.Status != ReplayStatusActive || restarted.SessionID == original.SessionID {
		t.Fatalf("restart was blocked by foreign state: state=%+v err=%v", restarted, err)
	}
	if raw, err := os.ReadFile(foreignPath); err != nil || string(raw) != "{" {
		t.Fatalf("healthy lifecycle changed foreign diagnostics: raw=%q err=%v", raw, err)
	}
}

func TestReplayForeignUnknownFieldDoesNotBlockOtherContext(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	original, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	foreignReq := replayTestRequestForIdentity(
		t,
		dir,
		ReplayClockManual,
		"/synthetic/future-config.yaml",
		"future-context",
		"https://future.example.invalid",
	)
	foreign, err := store.Start(foreignReq)
	if err != nil {
		t.Fatal(err)
	}
	foreignPath := store.StatePath(foreign.ContextKey)
	raw, err := os.ReadFile(foreignPath)
	if err != nil {
		t.Fatal(err)
	}
	var encoded map[string]any
	if err := json.Unmarshal(raw, &encoded); err != nil {
		t.Fatal(err)
	}
	encoded["future_writer_field"] = map[string]any{"enabled": true}
	raw, err = json.Marshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(foreignPath, raw, 0600); err != nil {
		t.Fatal(err)
	}

	status, diagnostics, err := store.StatusWithDiagnostics(req.Locator)
	if err != nil || status.SessionID != original.SessionID {
		t.Fatalf("version-skew state blocked healthy context: state=%+v err=%v", status, err)
	}
	wantDiagnostic := filepath.Base(foreignPath)
	if len(diagnostics.UnreadableStateFiles) != 1 || diagnostics.UnreadableStateFiles[0] != wantDiagnostic {
		t.Fatalf("diagnostics=%+v, want %q", diagnostics, wantDiagnostic)
	}
}

func TestReplayStateSymlinksFailClosed(t *testing.T) {
	t.Run("state file", func(t *testing.T) {
		dir := t.TempDir()
		clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
		store := NewReplayStateStore(dir, clock)
		req := replayTestRequest(t, dir, ReplayClockManual)
		state, err := store.Start(req)
		if err != nil {
			t.Fatal(err)
		}
		path := store.StatePath(state.ContextKey)
		backup := path + ".backup"
		if err := os.Rename(path, backup); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(backup, path); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Status(req.Locator); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("symlink status error = %v", err)
		}
	})

	t.Run("state directory", func(t *testing.T) {
		base := t.TempDir()
		target := filepath.Join(base, "target")
		if err := os.Mkdir(target, 0700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(base, "replay")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		store := NewReplayStateStore(link, newReplayFakeClock(time.Now()))
		req := replayTestRequest(t, link, ReplayClockManual)
		if _, err := store.Start(req); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("symlink directory error = %v", err)
		}
	})
}

func TestReplayStatusAndReadActiveAreLockFreeAndReadOnly(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	state, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	path := store.StatePath(state.ContextKey)
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	unlock, err := acquireReplayFileLock(store.writerLockPath(req.Locator.ContextIdentityHash), time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	done := make(chan error, 1)
	go func() {
		if _, err := store.Status(req.Locator); err != nil {
			done <- err
			return
		}
		_, err := store.ReadActive(req.Locator)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("lock-free read waited for writer lock")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.ModTime() != before.ModTime() || after.Mode().Perm() != 0400 {
		t.Fatal("status/read-active modified the state file")
	}
}

func TestReplayConcurrentReadersSeeOnlyCompleteSnapshots(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	writer := NewReplayStateStore(dir, clock)
	reader := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	if _, err := writer.Start(req); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	readErr := make(chan error, 1)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				state, err := reader.Status(req.Locator)
				if err != nil {
					select {
					case readErr <- err:
					default:
					}
					return
				}
				if state.Revision == 0 || state.AnchorVirtual.Before(state.DataStart) || state.AnchorVirtual.After(state.DataEnd) {
					select {
					case readErr <- errors.New("reader observed incomplete state"):
					default:
					}
					return
				}
			}
		}()
	}
	for range 100 {
		if _, err := writer.Advance(req.Locator, time.Millisecond, req.ContextInputHash); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
	select {
	case err := <-readErr:
		t.Fatal(err)
	default:
	}
	final, err := reader.Status(req.Locator)
	if err != nil {
		t.Fatal(err)
	}
	if final.Revision != 101 {
		t.Fatalf("final revision = %d, want 101", final.Revision)
	}
}

func TestReplayConcurrentLifecycleOperationsLeaveValidState(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	req := replayTestRequest(t, dir, ReplayClockManual)

	var wg sync.WaitGroup
	var starts atomic.Int32
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store := NewReplayStateStore(dir, clock)
			if _, err := store.Start(req); err == nil {
				starts.Add(1)
			}
		}()
	}
	wg.Wait()
	if starts.Load() != 1 {
		t.Fatalf("successful concurrent starts = %d, want 1", starts.Load())
	}

	req.Restart = true
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = NewReplayStateStore(dir, clock).Start(req)
		}()
	}
	wg.Wait()
	store := NewReplayStateStore(dir, clock)
	state, err := store.Status(req.Locator)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != ReplayStatusActive || state.SessionID == "" {
		t.Fatalf("invalid state after concurrent restarts: %+v", state)
	}

	// Advance and stop serialize through the same identity lock. Either order
	// is valid; the resulting file must remain a complete stopped snapshot.
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = NewReplayStateStore(dir, clock).Advance(req.Locator, time.Minute, req.ContextInputHash)
	}()
	go func() {
		defer wg.Done()
		_, _ = NewReplayStateStore(dir, clock).Stop(req.Locator)
	}()
	wg.Wait()
	state, err = store.Status(req.Locator)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != ReplayStatusStopped || state.StoppedAt == nil || state.FinalVirtualNow == nil {
		t.Fatalf("invalid final concurrent state: %+v", state)
	}
}

func TestReplayRestartRefusesAmbiguousMultipleLiveStates(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	store.retryInterval = time.Millisecond
	req := replayTestRequest(t, dir, ReplayClockManual)
	original, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a process dying in the narrow cross-key restart window after
	// publishing the new complete snapshot but before removing the old one.
	replacement := original
	replacement.SessionID = strings.Repeat("f", 32)
	replacement.ContextKey = ContextKey(strings.Repeat("b", 64))
	replacement.Revision++
	if err := store.write(replacement); err != nil {
		t.Fatal(err)
	}

	req.Restart = true
	if _, err := store.Start(req); err == nil || !strings.Contains(err.Error(), "multiple live replay states") {
		t.Fatalf("ambiguous restart error = %v", err)
	}
	if _, err := os.Stat(store.StatePath(original.ContextKey)); err != nil {
		t.Fatalf("ambiguous recovery changed the original state: %v", err)
	}
	if _, err := os.Stat(store.StatePath(replacement.ContextKey)); err != nil {
		t.Fatalf("ambiguous recovery changed the replacement state: %v", err)
	}
}

func TestReplayRestartRefusesCorruptCurrentStateWhenAnotherMatchExists(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	original, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	replacement := original
	replacement.SessionID = strings.Repeat("e", 32)
	replacement.ContextKey = ContextKey(strings.Repeat("c", 64))
	replacement.Revision++
	if err := store.write(replacement); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.StatePath(original.ContextKey), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}

	req.Restart = true
	if _, err := store.Start(req); err == nil || !strings.Contains(err.Error(), "lookup is ambiguous") {
		t.Fatalf("corrupt ambiguous restart error = %v", err)
	}
	data, err := os.ReadFile(store.StatePath(original.ContextKey))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "{" {
		t.Fatalf("ambiguous restart overwrote corrupt diagnostics: %q", data)
	}
	if got, err := store.read(replacement.ContextKey); err != nil || got.SessionID != replacement.SessionID {
		t.Fatalf("ambiguous restart changed matching state: state=%+v err=%v", got, err)
	}
}
