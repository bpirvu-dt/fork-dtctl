package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReplayWriterLockTimeoutsAreMandatory(t *testing.T) {
	tests := []struct {
		name string
		run  func(*ReplayStateStore, ReplayStartRequest) error
	}{
		{
			name: "start",
			run: func(store *ReplayStateStore, req ReplayStartRequest) error {
				_, err := store.Start(req)
				return err
			},
		},
		{
			name: "advance",
			run: func(store *ReplayStateStore, req ReplayStartRequest) error {
				_, err := store.Advance(req.Locator, time.Minute, req.ContextInputHash)
				return err
			},
		},
		{
			name: "stop",
			run: func(store *ReplayStateStore, req ReplayStartRequest) error {
				_, err := store.Stop(req.Locator)
				return err
			},
		},
		{
			name: "restart",
			run: func(store *ReplayStateStore, req ReplayStartRequest) error {
				req.Restart = true
				_, err := store.Start(req)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
			store := NewReplayStateStore(dir, clock)
			store.lockTimeout = 40 * time.Millisecond
			store.retryInterval = 5 * time.Millisecond
			req := replayTestRequest(t, dir, ReplayClockManual)
			if tt.name != "start" {
				if _, err := store.Start(req); err != nil {
					t.Fatal(err)
				}
			}
			if err := ensurePrivateReplayDirectory(dir); err != nil {
				t.Fatal(err)
			}
			unlock, err := acquireReplayFileLock(store.writerLockPath(req.Locator.ContextIdentityHash), time.Second, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer unlock()

			err = tt.run(store, req)
			if err == nil || !strings.Contains(err.Error(), "timed out") {
				t.Fatalf("lock error = %v", err)
			}
		})
	}
}

func TestReplayWriterLockRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, store.writerLockPath(req.Locator.ContextIdentityHash)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Start(req); err == nil {
		t.Fatal("start proceeded with symlinked mandatory writer lock")
	}
}

func TestReplayWriterLockFileMode(t *testing.T) {
	dir := t.TempDir()
	if err := ensurePrivateReplayDirectory(dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, strings.Repeat("a", 64)+".lock")
	unlock, err := acquireReplayFileLock(path, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	unlock()
	assertReplayPrivatePath(t, path, 0600)
}
