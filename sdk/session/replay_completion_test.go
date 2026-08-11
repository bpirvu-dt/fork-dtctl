package session

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func terminalReplaySession(t *testing.T) (*ReplayStateStore, *replayFakeClock, ReplayStartRequest, ReplaySession) {
	t.Helper()
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	started, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := store.Advance(req.Locator, started.DataEnd.Sub(started.VirtualStart), req.ContextInputHash)
	if err != nil {
		t.Fatal(err)
	}
	return store, clock, req, terminal
}

func TestReplayGuardedCompletionRecordsAndIsIdempotent(t *testing.T) {
	store, clock, req, terminal := terminalReplaySession(t)
	completedAt := clock.Now().Add(time.Minute)
	completed, disposition, err := store.MarkCompleted(req.Locator, terminal.SessionID, completedAt)
	if err != nil {
		t.Fatal(err)
	}
	if disposition != CompletionRecorded || completed.Status != ReplayStatusCompleted || completed.CompletedAt == nil || !completed.CompletedAt.Equal(completedAt) {
		t.Fatalf("completion = %+v, %s", completed, disposition)
	}

	again, disposition, err := store.MarkCompleted(req.Locator, terminal.SessionID, completedAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if disposition != CompletionAlreadyCompleted || again.CompletedAt == nil || !again.CompletedAt.Equal(completedAt) || again.Revision != completed.Revision {
		t.Fatalf("idempotent completion = %+v, %s", again, disposition)
	}
	if _, err := store.Start(req); err == nil || !strings.Contains(err.Error(), "is completed") {
		t.Fatalf("normal start replaced completed session: %v", err)
	}
}

func TestReplayConcurrentGuardedCompletionPreservesFirstTimestamp(t *testing.T) {
	store, clock, req, terminal := terminalReplaySession(t)
	times := []time.Time{clock.Now().Add(time.Second), clock.Now().Add(2 * time.Second)}
	type result struct {
		state       ReplaySession
		disposition CompletionDisposition
		err         error
	}
	results := make(chan result, len(times))
	var wg sync.WaitGroup
	for _, completionTime := range times {
		wg.Add(1)
		go func(now time.Time) {
			defer wg.Done()
			independent := NewReplayStateStore(store.dir, store.clock)
			state, disposition, err := independent.MarkCompleted(req.Locator, terminal.SessionID, now)
			results <- result{state, disposition, err}
		}(completionTime)
	}
	wg.Wait()
	close(results)

	counts := map[CompletionDisposition]int{}
	var first time.Time
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		counts[result.disposition]++
		if result.disposition == CompletionRecorded {
			first = *result.state.CompletedAt
		}
	}
	if counts[CompletionRecorded] != 1 || counts[CompletionAlreadyCompleted] != 1 {
		t.Fatalf("completion dispositions = %+v", counts)
	}
	final, err := store.Status(req.Locator)
	if err != nil {
		t.Fatal(err)
	}
	if final.CompletedAt == nil || !final.CompletedAt.Equal(first) {
		t.Fatalf("completed_at = %v, want first %s", final.CompletedAt, first)
	}
}

func TestReplayGuardedCompletionCannotCompleteStoppedSession(t *testing.T) {
	store, clock, req, terminal := terminalReplaySession(t)
	stopped, err := store.Stop(req.Locator)
	if err != nil {
		t.Fatal(err)
	}
	state, disposition, err := store.MarkCompleted(req.Locator, terminal.SessionID, clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if disposition != CompletionSessionReplaced || state.Status != ReplayStatusStopped || state.Revision != stopped.Revision {
		t.Fatalf("stopped completion = %+v, %s", state, disposition)
	}
}

func TestReplayGuardedCompletionCannotCompleteRestartedOrDifferentSession(t *testing.T) {
	store, clock, req, terminal := terminalReplaySession(t)
	req.Restart = true
	replacement, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	state, disposition, err := store.MarkCompleted(req.Locator, terminal.SessionID, clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if disposition != CompletionSessionReplaced || state.SessionID != replacement.SessionID || state.Status != ReplayStatusActive || state.Revision != replacement.Revision {
		t.Fatalf("restarted completion = %+v, %s", state, disposition)
	}

	state, disposition, err = store.MarkCompleted(req.Locator, "00000000000000000000000000000000", clock.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if disposition != CompletionSessionReplaced || state.SessionID != replacement.SessionID {
		t.Fatalf("different-ID completion = %+v, %s", state, disposition)
	}
}

func TestReplayFailedTerminalAttemptWritesNoState(t *testing.T) {
	store, _, req, terminal := terminalReplaySession(t)
	// A failed query does not call MarkCompleted. Reading from an independent
	// store demonstrates that no hidden lifecycle write occurred.
	other := NewReplayStateStore(store.dir, store.clock)
	after, err := other.Status(req.Locator)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != ReplayStatusTerminalReady || after.Revision != terminal.Revision || after.CompletedAt != nil {
		t.Fatalf("failed terminal attempt changed state: %+v", after)
	}
}

func TestReplayGuardedCompletionRequiresTerminalReady(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	started, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.MarkCompleted(req.Locator, started.SessionID, clock.Now()); err == nil {
		t.Fatal("non-terminal completion unexpectedly succeeded")
	}
	after, _ := store.Status(req.Locator)
	if after.Revision != started.Revision || after.Status != ReplayStatusActive {
		t.Fatal("failed completion changed state")
	}
}
