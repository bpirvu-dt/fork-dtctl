package session

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type replayFakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func newReplayFakeClock(now time.Time) *replayFakeClock {
	return &replayFakeClock{now: now.UTC()}
}

func (c *replayFakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *replayFakeClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now.UTC()
	c.mu.Unlock()
}

func replayTestRequest(t *testing.T, dir string, mode string) ReplayStartRequest {
	t.Helper()
	source := "/synthetic/config.yaml"
	name := "historical"
	environment := "https://tenant.example.invalid"
	key := NewReplayContextKey(source, name, environment)
	resolved, err := ResolveReplayConfig(&ReplayConfig{
		DataStart:    "2026-06-14T08:00:00Z",
		DataEnd:      "2026-06-14T12:00:00Z",
		VirtualStart: "2026-06-14T10:00:00Z",
		ClockMode:    mode,
	}, ReplayConfigOverrides{}, dir, key)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &Context{
		Environment: environment,
		SafetyLevel: SafetyLevelReadOnly,
		Profile:     ProfileReplay,
		Replay: &ReplayConfig{
			DataStart:    "2026-06-14T08:00:00Z",
			DataEnd:      "2026-06-14T12:00:00Z",
			VirtualStart: "2026-06-14T10:00:00Z",
			ClockMode:    mode,
		},
	}
	return ReplayStartRequest{
		Locator: ReplayLocator{
			ContextKey:          key,
			ContextIdentityHash: ReplayContextIdentityHash(source, name),
		},
		ContextName:      name,
		EnvironmentHash:  ReplayEnvironmentHash(environment),
		ContextInputHash: ReplayContextInputHash(ctx),
		Config:           resolved,
	}
}

func TestReplayRealtimeClockAndTwoStoreInstances(t *testing.T) {
	dir := t.TempDir()
	hostStart := time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC)
	clock := newReplayFakeClock(hostStart)
	first := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, "")
	state, err := first.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if state.ClockMode != ReplayClockRealtime || !state.SessionStartedAt.Equal(hostStart) || !state.AnchorHost.Equal(hostStart) {
		t.Fatalf("unexpected start state: %+v", state)
	}
	if got := VirtualNow(state, hostStart); !got.Equal(state.VirtualStart) {
		t.Fatalf("immediate virtual now = %s, want %s", got, state.VirtualStart)
	}

	clock.Set(hostStart.Add(time.Hour))
	second := NewReplayStateStore(dir, clock)
	observed, err := second.Status(req.Locator)
	if err != nil {
		t.Fatal(err)
	}
	if observed.SessionID != state.SessionID {
		t.Fatalf("second store saw session %q, want %q", observed.SessionID, state.SessionID)
	}
	want := time.Date(2026, 6, 14, 11, 0, 0, 0, time.UTC)
	if got := VirtualNow(observed, clock.Now()); !got.Equal(want) {
		t.Fatalf("virtual now = %s, want %s", got, want)
	}
}

func TestReplayManualClockAndAdvance(t *testing.T) {
	dir := t.TempDir()
	hostStart := time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC)
	clock := newReplayFakeClock(hostStart)
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	state, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(hostStart.Add(48 * time.Hour))
	if got := VirtualNow(state, clock.Now()); !got.Equal(state.VirtualStart) {
		t.Fatalf("manual virtual now moved with host time: %s", got)
	}

	advanced, err := store.Advance(req.Locator, 30*time.Minute, req.ContextInputHash)
	if err != nil {
		t.Fatal(err)
	}
	want := state.VirtualStart.Add(30 * time.Minute)
	if !advanced.AnchorVirtual.Equal(want) || !VirtualNow(advanced, clock.Now()).Equal(want) {
		t.Fatalf("manual advance = %s, want %s", advanced.AnchorVirtual, want)
	}
	clock.Set(clock.Now().Add(time.Hour))
	if got := VirtualNow(advanced, clock.Now()); !got.Equal(want) {
		t.Fatalf("advanced manual clock moved: %s", got)
	}
}

func TestReplayRealtimeAdvanceJumpsAndContinues(t *testing.T) {
	dir := t.TempDir()
	hostStart := time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC)
	clock := newReplayFakeClock(hostStart)
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockRealtime)
	state, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(hostStart.Add(15 * time.Minute))
	advanced, err := store.Advance(req.Locator, 30*time.Minute, req.ContextInputHash)
	if err != nil {
		t.Fatal(err)
	}
	wantAnchor := state.VirtualStart.Add(45 * time.Minute)
	if !advanced.AnchorHost.Equal(clock.Now()) || !advanced.AnchorVirtual.Equal(wantAnchor) {
		t.Fatalf("realtime anchors = %s/%s, want %s/%s", advanced.AnchorHost, advanced.AnchorVirtual, clock.Now(), wantAnchor)
	}
	clock.Set(clock.Now().Add(10 * time.Minute))
	if got, want := VirtualNow(advanced, clock.Now()), wantAnchor.Add(10*time.Minute); !got.Equal(want) {
		t.Fatalf("continued virtual time = %s, want %s", got, want)
	}
}

func TestReplayAdvanceTerminalAndOvershoot(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	state, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}

	before, err := store.Status(req.Locator)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(req.Locator, 2*time.Hour+time.Nanosecond, req.ContextInputHash); err == nil || !strings.Contains(err.Error(), "remaining duration is 2h0m0s") {
		t.Fatalf("overshoot error = %v", err)
	}
	after, _ := store.Status(req.Locator)
	if after.Revision != before.Revision || !after.AnchorVirtual.Equal(before.AnchorVirtual) {
		t.Fatal("overshoot changed state")
	}

	terminal, err := store.Advance(req.Locator, state.DataEnd.Sub(state.VirtualStart), req.ContextInputHash)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Status != ReplayStatusTerminalReady || !VirtualNow(terminal, clock.Now()).Equal(state.DataEnd) {
		t.Fatalf("terminal state = %+v", terminal)
	}
	clock.Set(clock.Now().Add(24 * time.Hour))
	if got := VirtualNow(terminal, clock.Now()); !got.Equal(state.DataEnd) {
		t.Fatalf("virtual time exceeded data_end: %s", got)
	}
}

func TestReplayStopAndRestart(t *testing.T) {
	dir := t.TempDir()
	hostStart := time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC)
	clock := newReplayFakeClock(hostStart)
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockRealtime)
	first, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Start(req); err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("second normal start error = %v", err)
	}

	clock.Set(hostStart.Add(30 * time.Minute))
	stopped, err := store.Stop(req.Locator)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.Status != ReplayStatusStopped || stopped.StoppedAt == nil || stopped.FinalVirtualNow == nil {
		t.Fatalf("stopped state = %+v", stopped)
	}
	if want := first.VirtualStart.Add(30 * time.Minute); !stopped.FinalVirtualNow.Equal(want) {
		t.Fatalf("final virtual = %s, want %s", stopped.FinalVirtualNow, want)
	}

	second, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if second.SessionID == first.SessionID {
		t.Fatal("normal start after stop reused session ID")
	}
	req.Restart = true
	third, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if third.SessionID == second.SessionID || !third.AnchorVirtual.Equal(req.Config.VirtualStart) {
		t.Fatal("restart did not create and reset a session")
	}
}

func TestReplayAdvanceDetectsContextAndEnvironmentDrift(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	started, err := store.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(req.Locator, time.Minute, strings.Repeat("f", 64)); err == nil || !strings.Contains(err.Error(), "context changed") {
		t.Fatalf("context drift error = %v", err)
	}
	unchanged, _ := store.Status(req.Locator)
	if unchanged.Revision != started.Revision {
		t.Fatal("drifted advance changed state")
	}

	newLocator := ReplayLocator{
		ContextKey:          NewReplayContextKey("/synthetic/config.yaml", "historical", "https://changed.example.invalid"),
		ContextIdentityHash: req.Locator.ContextIdentityHash,
	}
	found, err := store.Status(newLocator)
	if err != nil {
		t.Fatalf("environment-drift lookup did not find old state: %v", err)
	}
	if found.SessionID != started.SessionID {
		t.Fatalf("drift lookup found session %q, want %q", found.SessionID, started.SessionID)
	}
	if _, err := store.Advance(newLocator, time.Minute, strings.Repeat("e", 64)); err == nil || !strings.Contains(err.Error(), "context changed") {
		t.Fatalf("environment drift advance error = %v", err)
	}

	restart := req
	restart.Locator = newLocator
	restart.EnvironmentHash = ReplayEnvironmentHash("https://changed.example.invalid")
	restart.ContextInputHash = strings.Repeat("e", 64)
	restart.Restart = true
	replaced, err := store.Start(restart)
	if err != nil {
		t.Fatalf("restart after environment drift: %v", err)
	}
	if replaced.SessionID == started.SessionID || replaced.ContextKey != newLocator.ContextKey {
		t.Fatalf("environment-drift restart did not replace state: %+v", replaced)
	}
	if _, err := os.Stat(store.StatePath(req.Locator.ContextKey)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old environment-keyed state remains after restart: %v", err)
	}
}

func TestVirtualNowPureModesAndClamp(t *testing.T) {
	base := ReplaySession{
		DataStart:     time.Date(2026, 6, 14, 8, 0, 0, 0, time.UTC),
		DataEnd:       time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC),
		AnchorHost:    time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC),
		AnchorVirtual: time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC),
		Status:        ReplayStatusActive,
	}
	base.ClockMode = ReplayClockManual
	if got := VirtualNow(base, base.AnchorHost.Add(time.Hour)); !got.Equal(base.AnchorVirtual) {
		t.Fatalf("manual = %s", got)
	}
	base.ClockMode = ReplayClockRealtime
	if got := VirtualNow(base, base.AnchorHost.Add(time.Hour)); !got.Equal(base.AnchorVirtual.Add(time.Hour)) {
		t.Fatalf("realtime = %s", got)
	}
	if got := VirtualNow(base, base.AnchorHost.Add(10*time.Hour)); !got.Equal(base.DataEnd) {
		t.Fatalf("clamp = %s", got)
	}
	if got := VisibleEnd(base, base.AnchorHost.Add(-10*time.Hour)); !got.Equal(base.DataStart) {
		t.Fatalf("backward visible end = %s", got)
	}
}

func TestReplayReadActiveRejectsTerminalStates(t *testing.T) {
	dir := t.TempDir()
	clock := newReplayFakeClock(time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	store := NewReplayStateStore(dir, clock)
	req := replayTestRequest(t, dir, ReplayClockManual)
	if _, err := store.Start(req); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadActive(req.Locator); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stop(req.Locator); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadActive(req.Locator); err == nil {
		t.Fatal("ReadActive accepted stopped state")
	}
	missing := req.Locator
	missing.ContextIdentityHash = strings.Repeat("0", 64)
	if _, err := store.Status(missing); !errors.Is(err, ErrReplaySessionNotFound) {
		t.Fatalf("missing status error = %v", err)
	}
}
