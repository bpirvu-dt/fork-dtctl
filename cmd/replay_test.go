package cmd

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/dynatrace-oss/dtctl/pkg/config"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

type replayCLIFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *replayCLIFakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *replayCLIFakeClock) Add(by time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(by)
	c.mu.Unlock()
}

type replayCLIResult struct {
	stdout string
	stderr string
	err    error
}

func configureReplayCLI(t *testing.T, configPath, stateDir string, clock session.Clock) {
	t.Helper()
	origConfig, origContext, origFormat := cfgFile, contextName, outputFormat
	origAgent, origNoAgent, origPlain, origJQ := agentMode, noAgent, plainMode, jqFilter
	origClock, origStateDir := replayClock, replayStateDirectory
	t.Cleanup(func() {
		cfgFile, contextName, outputFormat = origConfig, origContext, origFormat
		agentMode, noAgent, plainMode, jqFilter = origAgent, origNoAgent, origPlain, origJQ
		replayClock, replayStateDirectory = origClock, origStateDir
	})
	cfgFile = configPath
	contextName = ""
	outputFormat = "table"
	agentMode, noAgent, plainMode, jqFilter = false, true, false, ""
	replayClock = clock
	replayStateDirectory = stateDir
}

func runReplayCLI(t *testing.T, format string, args ...string) replayCLIResult {
	t.Helper()
	oldFormat := outputFormat
	outputFormat = format
	defer func() { outputFormat = oldFormat }()
	cmd := newReplayCommand()
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return replayCLIResult{stdout: stdout.String(), stderr: stderr.String(), err: err}
}

func writeReplayCLIConfig(t *testing.T, path string, cfg *config.Config) {
	t.Helper()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func replayCLIConfig(raw *config.ReplayConfig) *config.Config {
	return &config.Config{
		APIVersion:     config.CurrentAPIVersion,
		Kind:           "Config",
		CurrentContext: "historical-window",
		Contexts: []config.NamedContext{
			{
				Name: "historical-window",
				Context: config.Context{
					Environment: "https://tenant.example.invalid",
					TokenRef:    "synthetic-reader",
					SafetyLevel: config.SafetyLevelReadOnly,
					Profile:     config.ProfileReplay,
					Replay:      raw,
				},
			},
		},
	}
}

func standardReplayBlock(mode string) *config.ReplayConfig {
	return &config.ReplayConfig{
		DataStart:    "2026-06-14T08:00:00+02:00",
		DataEnd:      "2026-06-14T12:00:00+02:00",
		VirtualStart: "2026-06-14T10:00:00+02:00",
		ClockMode:    mode,
	}
}

func replayLocatorForConfig(t *testing.T, path string) (session.ReplayLocator, *config.Context) {
	t.Helper()
	cfg, err := config.LoadFrom(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := cfg.CurrentContextObj()
	if err != nil {
		t.Fatal(err)
	}
	source, err := cfg.SourceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return session.ReplayLocator{
		ContextKey:          session.NewReplayContextKey(source, cfg.CurrentContext, ctx.Environment),
		ContextIdentityHash: session.ReplayContextIdentityHash(source, cfg.CurrentContext),
	}, ctx
}

func TestReplayCLILifecycleRealtimeAndIndependentStores(t *testing.T) {
	t.Setenv(config.ProfileEnvVar, "")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	stateDir := filepath.Join(dir, "state", "replay")
	writeReplayCLIConfig(t, configPath, replayCLIConfig(standardReplayBlock("")))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, stateDir, clock)

	started := runReplayCLI(t, "table", "start")
	if started.err != nil {
		t.Fatal(started.err)
	}
	if !strings.Contains(started.stdout, "realtime") || !strings.Contains(started.stdout, "2026-06-14T08:00:00Z") {
		t.Fatalf("start output missing normalized clock details:\n%s", started.stdout)
	}

	locator, _ := replayLocatorForConfig(t, configPath)
	firstStore := session.NewReplayStateStore(stateDir, clock)
	secondStore := session.NewReplayStateStore(stateDir, clock)
	first, err := firstStore.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	second, err := secondStore.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if first.SessionID != second.SessionID || first.ClockMode != session.ReplayClockRealtime {
		t.Fatalf("independent stores disagree: first=%+v second=%+v", first, second)
	}
	if !first.SessionStartedAt.Equal(clock.Now()) || !session.VirtualNow(first, clock.Now()).Equal(first.VirtualStart) {
		t.Fatal("realtime anchors were not captured at successful start")
	}

	clock.Add(10 * time.Minute)
	advanced := runReplayCLI(t, "json", "advance", "30m")
	if advanced.err != nil {
		t.Fatal(advanced.err)
	}
	state, err := secondStore.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	wantVirtual := time.Date(2026, 6, 14, 8, 40, 0, 0, time.UTC)
	if got := session.VirtualNow(state, clock.Now()); !got.Equal(wantVirtual) {
		t.Fatalf("virtual now after realtime advance = %s, want %s", got, wantVirtual)
	}

	status := runReplayCLI(t, "json", "status")
	if status.err != nil {
		t.Fatal(status.err)
	}
	var decoded ReplayStatusOutput
	if err := json.Unmarshal([]byte(status.stdout), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Status != session.ReplayStatusActive || decoded.ConfigurationDrift {
		t.Fatalf("unexpected status: %+v", decoded)
	}

	clock.Add(5 * time.Minute)
	stopped := runReplayCLI(t, "yaml", "stop")
	if stopped.err != nil {
		t.Fatal(stopped.err)
	}
	var stoppedView ReplayStatusOutput
	if err := yaml.Unmarshal([]byte(stopped.stdout), &stoppedView); err != nil {
		t.Fatal(err)
	}
	if stoppedView.Status != session.ReplayStatusStopped || stoppedView.StoppedAt == "" || stoppedView.FinalVirtualNow == "" {
		t.Fatalf("stopped status output = %+v", stoppedView)
	}
	state, err = firstStore.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != session.ReplayStatusStopped || state.StoppedAt == nil || state.FinalVirtualNow == nil {
		t.Fatalf("stop did not persist terminal metadata: %+v", state)
	}

	restarted := runReplayCLI(t, "json", "start")
	if restarted.err != nil {
		t.Fatal(restarted.err)
	}
	state, err = secondStore.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if state.SessionID == first.SessionID {
		t.Fatal("normal start after stop must allocate a new session ID")
	}
	activeID := state.SessionID
	if result := runReplayCLI(t, "table", "start"); result.err == nil || !strings.Contains(result.err.Error(), "already active") {
		t.Fatalf("normal active start error = %v", result.err)
	}
	if result := runReplayCLI(t, "table", "start", "--restart"); result.err != nil {
		t.Fatal(result.err)
	}
	state, err = firstStore.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if state.SessionID == activeID || !state.AnchorVirtual.Equal(state.VirtualStart) {
		t.Fatal("--restart did not replace and reset the active session")
	}
}

func TestReplayCLIManualFlagsPrecedenceAndEmptyWarning(t *testing.T) {
	t.Setenv(config.ProfileEnvVar, "")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	cfg := replayCLIConfig(standardReplayBlock(session.ReplayClockRealtime))
	writeReplayCLIConfig(t, configPath, cfg)
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), clock)

	result := runReplayCLI(t, "table", "start",
		"--data-start", "2026-07-01T00:00:00Z",
		"--data-end", "2026-07-01T02:00:00Z",
		"--virtual-start", "2026-07-01T00:00:00Z",
		"--clock-mode", "manual")
	if result.err != nil {
		t.Fatal(result.err)
	}
	if !strings.Contains(result.stderr, "visible replay interval starts empty") || !strings.Contains(result.stderr, "Manual mode requires 'dtctl replay advance'") {
		t.Fatalf("manual empty warning missing: %s", result.stderr)
	}

	locator, _ := replayLocatorForConfig(t, configPath)
	store := session.NewReplayStateStore(replayStateDirectory, clock)
	state, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if state.ClockMode != session.ReplayClockManual || state.ValueSources.ClockMode != session.ReplayValueFromFlag || state.ValueSources.DataStart != session.ReplayValueFromFlag {
		t.Fatalf("flag precedence was not recorded: %+v", state.ValueSources)
	}
	clock.Add(time.Hour)
	if got := session.VirtualNow(state, clock.Now()); !got.Equal(state.VirtualStart) {
		t.Fatalf("manual clock moved with host time: %s", got)
	}
	if advance := runReplayCLI(t, "table", "advance", "15m"); advance.err != nil {
		t.Fatal(advance.err)
	}
	state, err = store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if got := session.VirtualNow(state, clock.Now()); !got.Equal(state.VirtualStart.Add(15 * time.Minute)) {
		t.Fatalf("manual advance = %s", got)
	}
}

func TestReplayCLIStartFromFlagsOnlyDefaultsToFullRealtime(t *testing.T) {
	t.Setenv(config.ProfileEnvVar, "")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	writeReplayCLIConfig(t, configPath, replayCLIConfig(nil))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 11, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), clock)

	result := runReplayCLI(t, "json", "start",
		"--data-start", "2026-06-14T08:00:00Z",
		"--data-end", "2026-06-14T12:00:00Z")
	if result.err != nil {
		t.Fatal(result.err)
	}
	if !strings.Contains(result.stderr, "visible replay interval starts empty") || !strings.Contains(result.stderr, "Realtime mode reveals stored telemetry as the clock advances") {
		t.Fatalf("realtime empty warning missing: %s", result.stderr)
	}
	locator, _ := replayLocatorForConfig(t, configPath)
	state, err := session.NewReplayStateStore(replayStateDirectory, clock).Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if state.ClockMode != session.ReplayClockRealtime || state.Disclosure != session.ReplayDisclosureFull || state.ProvenancePath != "" {
		t.Fatalf("flags-only defaults = %+v", state)
	}
	if _, err := os.Stat(replayStateDirectory); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(replayStateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "provenance") {
			t.Fatalf("full disclosure created provenance artifact %q", entry.Name())
		}
	}
}

func TestReplayCLIAdvanceValidationAndTerminalBoundary(t *testing.T) {
	t.Setenv(config.ProfileEnvVar, "")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	raw := &config.ReplayConfig{
		DataStart:    "2026-06-14T08:00:00Z",
		DataEnd:      "2026-06-14T09:00:00Z",
		VirtualStart: "2026-06-14T08:00:00Z",
		ClockMode:    session.ReplayClockManual,
	}
	writeReplayCLIConfig(t, configPath, replayCLIConfig(raw))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), clock)
	if err := runReplayCLI(t, "table", "start").err; err != nil {
		t.Fatal(err)
	}

	for _, value := range []string{"0", "-1m", "tomorrow"} {
		result := runReplayCLI(t, "table", "advance", value)
		if result.err == nil {
			t.Fatalf("advance %q unexpectedly succeeded", value)
		}
	}
	locator, _ := replayLocatorForConfig(t, configPath)
	store := session.NewReplayStateStore(replayStateDirectory, clock)
	before, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	overshoot := runReplayCLI(t, "table", "advance", "61m")
	if overshoot.err == nil || !strings.Contains(overshoot.err.Error(), "remaining duration is 1h0m0s") {
		t.Fatalf("overshoot error = %v", overshoot.err)
	}
	after, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != before.Revision || !after.AnchorVirtual.Equal(before.AnchorVirtual) {
		t.Fatal("overshoot changed state")
	}
	exact := runReplayCLI(t, "json", "advance", "1h")
	if exact.err != nil {
		t.Fatal(exact.err)
	}
	var terminalView ReplayStatusOutput
	if err := json.Unmarshal([]byte(exact.stdout), &terminalView); err != nil {
		t.Fatal(err)
	}
	if terminalView.Status != session.ReplayStatusTerminalReady || terminalView.Position != "terminal-boundary" {
		t.Fatalf("terminal-ready status output = %+v", terminalView)
	}
	terminal, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Status != session.ReplayStatusTerminalReady || !session.VirtualNow(terminal, clock.Now()).Equal(terminal.DataEnd) {
		t.Fatalf("exact end = %+v", terminal)
	}
}

func TestReplayCLIRestrictedProvenanceAndStoredDriftAuthority(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode assertions")
	}
	t.Setenv(config.ProfileEnvVar, "")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	stateDir := filepath.Join(dir, "private-state", "replay")
	raw := standardReplayBlock(session.ReplayClockManual)
	raw.Disclosure = session.ReplayDisclosureRestricted
	cfg := replayCLIConfig(raw)
	writeReplayCLIConfig(t, configPath, cfg)
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 13, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, stateDir, clock)
	if result := runReplayCLI(t, "table", "start"); result.err != nil {
		t.Fatal(result.err)
	}
	locator, _ := replayLocatorForConfig(t, configPath)
	store := session.NewReplayStateStore(stateDir, clock)
	state, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if state.Disclosure != session.ReplayDisclosureRestricted || filepath.Dir(state.ProvenancePath) != stateDir {
		t.Fatalf("restricted route = %+v", state)
	}
	base := filepath.Base(state.ProvenancePath)
	if strings.Contains(base, "historical") || strings.Contains(base, "tenant") || !strings.HasPrefix(base, string(locator.ContextKey)) {
		t.Fatalf("identifying or unexpected provenance filename %q", base)
	}
	for _, path := range []string{stateDir, state.ProvenancePath} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0700)
		if path == state.ProvenancePath {
			want = 0600
		}
		if info.Mode().Perm() != want {
			t.Fatalf("mode %s = %04o, want %04o", path, info.Mode().Perm(), want)
		}
	}

	// Drift the context from restricted to full. The valid state remains the
	// authority for status output and therefore cannot widen disclosure.
	cfg.Contexts[0].Context.Replay.Disclosure = session.ReplayDisclosureFull
	writeReplayCLIConfig(t, configPath, cfg)
	status := runReplayCLI(t, "json", "status")
	if status.err != nil {
		t.Fatal(status.err)
	}
	var view ReplayStatusOutput
	if err := json.Unmarshal([]byte(status.stdout), &view); err != nil {
		t.Fatal(err)
	}
	if view.Status != session.ReplayStatusDrifted || !view.ConfigurationDrift || view.Disclosure != session.ReplayDisclosureRestricted || view.ProvenancePath != state.ProvenancePath {
		t.Fatalf("stored disclosure did not remain authoritative: %+v", view)
	}
	if advance := runReplayCLI(t, "table", "advance", "1m"); advance.err == nil || !strings.Contains(advance.err.Error(), "context changed") {
		t.Fatalf("drifted advance error = %v", advance.err)
	}
	if stop := runReplayCLI(t, "json", "stop"); stop.err != nil {
		t.Fatalf("drifted session must remain stoppable: %v", stop.err)
	} else {
		var stopped ReplayStatusOutput
		if err := json.Unmarshal([]byte(stop.stdout), &stopped); err != nil {
			t.Fatal(err)
		}
		if !stopped.ConfigurationDrift || stopped.Disclosure != session.ReplayDisclosureRestricted || stopped.ProvenancePath != state.ProvenancePath {
			t.Fatalf("drifted stop widened stored disclosure route: %+v", stopped)
		}
	}
}
