package cmd

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/pkg/config"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestReplayCLIValidationAndInactiveStatus(t *testing.T) {
	t.Setenv(config.ProfileEnvVar, "")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 14, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), clock)

	tests := []struct {
		name string
		edit func(*config.Config)
		want string
	}{
		{"invalid disclosure", func(c *config.Config) { c.Contexts[0].Context.Replay.Disclosure = "partial" }, "unknown replay disclosure"},
		{"relative provenance", func(c *config.Config) {
			c.Contexts[0].Context.Replay.Disclosure = session.ReplayDisclosureRestricted
			c.Contexts[0].Context.Replay.ProvenancePath = "relative.jsonl"
		}, "must be absolute"},
		{"not readonly", func(c *config.Config) { c.Contexts[0].Context.SafetyLevel = config.SafetyLevelReadWriteAll }, "mutating commands still target the live tenant"},
		{"missing profile", func(c *config.Config) { c.Contexts[0].Context.Profile = "" }, "reserved built-in profile"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := replayCLIConfig(standardReplayBlock(session.ReplayClockManual))
			test.edit(cfg)
			writeReplayCLIConfig(t, configPath, cfg)
			result := runReplayCLI(t, "table", "start")
			if result.err == nil || !strings.Contains(result.err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", result.err, test.want)
			}
		})
	}
	for _, flag := range []string{"--disclosure", "--provenance-path"} {
		result := runReplayCLI(t, "table", "start", flag, "restricted")
		if result.err == nil || !strings.Contains(result.err.Error(), "unknown flag") {
			t.Fatalf("context-only flag %s was accepted: %v", flag, result.err)
		}
	}

	writeReplayCLIConfig(t, configPath, replayCLIConfig(standardReplayBlock(session.ReplayClockManual)))
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	override := runReplayCLI(t, "table", "start")
	if override.err == nil || !strings.Contains(override.err.Error(), "profile resolution was overridden") {
		t.Fatalf("DTCTL_PROFILE override error = %v", override.err)
	}
	t.Setenv(config.ProfileEnvVar, "")
	shadow := replayCLIConfig(standardReplayBlock(session.ReplayClockManual))
	shadow.Profiles = map[string]config.Profile{
		config.ProfileReplay: {Commands: []string{"delete"}},
	}
	writeReplayCLIConfig(t, configPath, shadow)
	shadowed := runReplayCLI(t, "table", "start")
	if shadowed.err == nil || !strings.Contains(shadowed.err.Error(), "reserved") || !strings.Contains(shadowed.err.Error(), "rename") {
		t.Fatalf("reserved profile migration error = %v", shadowed.err)
	}

	writeReplayCLIConfig(t, configPath, replayCLIConfig(nil))
	inactive := runReplayCLI(t, "json", "status")
	if inactive.err != nil {
		t.Fatal(inactive.err)
	}
	var view ReplayStatusOutput
	if err := json.Unmarshal([]byte(inactive.stdout), &view); err != nil {
		t.Fatal(err)
	}
	if view.Status != session.ReplayStatusInactive || view.SessionID != "" {
		t.Fatalf("inactive status = %+v", view)
	}
	missing := runReplayCLI(t, "table", "advance", "1m")
	if missing.err == nil || !strings.Contains(missing.err.Error(), "has no replay session") {
		t.Fatalf("missing advance error = %v", missing.err)
	}
}

func TestReplayCLIStatusAgentEnvelope(t *testing.T) {
	t.Setenv(config.ProfileEnvVar, "")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	writeReplayCLIConfig(t, configPath, replayCLIConfig(standardReplayBlock(session.ReplayClockManual)))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 9, 30, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), clock)

	if result := runReplayCLI(t, "table", "start"); result.err != nil {
		t.Fatal(result.err)
	}
	agentMode = true
	status := runReplayCLI(t, "table", "status")
	if status.err != nil {
		t.Fatal(status.err)
	}

	var envelope struct {
		OK      bool               `json:"ok"`
		Result  ReplayStatusOutput `json:"result"`
		Context struct {
			Verb     string `json:"verb"`
			Resource string `json:"resource"`
		} `json:"context"`
	}
	if err := json.Unmarshal([]byte(status.stdout), &envelope); err != nil {
		t.Fatalf("agent status is not valid JSON: %v\n%s", err, status.stdout)
	}
	if !envelope.OK || envelope.Context.Verb != "replay" || envelope.Context.Resource != "session" {
		t.Fatalf("unexpected agent envelope: %+v", envelope)
	}
	if envelope.Result.ContextName != "historical-window" || envelope.Result.Status != session.ReplayStatusActive || envelope.Result.ClockMode != session.ReplayClockManual {
		t.Fatalf("agent envelope status = %+v", envelope.Result)
	}
	if envelope.Result.VirtualNow == "" || envelope.Result.DataStart == "" || envelope.Result.DataEnd == "" {
		t.Fatalf("agent envelope omits replay status fields: %+v", envelope.Result)
	}
}

func TestReplayCLIProvenanceOverrideAndSymlinkRefusal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink and POSIX private-mode assertions")
	}
	t.Setenv(config.ProfileEnvVar, "")
	dir := t.TempDir()
	privateDir := filepath.Join(dir, "private")
	if err := os.Mkdir(privateDir, 0700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "config.yaml")
	raw := standardReplayBlock(session.ReplayClockManual)
	raw.Disclosure = session.ReplayDisclosureRestricted
	raw.ProvenancePath = filepath.Join(privateDir, "events.jsonl")
	writeReplayCLIConfig(t, configPath, replayCLIConfig(raw))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 15, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), clock)
	if result := runReplayCLI(t, "table", "start"); result.err != nil {
		t.Fatal(result.err)
	}
	if info, err := os.Stat(raw.ProvenancePath); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("override provenance mode: info=%v err=%v", info, err)
	}

	if stop := runReplayCLI(t, "table", "stop"); stop.err != nil {
		t.Fatal(stop.err)
	}
	target := filepath.Join(privateDir, "target.jsonl")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(privateDir, "link.jsonl")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	raw.ProvenancePath = symlink
	writeReplayCLIConfig(t, configPath, replayCLIConfig(raw))
	result := runReplayCLI(t, "table", "start", "--restart")
	if result.err == nil || !strings.Contains(result.err.Error(), "symlinked replay provenance") {
		t.Fatalf("symlink provenance error = %v", result.err)
	}
}

func TestReplayCLIFailedCompletionLeavesTerminalReady(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	raw := &config.ReplayConfig{
		DataStart:    "2026-06-14T08:00:00Z",
		DataEnd:      "2026-06-14T09:00:00Z",
		VirtualStart: "2026-06-14T09:00:00Z",
		ClockMode:    session.ReplayClockManual,
	}
	writeReplayCLIConfig(t, configPath, replayCLIConfig(raw))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 16, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), clock)
	if err := runReplayCLI(t, "table", "start").err; err != nil {
		t.Fatal(err)
	}
	locator, _ := replayLocatorForConfig(t, configPath)
	store := session.NewReplayStateStore(replayStateDirectory, clock)
	before, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	// A failed terminal execution deliberately makes no store call.
	after, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != session.ReplayStatusTerminalReady || after.Revision != before.Revision {
		t.Fatal("failed terminal execution changed replay state")
	}
	completed, disposition, err := store.MarkCompleted(locator, before.SessionID, clock.Now())
	if err != nil || disposition != session.CompletionRecorded || completed.Status != session.ReplayStatusCompleted {
		t.Fatalf("guarded completion = state %+v disposition %q err %v", completed, disposition, err)
	}
	status := runReplayCLI(t, "json", "status")
	if status.err != nil || !strings.Contains(status.stdout, `"status": "completed"`) {
		t.Fatalf("completed status: out=%s err=%v", status.stdout, status.err)
	}
}

func TestReplayCLIWriterLockFailuresBlockAllStateChanges(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink-based deterministic lock failure")
	}
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	writeReplayCLIConfig(t, configPath, replayCLIConfig(standardReplayBlock(session.ReplayClockManual)))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 17, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), clock)
	if err := runReplayCLI(t, "table", "start").err; err != nil {
		t.Fatal(err)
	}
	locator, _ := replayLocatorForConfig(t, configPath)
	store := session.NewReplayStateStore(replayStateDirectory, clock)
	before, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(replayStateDirectory, locator.ContextIdentityHash+".lock")
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "unsafe-lock-target")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, lockPath); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"advance", "1m"}, {"stop"}, {"start", "--restart"}} {
		result := runReplayCLI(t, "table", args...)
		if result.err == nil || !strings.Contains(result.err.Error(), "replay lock") {
			t.Fatalf("%v lock error = %v", args, result.err)
		}
	}
	statePath := store.StatePath(locator.ContextKey)
	if err := os.Chmod(statePath, 0400); err != nil {
		t.Fatal(err)
	}
	fileBefore, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if status := runReplayCLI(t, "json", "status"); status.err != nil {
		t.Fatalf("lock-free status failed while writer lock was unusable: %v", status.err)
	}
	fileAfter, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if fileAfter.Mode().Perm() != 0400 || !fileAfter.ModTime().Equal(fileBefore.ModTime()) {
		t.Fatal("status required write access or modified the state snapshot")
	}
	after, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if after.SessionID != before.SessionID || after.Revision != before.Revision {
		t.Fatal("failed locked operations changed replay state")
	}
}

func TestReplayCLIProvenanceLockFailureBlocksRestrictedStart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink-based deterministic lock failure")
	}
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	raw := standardReplayBlock(session.ReplayClockManual)
	raw.Disclosure = session.ReplayDisclosureRestricted
	writeReplayCLIConfig(t, configPath, replayCLIConfig(raw))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), clock)
	locator, _ := replayLocatorForConfig(t, configPath)
	if err := os.MkdirAll(replayStateDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	provenancePath := filepath.Join(replayStateDirectory, string(locator.ContextKey)+".provenance.jsonl")
	sink := session.NewFileProvenanceSink(provenancePath, replayStateDirectory)
	target := filepath.Join(dir, "unsafe-provenance-lock-target")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, sink.LockPath()); err != nil {
		t.Fatal(err)
	}
	result := runReplayCLI(t, "table", "start")
	if result.err == nil || !strings.Contains(result.err.Error(), "provenance lock") {
		t.Fatalf("provenance lock error = %v", result.err)
	}
	if _, err := session.NewReplayStateStore(replayStateDirectory, clock).Status(locator); !errors.Is(err, session.ErrReplaySessionNotFound) {
		t.Fatalf("restricted start left state after lock failure: %v", err)
	}
}

func TestReplayCLIOutputValidationPrecedesLifecycleMutation(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	writeReplayCLIConfig(t, configPath, replayCLIConfig(standardReplayBlock(session.ReplayClockManual)))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 19, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, configPath, filepath.Join(dir, "state"), clock)
	locator, _ := replayLocatorForConfig(t, configPath)
	store := session.NewReplayStateStore(replayStateDirectory, clock)

	if result := runReplayCLI(t, "csv", "start"); result.err == nil || !strings.Contains(result.err.Error(), "unsupported output format") {
		t.Fatalf("invalid-output start error = %v", result.err)
	}
	if _, err := store.Status(locator); !errors.Is(err, session.ErrReplaySessionNotFound) {
		t.Fatalf("invalid-output start changed state: %v", err)
	}

	jqFilter = "["
	if result := runReplayCLI(t, "json", "start"); result.err == nil || !strings.Contains(result.err.Error(), "invalid --jq filter") {
		t.Fatalf("invalid-jq start error = %v", result.err)
	}
	jqFilter = ""
	if _, err := store.Status(locator); !errors.Is(err, session.ErrReplaySessionNotFound) {
		t.Fatalf("invalid-jq start changed state: %v", err)
	}

	if result := runReplayCLI(t, "table", "start"); result.err != nil {
		t.Fatal(result.err)
	}
	before, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"advance", "1m"}, {"stop"}} {
		if result := runReplayCLI(t, "csv", args...); result.err == nil {
			t.Fatalf("invalid-output %v unexpectedly succeeded", args)
		}
		after, err := store.Status(locator)
		if err != nil {
			t.Fatal(err)
		}
		if after.SessionID != before.SessionID || after.Revision != before.Revision || after.Status != before.Status {
			t.Fatalf("invalid-output %v changed state: before=%+v after=%+v", args, before, after)
		}
	}
}
