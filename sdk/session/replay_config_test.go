package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveReplayConfigDefaultsAndUTC(t *testing.T) {
	dir := t.TempDir()
	key := NewReplayContextKey("/config/a", "historical", "https://example.invalid/")
	cfg, err := ResolveReplayConfig(&ReplayConfig{
		DataStart: "2026-06-14T10:00:00.123456789+02:00",
		DataEnd:   "2026-06-14T12:00:00.123456789+02:00",
	}, ReplayConfigOverrides{}, dir, key)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := formatReplayTime(cfg.DataStart), "2026-06-14T08:00:00.123456789Z"; got != want {
		t.Fatalf("data start = %s, want %s", got, want)
	}
	if !cfg.VirtualStart.Equal(cfg.DataStart) {
		t.Fatalf("virtual start = %s, want data start", cfg.VirtualStart)
	}
	if cfg.ClockMode != ReplayClockRealtime || cfg.Disclosure != ReplayDisclosureFull || cfg.ProvenancePath != "" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.ValueSources.VirtualStart != ReplayValueFromDefault || cfg.ValueSources.ClockMode != ReplayValueFromDefault || cfg.ValueSources.Disclosure != ReplayValueFromDefault {
		t.Fatalf("unexpected default sources: %+v", cfg.ValueSources)
	}
}

func TestResolveReplayConfigFlagPrecedence(t *testing.T) {
	dataStart := "2026-06-14T09:00:00Z"
	clockMode := ReplayClockManual
	raw := &ReplayConfig{
		DataStart:    "2026-06-14T08:00:00Z",
		DataEnd:      "2026-06-14T12:00:00Z",
		VirtualStart: "2026-06-14T10:00:00Z",
		ClockMode:    ReplayClockRealtime,
	}
	cfg, err := ResolveReplayConfig(raw, ReplayConfigOverrides{
		DataStart: &dataStart,
		ClockMode: &clockMode,
	}, t.TempDir(), ContextKey(strings.Repeat("a", 64)))
	if err != nil {
		t.Fatal(err)
	}
	if got := formatReplayTime(cfg.DataStart); got != dataStart {
		t.Fatalf("data_start = %s, want flag %s", got, dataStart)
	}
	if cfg.ClockMode != ReplayClockManual {
		t.Fatalf("clock mode = %s, want manual", cfg.ClockMode)
	}
	if cfg.ValueSources.DataStart != ReplayValueFromFlag || cfg.ValueSources.ClockMode != ReplayValueFromFlag || cfg.ValueSources.DataEnd != ReplayValueFromContext {
		t.Fatalf("unexpected sources: %+v", cfg.ValueSources)
	}
}

func TestResolveReplayConfigValidation(t *testing.T) {
	base := ReplayConfig{
		DataStart: "2026-06-14T08:00:00Z",
		DataEnd:   "2026-06-14T12:00:00Z",
	}
	tests := []struct {
		name string
		edit func(*ReplayConfig)
		want string
	}{
		{"missing both", func(c *ReplayConfig) { c.DataStart, c.DataEnd = "", "" }, "missing data_start, data_end"},
		{"timestamp without timezone", func(c *ReplayConfig) { c.DataStart = "2026-06-14T08:00:00" }, "absolute RFC 3339"},
		{"reversed interval", func(c *ReplayConfig) { c.DataStart = c.DataEnd }, "data_start must be before data_end"},
		{"virtual before", func(c *ReplayConfig) { c.VirtualStart = "2026-06-14T07:59:59Z" }, "virtual_start must be within"},
		{"virtual after", func(c *ReplayConfig) { c.VirtualStart = "2026-06-14T12:00:01Z" }, "virtual_start must be within"},
		{"unknown clock", func(c *ReplayConfig) { c.ClockMode = "paused" }, "unknown replay clock_mode"},
		{"unknown disclosure", func(c *ReplayConfig) { c.Disclosure = "hidden" }, "unknown replay disclosure"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.edit(&cfg)
			_, err := ResolveReplayConfig(&cfg, ReplayConfigOverrides{}, t.TempDir(), ContextKey(strings.Repeat("b", 64)))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestResolveReplayConfigDisclosureAndProvenance(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	key := ContextKey(strings.Repeat("c", 64))
	raw := &ReplayConfig{
		DataStart:  "2026-06-14T08:00:00Z",
		DataEnd:    "2026-06-14T12:00:00Z",
		Disclosure: ReplayDisclosureRestricted,
	}
	cfg, err := ResolveReplayConfig(raw, ReplayConfigOverrides{}, dir, key)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, string(key)+".provenance.jsonl")
	if cfg.ProvenancePath != want || cfg.ValueSources.ProvenancePath != ReplayValueFromDefault {
		t.Fatalf("default provenance = %q (%s), want %q/default", cfg.ProvenancePath, cfg.ValueSources.ProvenancePath, want)
	}

	override := filepath.Join(dir, "audit.jsonl")
	raw.ProvenancePath = override
	cfg, err = ResolveReplayConfig(raw, ReplayConfigOverrides{}, dir, key)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ProvenancePath != override || cfg.ValueSources.ProvenancePath != ReplayValueFromContext {
		t.Fatalf("override provenance = %q (%s)", cfg.ProvenancePath, cfg.ValueSources.ProvenancePath)
	}

	// Full disclosure ignores an optional provenance path completely.
	raw.Disclosure = ""
	raw.ProvenancePath = "relative/is/ignored"
	cfg, err = ResolveReplayConfig(raw, ReplayConfigOverrides{}, dir, key)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Disclosure != ReplayDisclosureFull || cfg.ProvenancePath != "" {
		t.Fatalf("full disclosure unexpectedly retained provenance: %+v", cfg)
	}
}

func TestValidateReplayProvenancePathRejectsUnsafePaths(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := ValidateReplayProvenancePath("relative.jsonl", false); err == nil {
		t.Fatal("relative path should fail")
	}
	nonNormalized := filepath.Join(dir, "sub") + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "audit.jsonl"
	if err := ValidateReplayProvenancePath(nonNormalized, false); err == nil {
		t.Fatal("non-normalized path should fail")
	}
	t.Run("unsafe parent", func(t *testing.T) {
		skipPOSIXModeAssertionsOnWindows(t)
		unsafeParent := filepath.Join(dir, "unsafe")
		if err := os.Mkdir(unsafeParent, 0755); err != nil {
			t.Fatal(err)
		}
		if err := ValidateReplayProvenancePath(filepath.Join(unsafeParent, "audit.jsonl"), false); err == nil {
			t.Fatal("non-private parent should fail")
		}
	})
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "audit.jsonl")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := ValidateReplayProvenancePath(link, false); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink error = %v", err)
	}
}

func TestReplayConfigYAMLRoundTripAndSourceIdentity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	cfg := NewConfig()
	cfg.CurrentContext = "historical"
	cfg.Contexts = []NamedContext{{Name: "historical", Context: Context{
		Environment: "https://tenant.example.invalid",
		SafetyLevel: SafetyLevelReadOnly,
		Profile:     ProfileReplay,
		Replay: &ReplayConfig{
			DataStart:      "2026-06-14T08:00:00Z",
			DataEnd:        "2026-06-14T12:00:00Z",
			VirtualStart:   "2026-06-14T10:00:00Z",
			ClockMode:      ReplayClockManual,
			Disclosure:     ReplayDisclosureRestricted,
			ProvenancePath: filepath.Join(dir, "audit.jsonl"),
		},
	}}}
	if err := cfg.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadFrom(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := loaded.CurrentContextObj()
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Replay == nil || ctx.Replay.ClockMode != ReplayClockManual || ctx.Replay.Disclosure != ReplayDisclosureRestricted {
		t.Fatalf("replay YAML did not round-trip: %+v", ctx.Replay)
	}
	identity, err := loaded.SourceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	want, _ := filepath.EvalSymlinks(path)
	if identity != want {
		t.Fatalf("source identity = %q, want %q", identity, want)
	}

	otherPath := filepath.Join(dir, "other.yaml")
	if err := cfg.SaveTo(otherPath); err != nil {
		t.Fatal(err)
	}
	other, err := LoadFrom(otherPath)
	if err != nil {
		t.Fatal(err)
	}
	otherIdentity, _ := other.SourceIdentity()
	if NewReplayContextKey(identity, "historical", ctx.Environment) == NewReplayContextKey(otherIdentity, "historical", ctx.Environment) {
		t.Fatal("two config files produced the same replay context key")
	}
}

func TestReplayContextInputHashDetectsRequiredDrift(t *testing.T) {
	ctx := &Context{
		Environment: "https://tenant.example.invalid/",
		SafetyLevel: SafetyLevelReadOnly,
		Profile:     ProfileReplay,
		Replay:      &ReplayConfig{DataStart: "2026-06-14T08:00:00Z", DataEnd: "2026-06-14T12:00:00Z"},
	}
	base := ReplayContextInputHash(ctx)
	copy := *ctx
	copy.TokenRef = "different-token-ref"
	if got := ReplayContextInputHash(&copy); got != base {
		t.Fatal("token reference must not affect replay context input hash")
	}
	copy = *ctx
	copy.Environment = "https://other.example.invalid"
	if got := ReplayContextInputHash(&copy); got == base {
		t.Fatal("environment drift was not detected")
	}
	copy = *ctx
	copy.Replay = &ReplayConfig{DataStart: ctx.Replay.DataStart, DataEnd: "2026-06-14T13:00:00Z"}
	if got := ReplayContextInputHash(&copy); got == base {
		t.Fatal("replay config drift was not detected")
	}
	copy = *ctx
	copy.SafetyLevel = SafetyLevelReadWriteAll
	if got := ReplayContextInputHash(&copy); got == base {
		t.Fatal("safety drift was not detected")
	}
	copy = *ctx
	copy.Profile = ProfileFull
	if got := ReplayContextInputHash(&copy); got == base {
		t.Fatal("profile drift was not detected")
	}
}

func TestUserReplayProfileIsRejectedWithMigrationError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := "apiVersion: v1\nkind: Config\nprofiles:\n  replay:\n    commands: [query]\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadFrom(path)
	if err == nil || !strings.Contains(err.Error(), "reserved") || !strings.Contains(err.Error(), "rename") {
		t.Fatalf("migration error = %v", err)
	}
}

func TestVirtualStartMayEqualDataEnd(t *testing.T) {
	raw := &ReplayConfig{
		DataStart:    "2026-06-14T08:00:00Z",
		DataEnd:      "2026-06-14T12:00:00Z",
		VirtualStart: "2026-06-14T12:00:00Z",
		ClockMode:    ReplayClockManual,
	}
	cfg, err := ResolveReplayConfig(raw, ReplayConfigOverrides{}, t.TempDir(), ContextKey(strings.Repeat("d", 64)))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.VirtualStart.Equal(time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected virtual start: %s", cfg.VirtualStart)
	}
}
