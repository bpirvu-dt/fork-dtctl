package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dynatrace-oss/dtctl/pkg/commands"
	"github.com/dynatrace-oss/dtctl/pkg/config"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

type replayGuardCounters struct {
	ctxParent  int
	ctxCurrent int
	ctxView    int
	mutation   int
	credential int
	network    int
}

func newReplayGuardTestTree(counters *replayGuardCounters) *cobra.Command {
	root := &cobra.Command{Use: "dtctl", SilenceErrors: true, SilenceUsage: true}
	root.PersistentFlags().StringVar(&contextName, "context", "", "test context")

	query := &cobra.Command{Use: "query [dql]", Args: cobra.MaximumNArgs(1), RunE: func(*cobra.Command, []string) error {
		counters.network++
		return nil
	}}
	deleteCmd := &cobra.Command{Use: "delete <resource>", Args: cobra.ExactArgs(1), RunE: func(*cobra.Command, []string) error {
		counters.mutation++
		return nil
	}}
	ctxCmd := &cobra.Command{Use: "ctx [name]", Args: cobra.MaximumNArgs(1), RunE: func(*cobra.Command, []string) error {
		counters.ctxParent++
		return nil
	}}
	ctxCmd.AddCommand(
		&cobra.Command{Use: "current", RunE: func(*cobra.Command, []string) error {
			counters.ctxCurrent++
			return nil
		}},
		&cobra.Command{Use: "describe <name>", Args: cobra.ExactArgs(1), RunE: func(*cobra.Command, []string) error {
			counters.ctxView++
			return nil
		}},
		&cobra.Command{Use: "set <name>", Args: cobra.ExactArgs(1), RunE: func(*cobra.Command, []string) error {
			counters.mutation++
			return nil
		}},
		&cobra.Command{Use: "delete <name>", Aliases: []string{"rm"}, Args: cobra.ExactArgs(1), RunE: func(*cobra.Command, []string) error {
			counters.mutation++
			return nil
		}},
		&cobra.Command{Use: "token [name]", Args: cobra.MaximumNArgs(1), RunE: func(*cobra.Command, []string) error {
			counters.credential++
			return nil
		}},
	)
	replay := &cobra.Command{Use: "replay"}
	for _, name := range []string{"start", "advance", "status", "stop"} {
		replay.AddCommand(&cobra.Command{Use: name, RunE: func(*cobra.Command, []string) error { return nil }})
	}
	root.AddCommand(query, deleteCmd, ctxCmd, replay)
	return root
}

func executeReplayGuardTree(t *testing.T, args ...string) (*replayGuardCounters, error) {
	t.Helper()
	counters := &replayGuardCounters{}
	root := newReplayGuardTestTree(counters)
	installReplayGuard(root)
	root.SetArgs(args)
	return counters, root.Execute()
}

func configureReplayGuardTest(t *testing.T) (string, string, *replayCLIFakeClock) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeReplayCLIConfig(t, path, replayCLIConfig(standardReplayBlock(session.ReplayClockManual)))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 17, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, path, filepath.Join(dir, "state"), clock)
	return path, dir, clock
}

func TestReplayHardGuardBlocksCanonicalCtxPathsBeforeSideEffects(t *testing.T) {
	configureReplayGuardTest(t)
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)

	tests := []struct {
		name      string
		args      []string
		canonical string
	}{
		{"set", []string{"ctx", "set", "new-context"}, "ctx set"},
		{"set missing argument", []string{"ctx", "set"}, "ctx set"},
		{"delete", []string{"ctx", "delete", "old-context"}, "ctx delete"},
		{"rm alias", []string{"ctx", "rm", "old-context"}, "ctx delete"},
		{"token", []string{"ctx", "token"}, "ctx token"},
		{"token unknown flag", []string{"ctx", "token", "--unrecognized"}, "ctx token"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			counters, err := executeReplayGuardTree(t, test.args...)
			var guardErr *ReplayGuardError
			if !errors.As(err, &guardErr) {
				t.Fatalf("error = %v, want ReplayGuardError", err)
			}
			if guardErr.Command != test.canonical {
				t.Fatalf("canonical command = %q, want %q", guardErr.Command, test.canonical)
			}
			for _, exit := range []string{"ctx <name>", "--context <name>", "DTCTL_CONTEXT"} {
				if !strings.Contains(err.Error(), exit) {
					t.Fatalf("blocked ctx error omits exit %q: %v", exit, err)
				}
			}
			if counters.mutation != 0 || counters.credential != 0 {
				t.Fatalf("blocked command reached side effect: %+v", counters)
			}
		})
	}
}

func TestReplayHardGuardAllowsCtxInspectionAndSwitchParent(t *testing.T) {
	configureReplayGuardTest(t)
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)

	tests := []struct {
		args  []string
		check func(*replayGuardCounters) int
	}{
		{[]string{"ctx"}, func(c *replayGuardCounters) int { return c.ctxParent }},
		{[]string{"ctx", "production"}, func(c *replayGuardCounters) int { return c.ctxParent }},
		{[]string{"ctx", "current"}, func(c *replayGuardCounters) int { return c.ctxCurrent }},
		{[]string{"ctx", "describe", "production"}, func(c *replayGuardCounters) int { return c.ctxView }},
	}
	for _, test := range tests {
		counters, err := executeReplayGuardTree(t, test.args...)
		if err != nil {
			t.Fatalf("%v: %v", test.args, err)
		}
		if test.check(counters) != 1 {
			t.Fatalf("allowed command %v did not run: %+v", test.args, counters)
		}
	}
}

func TestReplayHardGuardAndInterimDQLBlockIgnoreFullProfileOverride(t *testing.T) {
	configureReplayGuardTest(t)
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)

	counters, err := executeReplayGuardTree(t, "delete", "workflows")
	var guardErr *ReplayGuardError
	if !errors.As(err, &guardErr) || counters.mutation != 0 {
		t.Fatalf("full profile bypassed hard guard: counters=%+v err=%v", counters, err)
	}

	counters, err = executeReplayGuardTree(t, "query", "fetch logs")
	var unavailable *ReplayQueryUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("query error = %v, want temporary replay implementation block", err)
	}
	if counters.network != 0 || !strings.Contains(err.Error(), "no network request was made") {
		t.Fatalf("DQL path reached network or unclear error: counters=%+v err=%v", counters, err)
	}
	if got := exitCodeForError(err); got == 0 {
		t.Fatal("interim DQL block must return non-zero")
	}
}

func TestReplayActivationForcesReservedProfileDespiteFullOverride(t *testing.T) {
	path, _, _ := configureReplayGuardTest(t)
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	profile, err := resolveActiveProfile([]string{"--config", path})
	if err != nil {
		t.Fatal(err)
	}
	if profile == nil || profile.Name != config.ProfileReplay {
		t.Fatalf("replay activation resolved widened profile: %+v", profile)
	}
	root := newReplayGuardTestTree(&replayGuardCounters{})
	applyProfile(root, profile)
	if !find(root, "delete").Hidden || find(root, "replay", "status").Hidden {
		t.Fatal("forced replay profile did not reduce discovery surface")
	}
	listing := commands.Build(root)
	annotateListingContext(listing)
	if listing.Profile != config.ProfileReplay {
		t.Fatalf("catalog advertised widened profile %q", listing.Profile)
	}
}

func TestReplayHardGuardCoversLegacyRunHandlers(t *testing.T) {
	configureReplayGuardTest(t)
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	runs := 0
	root := &cobra.Command{Use: "dtctl", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(&cobra.Command{Use: "version", Run: func(*cobra.Command, []string) { runs++ }})
	installReplayGuard(root)
	root.SetArgs([]string{"version"})
	err := root.Execute()
	var guardErr *ReplayGuardError
	if !errors.As(err, &guardErr) || runs != 0 {
		t.Fatalf("legacy Run handler bypassed replay guard: runs=%d err=%v", runs, err)
	}
}

func TestReplayHardGuardActivatesFromStoredSessionAfterContextEdit(t *testing.T) {
	path, _, clock := configureReplayGuardTest(t)
	t.Setenv(config.ProfileEnvVar, "")
	if result := runReplayCLI(t, "table", "start"); result.err != nil {
		t.Fatal(result.err)
	}
	locator, _ := replayLocatorForConfig(t, path)
	store := session.NewReplayStateStore(replayStateDirectory, clock)
	before, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}

	cfg := replayCLIConfig(nil)
	cfg.Contexts[0].Context.Profile = config.ProfileFull
	writeReplayCLIConfig(t, path, cfg)
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	counters, err := executeReplayGuardTree(t, "delete", "workflows")
	var guardErr *ReplayGuardError
	if !errors.As(err, &guardErr) || counters.mutation != 0 {
		t.Fatalf("active stored state did not preserve guard: counters=%+v err=%v", counters, err)
	}
	after, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if after.SessionID != before.SessionID || after.Revision != before.Revision {
		t.Fatal("guard inspection rewrote the replay state")
	}
}

func TestReplayContextFlagAndEnvironmentSelectNonReplayExit(t *testing.T) {
	path, _, clock := configureReplayGuardTest(t)
	cfg := replayCLIConfig(standardReplayBlock(session.ReplayClockManual))
	cfg.Contexts = append(cfg.Contexts, config.NamedContext{
		Name: "production",
		Context: config.Context{
			Environment: "https://production.example.invalid",
			TokenRef:    "synthetic-production-reader",
			SafetyLevel: config.SafetyLevelReadOnly,
		},
	})
	writeReplayCLIConfig(t, path, cfg)
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)

	// Start the replay session with the context binding authoritative.
	t.Setenv(config.ProfileEnvVar, "")
	if result := runReplayCLI(t, "table", "start"); result.err != nil {
		t.Fatal(result.err)
	}
	locator, _ := replayLocatorForConfig(t, path)
	store := session.NewReplayStateStore(replayStateDirectory, clock)
	before, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)

	counters, err := executeReplayGuardTree(t, "--context", "production", "query", "fetch logs")
	if err != nil || counters.network != 1 {
		t.Fatalf("--context exit did not select non-replay context: counters=%+v err=%v", counters, err)
	}
	contextName = ""
	t.Setenv("DTCTL_CONTEXT", "production")
	t.Setenv(config.ProfileEnvVar, "")
	profile, profileErr := resolveActiveProfile([]string{"--config", path})
	if profileErr != nil || profile != nil {
		t.Fatalf("DTCTL_CONTEXT did not influence profile selection: profile=%+v err=%v", profile, profileErr)
	}
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	counters, err = executeReplayGuardTree(t, "query", "fetch logs")
	if err != nil || counters.network != 1 {
		t.Fatalf("DTCTL_CONTEXT exit did not select non-replay context: counters=%+v err=%v", counters, err)
	}
	after, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if after.SessionID != before.SessionID || after.Revision != before.Revision {
		t.Fatal("switching selection stopped or rewrote the replay session")
	}
}

func TestReplayRootAliasIsGuardedAfterCanonicalResolution(t *testing.T) {
	path, _, _ := configureReplayGuardTest(t)
	cfg, err := config.LoadFrom(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Aliases = map[string]string{"remove-context": "ctx delete old-context"}
	writeReplayCLIConfig(t, path, cfg)
	loaded, err := config.LoadFrom(path)
	if err != nil {
		t.Fatal(err)
	}
	expanded, shell, err := resolveAlias([]string{"remove-context"}, loaded)
	if err != nil || shell {
		t.Fatalf("resolve alias: expanded=%v shell=%t err=%v", expanded, shell, err)
	}
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	counters, err := executeReplayGuardTree(t, expanded...)
	var guardErr *ReplayGuardError
	if !errors.As(err, &guardErr) || guardErr.Command != "ctx delete" || counters.mutation != 0 {
		t.Fatalf("expanded alias bypassed canonical guard: counters=%+v err=%v", counters, err)
	}
}

func TestReplayPluginDispatchIsBlockedBeforeLookup(t *testing.T) {
	path, _, _ := configureReplayGuardTest(t)
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	code, handled := tryPluginDispatch([]string{"--config", path, "definitely-no-replay-plugin"})
	if !handled || code == 0 {
		t.Fatalf("plugin guard: handled=%t code=%d", handled, code)
	}
	err := replayPluginDispatchGuard([]string{"--config", path}, []string{"definitely-no-replay-plugin"})
	var guardErr *ReplayGuardError
	if !errors.As(err, &guardErr) || !guardErr.Plugin {
		t.Fatalf("plugin error = %v", err)
	}
}

func TestReplayHardGuardExactLeavesAndCatalogRegistration(t *testing.T) {
	for _, path := range []string{"replay start child", "ctx token child", "wait query child", "future command"} {
		if replayHardGuardAllows(path) {
			t.Fatalf("future/blocked path %q was allowed", path)
		}
	}
	for _, path := range []string{"ctx", "ctx current", "ctx describe", "replay start", "auth status"} {
		if !replayHardGuardAllows(path) {
			t.Fatalf("safe path %q was blocked", path)
		}
	}

	listing := commands.Build(rootCmd)
	verb := listing.Verbs["replay"]
	if verb == nil {
		t.Fatal("replay command missing from dtctl commands catalog")
	}
	for _, child := range []string{"start", "advance", "status", "stop"} {
		found := false
		for _, resource := range verb.Resources {
			if resource == child {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("replay %s missing from command catalog", child)
		}
	}
}

func TestReplayProfileShapesCtxLeavesAndPreservesAncestor(t *testing.T) {
	path, _, _ := configureReplayGuardTest(t)
	t.Setenv(config.ProfileEnvVar, "")
	cfg, err := config.LoadFrom(path)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := cfg.ResolveProfile()
	if err != nil {
		t.Fatal(err)
	}
	root := newReplayGuardTestTree(&replayGuardCounters{})
	future := &cobra.Command{Use: "future-child", RunE: func(*cobra.Command, []string) error { return nil }}
	find(root, "replay", "start").AddCommand(future)
	applyProfile(root, profile)
	for _, path := range [][]string{{"ctx"}, {"ctx", "current"}, {"ctx", "describe"}} {
		if cmd := find(root, path...); cmd == nil || cmd.Hidden {
			t.Fatalf("replay profile hid reachable path %v", path)
		}
	}
	for _, path := range [][]string{{"ctx", "set"}, {"ctx", "delete"}, {"ctx", "token"}, {"delete"}} {
		if cmd := find(root, path...); cmd == nil || !cmd.Hidden {
			t.Fatalf("replay profile exposed sibling path %v", path)
		}
	}
	if !future.Hidden {
		t.Fatal("replay profile allowed a future child below an exact lifecycle leaf")
	}
}

func TestReplayPluginGuardHonorsConfigAndContextFlags(t *testing.T) {
	path, dir, _ := configureReplayGuardTest(t)
	nonReplayPath := filepath.Join(dir, "other-config.yaml")
	nonReplay := replayCLIConfig(nil)
	writeReplayCLIConfig(t, nonReplayPath, nonReplay)
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	if err := replayPluginDispatchGuard([]string{"--config", nonReplayPath}, []string{"plugin"}); err != nil {
		t.Fatalf("non-replay --config was blocked: %v", err)
	}
	if err := replayPluginDispatchGuard([]string{"--config", path}, []string{"plugin"}); err == nil {
		t.Fatal("explicit replay --config did not activate plugin guard")
	}
}

func TestReplayGuardDoesNotReadCredentialMaterial(t *testing.T) {
	path, _, _ := configureReplayGuardTest(t)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "token:") {
		t.Fatal("test setup accidentally embedded credential material")
	}
	counters, err := executeReplayGuardTree(t, "ctx", "token")
	if err == nil || counters.credential != 0 {
		t.Fatalf("credential handler ran: counters=%+v err=%v", counters, err)
	}
}

func TestReplayFlagsOnlyStoppedSessionDropsActiveStateGuard(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeReplayCLIConfig(t, path, replayCLIConfig(nil))
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 19, 0, 0, 0, time.UTC)}
	configureReplayCLI(t, path, filepath.Join(dir, "state"), clock)
	t.Setenv(config.ProfileEnvVar, "")
	start := runReplayCLI(t, "table", "start",
		"--data-start", "2026-06-14T08:00:00Z",
		"--data-end", "2026-06-14T12:00:00Z")
	if start.err != nil {
		t.Fatal(start.err)
	}
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	blocked, err := executeReplayGuardTree(t, "delete", "workflows")
	if err == nil || blocked.mutation != 0 {
		t.Fatalf("active flags-only state did not activate guard: counters=%+v err=%v", blocked, err)
	}
	t.Setenv(config.ProfileEnvVar, "")
	if stop := runReplayCLI(t, "table", "stop"); stop.err != nil {
		t.Fatal(stop.err)
	}
	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	allowed, err := executeReplayGuardTree(t, "delete", "workflows")
	if err != nil || allowed.mutation != 1 {
		t.Fatalf("stopped flags-only state remained an activation signal: counters=%+v err=%v", allowed, err)
	}
}

func TestReplayContextBackedStoppedSessionKeepsConfiguredGuard(t *testing.T) {
	configureReplayGuardTest(t)
	t.Setenv(config.ProfileEnvVar, "")
	if start := runReplayCLI(t, "table", "start"); start.err != nil {
		t.Fatal(start.err)
	}
	if stop := runReplayCLI(t, "table", "stop"); stop.err != nil {
		t.Fatal(stop.err)
	}

	t.Setenv(config.ProfileEnvVar, config.ProfileFull)
	counters, err := executeReplayGuardTree(t, "delete", "workflows")
	var guardErr *ReplayGuardError
	if !errors.As(err, &guardErr) || counters.mutation != 0 {
		t.Fatalf("context-backed stopped session lost configured guard: counters=%+v err=%v", counters, err)
	}
}

func TestReplayHelpDocumentsExitRoutesAndFlagsOnlyLimitation(t *testing.T) {
	command := newReplayCommand()
	text := command.Long + "\n" + command.Example
	for _, required := range []string{
		"dtctl ctx <name>",
		"--context <name>",
		"DTCTL_CONTEXT=<name>",
		"started only from flags",
		"after stop",
		"does not stop the local replay session",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("replay help omits %q:\n%s", required, text)
		}
	}
}
