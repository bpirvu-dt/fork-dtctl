package cmd

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/pkg/config"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestReplayExecuteHelper(t *testing.T) {
	if os.Getenv("DTCTL_REPLAY_EXECUTE_HELPER") != "1" {
		return
	}
	configPath := os.Getenv("DTCTL_REPLAY_EXECUTE_CONFIG")
	var args []string
	expectSuccess := false
	switch os.Getenv("DTCTL_REPLAY_EXECUTE_CASE") {
	case "ctx-token":
		args = []string{"--no-agent", "--config", configPath, "ctx", "token"}
	case "ctx-rm":
		args = []string{"--no-agent", "--config", configPath, "ctx", "rm", "old-context"}
	case "query":
		args = []string{"--no-agent", "--config", configPath, "query", "fetch logs"}
	case "exec-dql":
		args = []string{"--no-agent", "--config", configPath, "exec", "dql", "fetch logs"}
	case "plugin":
		args = []string{"--no-agent", "--config", configPath, "definitely-no-replay-plugin"}
	case "root-alias":
		args = []string{"--no-agent", "--config", configPath, "purge-context"}
	case "shell-alias":
		args = []string{"--no-agent", "--config", configPath, "shell-escape"}
	case "ctx-list":
		args = []string{"--no-agent", "--config", configPath, "ctx"}
		expectSuccess = true
	case "ctx-current":
		args = []string{"--no-agent", "--config", configPath, "ctx", "current"}
		expectSuccess = true
	case "ctx-describe":
		args = []string{"--no-agent", "--config", configPath, "ctx", "describe", "production"}
		expectSuccess = true
	case "ctx-switch":
		args = []string{"--no-agent", "--config", configPath, "ctx", "production"}
		expectSuccess = true
	case "status-override":
		args = []string{"--no-agent", "--config", configPath, "--context", "historical-window", "replay", "status", "-o", "json"}
		expectSuccess = true
	default:
		t.Fatalf("unknown helper case")
	}
	os.Args = append([]string{"dtctl"}, args...)
	code := execute()
	if expectSuccess && code != 0 {
		t.Fatalf("allowed replay invocation returned exit code %d", code)
	}
	if !expectSuccess && code == 0 {
		t.Fatalf("blocked replay invocation returned exit code 0")
	}
}

func TestReplayExecuteAllowsDocumentedContextExitsWithoutChangingSession(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	cfg := replayCLIConfig(standardReplayBlock(session.ReplayClockManual))
	cfg.Contexts = append(cfg.Contexts, config.NamedContext{
		Name: "production",
		Context: config.Context{
			Environment: "https://production.example.invalid",
			TokenRef:    "synthetic-production-reader",
			SafetyLevel: config.SafetyLevelReadOnly,
		},
	})
	writeReplayCLIConfig(t, configPath, cfg)

	loaded, err := config.LoadFrom(configPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := loaded.CurrentContextObj()
	if err != nil {
		t.Fatal(err)
	}
	source, err := loaded.SourceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	stateHome := filepath.Join(dir, "state-home")
	stateDir := filepath.Join(stateHome, "dtctl", "replay")
	locator := session.ReplayLocator{
		ContextKey:          session.NewReplayContextKey(source, loaded.CurrentContext, ctx.Environment),
		ContextIdentityHash: session.ReplayContextIdentityHash(source, loaded.CurrentContext),
	}
	resolved, err := session.ResolveReplayConfig(ctx.Replay, session.ReplayConfigOverrides{}, stateDir, locator.ContextKey)
	if err != nil {
		t.Fatal(err)
	}
	clock := &replayCLIFakeClock{now: time.Date(2026, 8, 11, 20, 0, 0, 0, time.UTC)}
	store := session.NewReplayStateStore(stateDir, clock)
	started, err := store.Start(session.ReplayStartRequest{
		Locator:          locator,
		ContextName:      loaded.CurrentContext,
		EnvironmentHash:  session.ReplayEnvironmentHash(ctx.Environment),
		ContextInputHash: session.ReplayContextInputHash(ctx),
		Config:           resolved,
	})
	if err != nil {
		t.Fatal(err)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	run := func(testCase string) string {
		t.Helper()
		command := exec.Command(executable, "-test.run=^TestReplayExecuteHelper$")
		command.Env = replayExecuteHelperEnv(os.Environ(), map[string]string{
			"DTCTL_REPLAY_EXECUTE_HELPER": "1",
			"DTCTL_REPLAY_EXECUTE_CONFIG": configPath,
			"DTCTL_REPLAY_EXECUTE_CASE":   testCase,
			"DTCTL_PROFILE":               config.ProfileFull,
			"XDG_STATE_HOME":              stateHome,
			"DTCTL_CONTEXT":               "",
		})
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("helper %s failed: %v\n%s", testCase, err, output)
		}
		return string(output)
	}

	if output := run("ctx-list"); !strings.Contains(output, "historical-window") || !strings.Contains(output, "production") {
		t.Fatalf("bare ctx did not list both contexts:\n%s", output)
	}
	if output := run("ctx-current"); !strings.Contains(output, "historical-window") {
		t.Fatalf("ctx current did not report replay context:\n%s", output)
	}
	if output := run("ctx-describe"); !strings.Contains(output, "production.example.invalid") {
		t.Fatalf("ctx describe did not inspect the target context:\n%s", output)
	}
	if output := run("ctx-switch"); !strings.Contains(output, "Switched to context") {
		t.Fatalf("ctx <name> did not switch context:\n%s", output)
	}

	afterConfig, err := config.LoadFrom(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if afterConfig.CurrentContext != "production" {
		t.Fatalf("persisted current context = %q, want production", afterConfig.CurrentContext)
	}
	afterState, err := store.Status(locator)
	if err != nil {
		t.Fatal(err)
	}
	if afterState.SessionID != started.SessionID || afterState.Revision != started.Revision {
		t.Fatal("context switch stopped or rewrote the replay session")
	}
	if output := run("status-override"); !strings.Contains(output, started.SessionID) {
		t.Fatalf("--context did not retain access to replay status:\n%s", output)
	}
}

func TestReplayExecuteEnforcesGuardInRealRootPipeline(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	cfg := replayCLIConfig(standardReplayBlock("manual"))
	cfg.Aliases = map[string]string{
		"purge-context": "ctx delete old-context",
		"shell-escape":  "!true",
	}
	cfg.Tokens = []config.NamedToken{{Name: "synthetic-reader", Token: "synthetic-secret-must-not-print"}}
	writeReplayCLIConfig(t, configPath, cfg)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		want string
	}{
		{"ctx-token", "ctx token"},
		{"ctx-rm", "ctx delete"},
		{"query", "no replay session is active"},
		{"exec-dql", "no replay session is active"},
		{"plugin", "plugin command"},
		{"root-alias", "ctx delete"},
		{"shell-alias", "shell alias"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command(executable, "-test.run=^TestReplayExecuteHelper$")
			command.Env = replayExecuteHelperEnv(os.Environ(), map[string]string{
				"DTCTL_REPLAY_EXECUTE_HELPER": "1",
				"DTCTL_REPLAY_EXECUTE_CONFIG": configPath,
				"DTCTL_REPLAY_EXECUTE_CASE":   test.name,
				"DTCTL_PROFILE":               config.ProfileFull,
				"XDG_STATE_HOME":              filepath.Join(dir, "state-home"),
				"DTCTL_CONTEXT":               "",
			})
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("helper process failed: %v\n%s", err, output)
			}
			text := string(output)
			if !strings.Contains(text, test.want) {
				t.Fatalf("output does not contain %q:\n%s", test.want, text)
			}
			if strings.Contains(text, "synthetic-secret-must-not-print") {
				t.Fatalf("blocked invocation disclosed the credential:\n%s", text)
			}
		})
	}
}

func replayExecuteHelperEnv(current []string, replacements map[string]string) []string {
	filtered := make([]string, 0, len(current)+len(replacements))
	for _, entry := range current {
		name, _, _ := strings.Cut(entry, "=")
		if _, replace := replacements[name]; !replace {
			filtered = append(filtered, entry)
		}
	}
	for name, value := range replacements {
		filtered = append(filtered, name+"="+value)
	}
	return filtered
}
