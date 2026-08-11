package session

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplayConfigSourceIdentityExplicitEnvAndLocalRoutes(t *testing.T) {
	dir := t.TempDir()
	explicitPath := filepath.Join(dir, "explicit.yaml")
	if err := NewConfig().SaveTo(explicitPath); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvConfig, explicitPath)
	explicit, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	assertReplaySourcePath(t, explicit, explicitPath)

	t.Setenv(EnvConfig, "")
	project := filepath.Join(dir, "project")
	nested := filepath.Join(project, "nested")
	if err := os.MkdirAll(nested, 0700); err != nil {
		t.Fatal(err)
	}
	localPath := filepath.Join(project, LocalConfigName)
	if err := NewConfig().SaveTo(localPath); err != nil {
		t.Fatal(err)
	}
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(nested); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWD) })
	local, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !local.IsLocal() {
		t.Fatal("auto-discovered config was not marked local")
	}
	assertReplaySourcePath(t, local, localPath)
}

func TestReplayConfigSourceIdentityGlobalRoute(t *testing.T) {
	if os.Getenv("DTCTL_REPLAY_GLOBAL_SOURCE_HELPER") == "1" {
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		source, err := cfg.SourceIdentity()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(os.Getenv("DTCTL_REPLAY_GLOBAL_SOURCE_RESULT"), []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		return
	}

	dir := t.TempDir()
	configHome := filepath.Join(dir, "config-home")
	globalPath := filepath.Join(configHome, "dtctl", "config")
	if err := os.MkdirAll(filepath.Dir(globalPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := NewConfig().SaveTo(globalPath); err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(dir, "source-result")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestReplayConfigSourceIdentityGlobalRoute$")
	command.Env = replaySourceHelperEnv(os.Environ(), map[string]string{
		"XDG_CONFIG_HOME":                   configHome,
		"DTCTL_CONFIG":                      "",
		"DTCTL_REPLAY_GLOBAL_SOURCE_HELPER": "1",
		"DTCTL_REPLAY_GLOBAL_SOURCE_RESULT": resultPath,
	})
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("global source helper: %v\n%s", err, output)
	}
	data, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(globalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != filepath.Clean(want) {
		t.Fatalf("global source identity = %q, want %q", data, filepath.Clean(want))
	}
}

func assertReplaySourcePath(t *testing.T, cfg *Config, path string) {
	t.Helper()
	got, err := cfg.SourceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Clean(want) {
		t.Fatalf("source identity = %q, want %q", got, filepath.Clean(want))
	}
}

func replaySourceHelperEnv(current []string, replacements map[string]string) []string {
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
