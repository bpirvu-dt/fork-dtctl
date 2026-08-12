package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dynatrace-oss/dtctl/cmd/testutil"
	"github.com/dynatrace-oss/dtctl/pkg/commands"
	"github.com/dynatrace-oss/dtctl/pkg/config"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestRestrictedReplayDiscoveryHidesManagementAndExplain(t *testing.T) {
	configPath := writeReplayDiscoveryConfig(t, session.ReplayDisclosureRestricted)
	root, replayCommand, queryCommand, calls := replayDiscoveryTree()

	if err := applyReplayDisclosureDiscovery(root, []string{"--config", configPath}); err != nil {
		t.Fatal(err)
	}
	if !replayCommand.Hidden {
		t.Fatal("restricted discovery exposed replay management")
	}
	if flag := queryCommand.Flags().Lookup("explain-replay"); flag == nil || !flag.Hidden {
		t.Fatal("restricted discovery exposed --explain-replay")
	}

	var rootHelp bytes.Buffer
	root.SetOut(&rootHelp)
	if err := root.Help(); err != nil {
		t.Fatal(err)
	}
	assertNoRestrictedGeneratedWords(t, "root help", rootHelp.String())

	var queryHelp bytes.Buffer
	queryCommand.SetOut(&queryHelp)
	if err := queryCommand.Help(); err != nil {
		t.Fatal(err)
	}
	assertNoRestrictedGeneratedWords(t, "query help", queryHelp.String())

	listing := commands.Build(root)
	listing.Version = "synthetic-version"
	rawListing, err := json.Marshal(listing)
	if err != nil {
		t.Fatal(err)
	}
	assertNoRestrictedGeneratedWords(t, "command catalog", string(rawListing))

	var completion bytes.Buffer
	if err := root.GenBashCompletionV2(&completion, true); err != nil {
		t.Fatal(err)
	}
	assertNoRestrictedGeneratedWords(t, "completion", completion.String())
	testutil.AssertGolden(t, "replay/discovery-restricted", fmt.Sprintf(
		"Root help:\n%s\nQuery help:\n%s\nCatalog:\n%s\nCompletion disclosure entries: <none>\n",
		trimDiscoveryGoldenWhitespace(rootHelp.String()), trimDiscoveryGoldenWhitespace(queryHelp.String()), rawListing,
	))

	// Cobra Hidden affects discovery only. A same-user caller that already
	// knows a lifecycle verb can still invoke every one explicitly.
	for _, verb := range []string{"start", "advance", "status", "stop"} {
		root.SetArgs([]string{"replay", verb})
		if err := root.Execute(); err != nil {
			t.Fatalf("explicit replay %s: %v", verb, err)
		}
		if calls[verb] != 1 {
			t.Fatalf("hidden replay %s was not explicitly callable", verb)
		}
	}
}

func TestFullReplayDiscoveryRemainsUnchanged(t *testing.T) {
	configPath := writeReplayDiscoveryConfig(t, session.ReplayDisclosureFull)
	root, replayCommand, queryCommand, _ := replayDiscoveryTree()

	if err := applyReplayDisclosureDiscovery(root, []string{"--config", configPath}); err != nil {
		t.Fatal(err)
	}
	if replayCommand.Hidden {
		t.Fatal("full disclosure hid replay management")
	}
	if flag := queryCommand.Flags().Lookup("explain-replay"); flag == nil || flag.Hidden {
		t.Fatal("full disclosure hid --explain-replay")
	}
}

func TestReplayDiscoveryUsesSelectedContextDisclosure(t *testing.T) {
	dir := t.TempDir()
	full := standardReplayBlock(session.ReplayClockManual)
	full.Disclosure = session.ReplayDisclosureFull
	cfg := replayCLIConfig(full)
	restricted := cfg.Contexts[0]
	restricted.Name = "restricted-window"
	restricted.Context.Replay = standardReplayBlock(session.ReplayClockManual)
	restricted.Context.Replay.Disclosure = session.ReplayDisclosureRestricted
	cfg.Contexts = append(cfg.Contexts, restricted)
	configPath := filepath.Join(dir, "config.yaml")
	writeReplayCLIConfig(t, configPath, cfg)
	originalStateDir := replayStateDirectory
	replayStateDirectory = filepath.Join(dir, "state")
	t.Cleanup(func() { replayStateDirectory = originalStateDir })

	root, replayCommand, queryCommand, _ := replayDiscoveryTree()
	if err := applyReplayDisclosureDiscovery(root, []string{"--config", configPath, "--context", "restricted-window"}); err != nil {
		t.Fatal(err)
	}
	if !replayCommand.Hidden || !queryCommand.Flags().Lookup("explain-replay").Hidden {
		t.Fatal("discovery ignored the selected restricted context")
	}
}

func TestReplayDiscoveryStoredRestrictedRouteSurvivesContextDrift(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	stateDir := filepath.Join(dir, "state")
	raw := standardReplayBlock(session.ReplayClockManual)
	raw.Disclosure = session.ReplayDisclosureRestricted
	cfg := replayCLIConfig(raw)
	writeReplayCLIConfig(t, configPath, cfg)
	configureReplayCLI(t, configPath, stateDir, &replayCLIFakeClock{
		now: time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC),
	})
	if result := runReplayCLI(t, "table", "start"); result.err != nil {
		t.Fatal(result.err)
	}

	// Remove the current context block entirely. The valid stored state still
	// owns the restricted route, so discovery cannot widen itself while
	// reporting configuration drift.
	cfg.Contexts[0].Context.Replay = nil
	writeReplayCLIConfig(t, configPath, cfg)
	root, replayCommand, queryCommand, _ := replayDiscoveryTree()
	if err := applyReplayDisclosureDiscovery(root, []string{"--config", configPath}); err != nil {
		t.Fatal(err)
	}
	if !replayCommand.Hidden || !queryCommand.Flags().Lookup("explain-replay").Hidden {
		t.Fatal("stored restricted disclosure widened after context drift")
	}
}

func replayDiscoveryTree() (*cobra.Command, *cobra.Command, *cobra.Command, map[string]int) {
	root := &cobra.Command{Use: "dtctl", SilenceErrors: true, SilenceUsage: true}
	calls := make(map[string]int)
	replayCommand := &cobra.Command{Use: "replay"}
	for _, verb := range []string{"start", "advance", "status", "stop"} {
		verb := verb
		replayCommand.AddCommand(&cobra.Command{Use: verb, RunE: func(*cobra.Command, []string) error {
			calls[verb]++
			return nil
		}})
	}
	queryCommand := &cobra.Command{Use: "query", RunE: func(*cobra.Command, []string) error { return nil }}
	queryCommand.Flags().Bool("explain-replay", false, "explain replay query preparation")
	root.AddCommand(replayCommand, queryCommand)
	return root, replayCommand, queryCommand, calls
}

func writeReplayDiscoveryConfig(t *testing.T, disclosure string) string {
	t.Helper()
	dir := t.TempDir()
	raw := standardReplayBlock(session.ReplayClockManual)
	raw.Disclosure = disclosure
	cfg := replayCLIConfig(raw)
	configPath := filepath.Join(dir, "config.yaml")
	writeReplayCLIConfig(t, configPath, cfg)
	originalStateDir := replayStateDirectory
	replayStateDirectory = filepath.Join(dir, "state")
	t.Cleanup(func() { replayStateDirectory = originalStateDir })
	t.Setenv(config.ProfileEnvVar, "")
	return configPath
}

func assertNoRestrictedGeneratedWords(t *testing.T, surface, value string) {
	t.Helper()
	lower := strings.ToLower(value)
	for _, word := range []string{"replay", "virtual", "session", "clock", "interval", "effective"} {
		if strings.Contains(lower, word) {
			t.Fatalf("restricted %s contains %q: %s", surface, word, value)
		}
	}
}

func trimDiscoveryGoldenWhitespace(value string) string {
	lines := strings.Split(value, "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " \t")
	}
	return strings.Join(lines, "\n")
}
