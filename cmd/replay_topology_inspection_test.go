package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dynatrace-oss/dtctl/pkg/exec"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestReplayTopologyExplainRejectsWithoutExecutionOrRetry(t *testing.T) {
	for _, test := range []struct {
		name        string
		fixture     string
		virtualNow  string
		wantMessage string
	}{
		{"relative five seconds", "nodes-subminute", "2026-08-01T12:00:00Z", "at least 60 seconds"},
		{"widening twenty seconds", "nodes-window", "2026-08-01T11:00:20Z", "at least 60 seconds"},
		{"traverse without feeder", "traverse-after-data", "2026-08-01T12:00:00Z", "traverse has no topology feeder"},
	} {
		for _, clockMode := range []string{session.ReplayClockManual, session.ReplayClockRealtime} {
			for _, live := range []bool{false, true} {
				name := test.name + "/" + clockMode
				if live {
					name += "/live flag"
				}
				t.Run(name, func(t *testing.T) {
					fixturePath := "topology/fixtures/" + test.fixture + "/"
					original, err := os.ReadFile(filepath.Join("..", "sdk", "api", "query", "testdata", fixturePath, "original.dql"))
					if err != nil {
						t.Fatal(err)
					}
					api := &replayCLIQueryAPI{
						t: t, originalQuery: string(original), originalBody: replayCLIQueryFixtureBody(t, fixturePath+"parse.json"),
					}
					server := httptest.NewServer(api)
					defer server.Close()
					fixture := newReplayCLIQueryFixture(t, server.URL, session.ReplayDisclosureFull, clockMode,
						mustReplayCLITime("2026-08-01T10:00:00Z"), mustReplayCLITime(test.virtualNow), mustReplayCLITime("2026-08-01T14:00:00Z"))
					setReplayQueryFlags(t, live, time.Minute, true)
					var runErr error
					stdout, stderr := captureReplayQueryStreams(t, func() {
						runErr = queryCmd.RunE(queryCmd, []string{string(original)})
					})
					if runErr == nil || !strings.Contains(runErr.Error(), test.wantMessage) ||
						exec.ReplayTemporaryNonOverlap(runErr) || !exec.ReplayLoopHardFailure(runErr) {
						t.Fatalf("stdout=%q stderr=%q error=%v", stdout, stderr, runErr)
					}
					if parses, executes := api.counts(); parses != 1 || executes != 0 || api.verifyCount() != 0 {
						t.Fatalf("parse=%d execute=%d verify=%d, want 1/0/0", parses, executes, api.verifyCount())
					}
					state, err := fixture.store.Status(fixture.locator)
					if err != nil || state.Status != session.ReplayStatusActive || state.CompletedAt != nil {
						t.Fatalf("explain changed state: %#v err=%v", state, err)
					}
				})
			}
		}
	}
}

func TestReplayTopologyVerifyReportsIncompatibilityWithoutExecutionOrRetry(t *testing.T) {
	for _, test := range []struct {
		name          string
		fixture       string
		virtualNow    string
		wantMessage   string
		wantConstruct string
	}{
		{"relative five seconds", "nodes-subminute", "2026-08-01T12:00:00Z", "at least 60 seconds", "topology timeframe"},
		{"widening twenty seconds", "nodes-window", "2026-08-01T11:00:20Z", "at least 60 seconds", "topology timeframe"},
		{"traverse without feeder", "traverse-after-data", "2026-08-01T12:00:00Z", "traverse has no topology feeder", "traverse"},
	} {
		for _, clockMode := range []string{session.ReplayClockManual, session.ReplayClockRealtime} {
			for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
				t.Run(test.name+"/"+clockMode+"/"+disclosure, func(t *testing.T) {
					fixturePath := "topology/fixtures/" + test.fixture + "/"
					original, err := os.ReadFile(filepath.Join("..", "sdk", "api", "query", "testdata", fixturePath, "original.dql"))
					if err != nil {
						t.Fatal(err)
					}
					api := &replayCLIQueryAPI{
						t: t, originalQuery: string(original), originalBody: replayCLIQueryFixtureBody(t, fixturePath+"parse.json"),
					}
					server := httptest.NewServer(api)
					defer server.Close()
					fixture := newReplayCLIQueryFixture(t, server.URL, disclosure, clockMode,
						mustReplayCLITime("2026-08-01T10:00:00Z"), mustReplayCLITime(test.virtualNow), mustReplayCLITime("2026-08-01T14:00:00Z"))
					setReplayVerifyQueryFlags(t)
					// Run the real handler with its normal flags and an explicit
					// output flag, without changing the shared root flag set.
					inspectionCommand := &cobra.Command{}
					inspectionCommand.Flags().String("output", "json", "")
					inspectionCommand.Flags().AddFlagSet(verifyQueryCmd.Flags())
					waits := 0
					replayQueryWaitFunc = func(context.Context, time.Duration) error {
						waits++
						return context.Canceled
					}
					statePath := fixture.store.StatePath(fixture.locator.ContextKey)
					before, err := os.ReadFile(statePath)
					if err != nil {
						t.Fatal(err)
					}
					var runErr error
					stdout, stderr := captureReplayQueryStreams(t, func() {
						runErr = verifyQueryCmd.RunE(inspectionCommand, []string{string(original)})
					})
					if runErr != nil || stderr != "" {
						t.Fatalf("verify changed the successful original-DQL outcome: stdout=%q stderr=%q error=%v", stdout, stderr, runErr)
					}
					var report map[string]any
					if err := json.Unmarshal([]byte(stdout), &report); err != nil {
						t.Fatalf("decode verification output: %v; stdout=%q", err, stdout)
					}
					if report["valid"] != true || report["canonicalQuery"] != string(original) {
						t.Fatalf("original-DQL verification result changed: %#v", report)
					}
					compatibility, ok := report["replay"].(map[string]any)
					if disclosure == session.ReplayDisclosureRestricted {
						if len(report) != 2 || ok || strings.Contains(stdout, test.wantMessage) {
							t.Fatalf("restricted verify changed ordinary output or exposed compatibility details: %s", stdout)
						}
						provenance, err := os.ReadFile(fixture.state.ProvenancePath)
						if err != nil {
							t.Fatal(err)
						}
						lines := bytes.Split(bytes.TrimSpace(provenance), []byte("\n"))
						if len(lines) != 1 {
							t.Fatalf("verify recorded %d attempts, want one: %s", len(lines), provenance)
						}
						var record session.ReplayProvenanceRecord
						if err := json.Unmarshal(lines[0], &record); err != nil {
							t.Fatal(err)
						}
						detail, _ := record.Fields["detail"].(string)
						if record.Event != "query_pre_execution" || !strings.Contains(detail, test.wantMessage) ||
							record.Fields["original_dql"] != string(original) || record.Fields["effective_dql"] != "" {
							t.Fatalf("restricted verification lost rejection details: %+v", record)
						}
						compatibility, ok = record.Fields["verification"].(map[string]any)
					}
					if !ok || compatibility["original_dql_valid"] != true || compatibility["compiler_supported"] != false || compatibility["effective_query_valid"] != false {
						t.Fatalf("missing incompatible replay verdict: %#v", compatibility)
					}
					unsupported, ok := compatibility["unsupported_constructs"].([]any)
					if !ok || len(unsupported) != 1 || unsupported[0] != test.wantConstruct {
						t.Fatalf("unsupported constructs = %#v, want %q", compatibility["unsupported_constructs"], test.wantConstruct)
					}
					if parses, executes := api.counts(); parses != 1 || executes != 0 || api.verifyCount() != 1 ||
						api.coverageCount() != 0 || api.inspectionCount() != 0 || waits != 0 {
						t.Fatalf("parse=%d execute=%d verify=%d coverage=%d inspection=%d waits=%d, want 1/0/1/0/0/0",
							parses, executes, api.verifyCount(), api.coverageCount(), api.inspectionCount(), waits)
					}
					after, err := os.ReadFile(statePath)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatalf("verify changed session state: %v", err)
					}
				})
			}
		}
	}
}
