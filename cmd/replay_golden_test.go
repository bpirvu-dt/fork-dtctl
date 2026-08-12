package cmd

import (
	"bytes"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dynatrace-oss/dtctl/cmd/testutil"
	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/output"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func replayGoldenStatus() ReplayStatusOutput {
	return ReplayStatusOutput{
		ContextName:        "historical-window",
		Status:             session.ReplayStatusActive,
		SessionID:          "0123456789abcdef0123456789abcdef",
		SessionStartedAt:   "2026-08-11T09:00:00Z",
		HostNow:            "2026-08-11T09:30:00Z",
		ClockMode:          session.ReplayClockRealtime,
		AnchorHost:         "2026-08-11T09:00:00Z",
		AnchorVirtual:      "2026-06-14T10:00:00Z",
		VirtualStart:       "2026-06-14T10:00:00Z",
		VirtualNow:         "2026-06-14T10:30:00Z",
		DataStart:          "2026-06-14T08:00:00Z",
		DataEnd:            "2026-06-14T12:00:00Z",
		VisibleEnd:         "2026-06-14T10:30:00Z",
		Position:           "inside-replay-interval",
		ConfigurationDrift: false,
		StateKey:           "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		UnreadableStateFiles: []string{
			"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.state.json",
		},
	}
}

func TestReplayEmptyDisclosureMatchesExplicitFullStatusBytes(t *testing.T) {
	origFormat, origAgent, origPlain := outputFormat, agentMode, plainMode
	t.Cleanup(func() { outputFormat, agentMode, plainMode = origFormat, origAgent, origPlain })
	outputFormat, agentMode, plainMode = "json", false, false
	now := time.Date(2026, 8, 11, 9, 30, 0, 0, time.UTC)
	base := session.ReplaySession{
		SessionID:        "0123456789abcdef0123456789abcdef",
		Status:           session.ReplayStatusActive,
		ContextName:      "historical-window",
		ContextKey:       session.ContextKey("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		DataStart:        time.Date(2026, 6, 14, 8, 0, 0, 0, time.UTC),
		DataEnd:          time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC),
		VirtualStart:     time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC),
		ClockMode:        session.ReplayClockRealtime,
		Disclosure:       session.ReplayDisclosureFull,
		SessionStartedAt: time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC),
		AnchorHost:       time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC),
		AnchorVirtual:    time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC),
	}
	defaulted := base
	defaulted.ValueSources.Disclosure = session.ReplayValueFromDefault
	explicit := base
	explicit.ValueSources.Disclosure = session.ReplayValueFromContext

	render := func(state session.ReplaySession) string {
		var buf bytes.Buffer
		cmd := &cobra.Command{Use: "status"}
		cmd.SetOut(&buf)
		if err := printReplayStatus(cmd, replayStatusFromSession(state, now, false)); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	if got, want := render(defaulted), render(explicit); got != want {
		t.Fatalf("empty and explicit full disclosure changed status bytes:\n--- empty ---\n%s--- full ---\n%s", got, want)
	}
}

func TestReplayStatusGoldens(t *testing.T) {
	origFormat, origAgent, origPlain := outputFormat, agentMode, plainMode
	t.Cleanup(func() { outputFormat, agentMode, plainMode = origFormat, origAgent, origPlain })
	agentMode, plainMode = false, false
	for _, format := range []string{"table", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			outputFormat = format
			var buf bytes.Buffer
			cmd := &cobra.Command{Use: "status"}
			cmd.SetOut(&buf)
			if err := printReplayStatus(cmd, replayGoldenStatus()); err != nil {
				t.Fatal(err)
			}
			testutil.AssertGolden(t, "replay/status-"+format, testutil.StripANSI(buf.String()))
		})
	}
}

func TestReplayExplainGolden(t *testing.T) {
	origFormat, origAgent, origPlain := outputFormat, agentMode, plainMode
	t.Cleanup(func() { outputFormat, agentMode, plainMode = origFormat, origAgent, origPlain })
	outputFormat, agentMode, plainMode = "table", false, false
	dataStart := time.Date(2026, 6, 14, 8, 0, 0, 0, time.UTC)
	dataEnd := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)
	virtualStart := time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC)
	virtualNow := time.Date(2026, 6, 14, 10, 30, 0, 0, time.UTC)
	requested := execreplay.RequestedRange{
		Range: execreplay.Interval{Start: virtualNow.Add(-time.Hour), End: virtualNow},
		Basis: "explicit from with implicit virtual-now end",
	}
	effective := execreplay.Interval{Start: virtualNow.Add(-time.Hour), End: virtualNow}
	value := execreplay.ExplainData{
		Clock: execreplay.ClockExplain{
			VirtualNow: virtualNow, VirtualStart: virtualStart,
			ReplayInterval:  execreplay.Interval{Start: dataStart, End: dataEnd},
			VisibleInterval: execreplay.Interval{Start: dataStart, End: virtualNow},
			Locale:          "en_US", Timezone: "UTC",
		},
		Sources: []execreplay.SourceExplain{{
			Ordinal: 0, Class: execreplay.SourceRecord, Name: "logs",
			BoundaryPolicy: execreplay.BoundaryExact, Requested: &requested, Effective: &effective,
			Classification: execreplay.OverlapPresent,
			Proof:          execreplay.OverlapProof{Reason: "the requested source range intersects the visible replay interval"},
		}},
		EffectiveDQL: `fetch logs, from:toTimestamp("2026-06-14T09:30:00Z"), to:toTimestamp("2026-06-14T10:30:00Z")`,
	}
	got := captureStdout(t, func() {
		if err := printReplayExplanation(value); err != nil {
			t.Fatal(err)
		}
	})
	testutil.AssertGolden(t, "replay/explain-table", got)
}

func TestReplayErrorGoldens(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "hard-guard",
			err: &ReplayGuardError{
				Command:     "ctx token",
				ContextName: "historical-window",
			},
		},
		{
			name: "exec-dql-not-implemented",
			err: &ReplayQueryUnavailableError{
				Command:     "exec dql",
				ContextName: "historical-window",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := output.PrintError(&buf, errorToDetail(test.err)); err != nil {
				t.Fatal(err)
			}
			testutil.AssertGolden(t, "replay/error-"+test.name, buf.String())
		})
	}
}

func TestReplayRestrictedQueryErrorGoldens(t *testing.T) {
	tests := []struct {
		name    string
		message string
	}{
		{"non-overlap", "No data is available for the requested timeframe. The query was not executed."},
		{"temporary-no-data", "no data yet for the requested timeframe; retrying"},
		{"readiness", "this context is not ready for queries"},
		{"preparation", "The query could not be prepared. It was not executed."},
		{"result-validation", "The returned data could not be validated. No result was returned."},
		{"finalization", "The result could not be finalized. No result was returned."},
		{"sink-preflight", "Required local recording is unavailable. The query was not executed."},
		{"sink-post-execution", "Required local recording failed. No result was returned."},
		{"remote", "The query failed. No result was returned."},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := output.PrintError(&buf, &output.ErrorDetail{Code: "query_failed", Message: test.message}); err != nil {
				t.Fatal(err)
			}
			testutil.AssertGolden(t, "replay/error-restricted-"+test.name, buf.String())
		})
	}
}
