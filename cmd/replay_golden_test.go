package cmd

import (
	"bytes"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dynatrace-oss/dtctl/cmd/testutil"
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
			name: "query-not-implemented",
			err: &ReplayQueryUnavailableError{
				Command:     "query",
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
