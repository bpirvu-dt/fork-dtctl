package cmd

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dynatrace-oss/dtctl/cmd/testutil"
	"github.com/dynatrace-oss/dtctl/pkg/exec"
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
			name: "restricted-hard-guard",
			err: &ReplayGuardError{
				Command: "ctx token", ContextName: "historical-window", Restricted: true,
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
		{"davis-current-view", "The query could not be prepared. It was not executed."},
		{"other", "The query could not be prepared. It was not executed."},
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

func TestReplayRestrictedNonMessageMappingGoldens(t *testing.T) {
	renderError := func(t *testing.T, err error) string {
		t.Helper()
		var buf bytes.Buffer
		if err := output.PrintError(&buf, errorToDetail(err)); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	verify := &exec.DQLVerifyResponse{Valid: true, CanonicalQuery: "fetch logs"}
	var verifyBuf bytes.Buffer
	if err := output.NewPrinterWithWriter("json", &verifyBuf).Print(verify); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		value string
	}{
		{"remote-passthrough", renderError(t, &exec.QueryError{StatusCode: 500, ErrorType: "SYNTHETIC_REMOTE", Message: "synthetic remote failure"})},
		{"warning-suppressed", ""},
		{"completion-note-suppressed", ""},
		{"verify-normal-response", verifyBuf.String()},
		{"explain-unknown-flag", renderError(t, restrictedExplainFlagError())},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			testutil.AssertGolden(t, "replay/error-restricted-"+test.name, test.value)
		})
	}
}

func replayGoldenMetadata() *output.ReplayMetadata {
	return &output.ReplayMetadata{
		Active: true, SessionID: "0123456789abcdef0123456789abcdef", SessionStartedAt: "2026-08-11T09:00:00Z",
		ClockMode: session.ReplayClockManual, AnchorHost: "2026-08-11T09:00:00Z", AnchorVirtual: "2026-06-14T10:00:00Z",
		VirtualNow: "2026-06-14T10:30:00Z", DataStart: "2026-06-14T08:00:00Z", DataEnd: "2026-06-14T12:00:00Z",
		VisibleEnd: "2026-06-14T10:30:00Z", State: session.ReplayStatusActive,
		OriginalQuery:                "fetch logs, from:now()-1h",
		EffectiveQuery:               `fetch logs, from:toTimestamp("2026-06-14T09:30:00Z"), to:toTimestamp("2026-06-14T10:30:00Z")`,
		GrailCanonicalEffectiveQuery: `fetch logs, from:toTimestamp("2026-06-14T09:30:00Z"), to:toTimestamp("2026-06-14T10:30:00Z")`,
		Sources: []output.ReplaySourceMetadata{{
			Ordinal: 0, Path: "root.children[0]", Name: "logs", Class: "record", BoundaryPolicy: "exact_half_open",
			RequestedFrom: "2026-06-14T09:30:00Z", RequestedTo: "2026-06-14T10:30:00Z",
			EffectiveFrom: "2026-06-14T09:30:00Z", EffectiveTo: "2026-06-14T10:30:00Z",
			PhysicalFrom: "2026-06-14T09:30:00Z", PhysicalTo: "2026-06-14T10:30:00Z",
		}},
		Warnings: []string{"synthetic historical-resolution warning"},
	}
}

func TestReplayOutputSurfaceGoldens(t *testing.T) {
	replayMetadata := replayGoldenMetadata()
	manifest := &output.ResultFileManifest{
		Kind: output.KindResultFile, Path: "/synthetic/q-result.jsonl", Query: replayMetadata.OriginalQuery,
		EffectiveQuery: replayMetadata.EffectiveQuery, GrailCanonicalEffectiveQuery: replayMetadata.GrailCanonicalEffectiveQuery,
		Replay: replayMetadata, Format: "jsonl", Rows: 1, Bytes: 32, ContextName: "historical-window",
		SampleRows: []map[string]interface{}{{"message": "synthetic record"}},
	}
	sidecar := &output.SidecarManifest{
		EnvelopeVersion: output.EnvelopeVersion, Format: "jsonl", ContextName: "historical-window",
		Query: replayMetadata.OriginalQuery, EffectiveQuery: replayMetadata.EffectiveQuery,
		GrailCanonicalEffectiveQuery: replayMetadata.GrailCanonicalEffectiveQuery, Replay: replayMetadata,
		Rows: 1, Bytes: 32, Created: time.Date(2026, 8, 11, 9, 31, 0, 0, time.UTC),
	}
	fullAgent := output.Response{
		OK: true, EnvelopeVersion: output.EnvelopeVersion,
		Result:  &output.InlineRecords{Kind: output.KindRecords, Records: []map[string]interface{}{{"message": "synthetic record"}}},
		Context: &output.ResponseContext{Verb: "query", Resource: "logs", Decided: "inline"}, Replay: replayMetadata,
	}
	restrictedAgent := fullAgent
	restrictedAgent.Replay = nil

	encodeEnvelope := func(t *testing.T, response output.Response) string {
		t.Helper()
		var buf bytes.Buffer
		if err := output.EncodeEnvelope(&buf, response); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	encodeJSON := func(t *testing.T, value any) string {
		t.Helper()
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		return string(data) + "\n"
	}

	cases := []struct {
		name  string
		value string
	}{
		{"agent-full", encodeEnvelope(t, fullAgent)},
		{"agent-restricted", encodeEnvelope(t, restrictedAgent)},
		{"spill-full", encodeJSON(t, manifest)},
		{"sidecar-full", encodeJSON(t, sidecar)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			testutil.AssertGolden(t, "replay/"+test.name, test.value)
		})
	}
}

func TestReplayRestrictedContextAndDoctorOmissionGoldens(t *testing.T) {
	configPath := writeReplayDiscoveryConfig(t, session.ReplayDisclosureRestricted)
	originalConfig, originalContext := cfgFile, contextName
	originalFormat, originalAgent, originalPlain := outputFormat, agentMode, plainMode
	cfgFile, contextName = configPath, ""
	outputFormat, agentMode, plainMode = "table", false, true
	t.Cleanup(func() {
		cfgFile, contextName = originalConfig, originalContext
		outputFormat, agentMode, plainMode = originalFormat, originalAgent, originalPlain
	})

	var currentErr error
	current := captureStdout(t, func() {
		currentErr = ctxCurrentCmd.RunE(ctxCurrentCmd, nil)
	})
	if currentErr != nil {
		t.Fatal(currentErr)
	}
	var describeErr error
	describe := captureStdout(t, func() {
		describeErr = describeContext("historical-window")
	})
	if describeErr != nil {
		t.Fatal(describeErr)
	}
	var listErr error
	list := captureStdout(t, func() {
		listErr = listContexts()
	})
	if listErr != nil {
		t.Fatal(listErr)
	}
	doctor := captureStdout(t, func() {
		printDoctorResults([]checkResult{
			{Name: "Configuration", Status: "ok", Detail: "/synthetic/config.yaml"},
			{Name: "Current context", Status: "ok", Detail: "historical-window (environment: https://tenant.example.invalid, safety: readonly)"},
		})
	})

	for name, value := range map[string]string{"ctx current": current, "ctx describe": describe, "ctx list": list, "doctor": doctor} {
		assertNoRestrictedGeneratedWords(t, name, value)
	}
	testutil.AssertGolden(t, "replay/context-current-restricted", testutil.StripANSI(current))
	testutil.AssertGolden(t, "replay/context-describe-restricted", testutil.StripANSI(describe))
	testutil.AssertGolden(t, "replay/context-list-restricted", testutil.StripANSI(list))
	testutil.AssertGolden(t, "replay/doctor-restricted", testutil.StripANSI(doctor))
}
