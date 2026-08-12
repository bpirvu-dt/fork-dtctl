package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestRestrictedRetentionWarningsStayOutOfOrdinaryCLIOutput(t *testing.T) {
	tests := []struct {
		name             string
		dataStart        time.Time
		inspectionStatus int
		provenanceCode   string
		provenanceText   string
	}{
		{
			name:           "known boundary",
			dataStart:      mustReplayCLITime("2026-06-01T00:00:00Z"),
			provenanceCode: "retention_boundary",
			provenanceText: "shortest current logs retention setting",
		},
		{
			name:             "inspection failure",
			dataStart:        mustReplayCLITime("2026-08-10T10:50:02.718012207Z"),
			inspectionStatus: http.StatusForbidden,
			provenanceCode:   "retention_not_verified",
			provenanceText:   "not verified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &replayCLIQueryAPI{
				t: t, originalQuery: replayCLIRecordOriginal, inspectionStatus: tt.inspectionStatus,
				originalBody:   replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/parse.json"),
				validationBody: replayCLIQueryFixtureBody(t, "phase0b/fixtures/records/logs/01-to-at-t/validation-parse.json"),
				executeResponse: &sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{
					Records: []map[string]interface{}{{"matched": float64(1)}},
				}},
			}
			server := httptest.NewServer(api)
			defer server.Close()
			fixture := newReplayCLIQueryFixture(
				t, server.URL, session.ReplayDisclosureRestricted, session.ReplayClockManual,
				tt.dataStart, mustReplayCLITime("2026-08-10T10:55:02.718012207Z"), mustReplayCLITime("2026-08-10T11:05:02.718012207Z"),
			)
			setReplayQueryFlags(t, false, time.Minute, false)
			previousAgentMode := agentMode
			agentMode = true
			t.Cleanup(func() { agentMode = previousAgentMode })

			var runErr error
			stdout, stderr := captureReplayQueryStreams(t, func() {
				runErr = queryCmd.RunE(queryCmd, []string{replayCLIRecordOriginal})
			})
			if runErr != nil {
				t.Fatal(runErr)
			}
			if stderr != "" {
				t.Fatalf("restricted retention warning reached stderr: %q", stderr)
			}
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
				t.Fatalf("decode restricted envelope %q: %v", stdout, err)
			}
			delete(envelope, "result")
			generated, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			assertNoRestrictedGeneratedWords(t, "retention warning envelope outside result", string(generated))

			provenance, err := os.ReadFile(fixture.state.ProvenancePath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(provenance), tt.provenanceCode) || !strings.Contains(string(provenance), tt.provenanceText) {
				t.Fatalf("restricted provenance missing retention warning: %s", provenance)
			}
			if parses, executes := api.counts(); parses != 2 || executes != 1 {
				t.Fatalf("parse=%d execute=%d, warning must not block", parses, executes)
			}
			if inspections := api.inspectionCount(); inspections != 1 {
				t.Fatalf("retention inspections=%d, want one", inspections)
			}
		})
	}
}
