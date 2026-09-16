package cmd

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
