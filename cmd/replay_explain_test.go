package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/cmd/testutil"
	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/suggest"
)

func TestRestrictedExplainFlagMatchesGenuinelyUnknownFlagResponse(t *testing.T) {
	restricted := restrictedExplainFlagError()
	unknown := suggest.ParseFlagError("unknown flag: --synthetic", nil)
	if exitCodeForError(restricted) != exitCodeForError(unknown) {
		t.Fatalf("exit codes differ: restricted=%d unknown=%d", exitCodeForError(restricted), exitCodeForError(unknown))
	}
	restrictedDetail := errorToDetail(restricted)
	unknownDetail := errorToDetail(unknown)
	if restrictedDetail.Code != unknownDetail.Code {
		t.Fatalf("error codes differ: restricted=%q unknown=%q", restrictedDetail.Code, unknownDetail.Code)
	}
	if got, want := strings.Replace(restrictedDetail.Message, "explain-replay", "synthetic", 1), unknownDetail.Message; got != want {
		t.Fatalf("message formats differ: restricted=%q unknown=%q", restrictedDetail.Message, unknownDetail.Message)
	}
}

func TestReplayTopologyExplainShowsStructuralFeederAndWindow(t *testing.T) {
	const fixture = "topology/fixtures/traverse-limit/"
	original, err := os.ReadFile(filepath.Join("..", "sdk", "api", "query", "testdata", fixture, "original.dql"))
	if err != nil {
		t.Fatal(err)
	}
	ast, err := execreplay.AdaptJSON(replayCLIQueryFixtureBody(t, fixture+"parse.json"))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	now := start.Add(2 * time.Hour)
	compiled, err := execreplay.Compile(execreplay.CompileInput{
		OriginalDQL: string(original), AST: ast, VirtualNow: now, VirtualStart: now,
		SourcePolicy:    execreplay.Milestone1SourcePolicy(),
		ReplayInterval:  execreplay.Interval{Start: start, End: start.Add(4 * time.Hour)},
		VisibleInterval: execreplay.Interval{Start: start, End: now}, TimezoneName: "UTC", Timezone: time.UTC,
	})
	if err != nil {
		t.Fatal(err)
	}
	origFormat, origAgent, origPlain := outputFormat, agentMode, plainMode
	t.Cleanup(func() { outputFormat, agentMode, plainMode = origFormat, origAgent, origPlain })
	agentMode, plainMode = false, true
	for _, format := range []string{"table", "json"} {
		t.Run(format, func(t *testing.T) {
			outputFormat = format
			got := captureStdout(t, func() {
				if err := printReplayExplanation(compiled.Explain); err != nil {
					t.Fatal(err)
				}
			})
			for _, required := range []string{"topology", "window_only", "root.children[0]", "root.children[4]", "2026-08-01T11:59:00Z", "2026-08-01T12:00:00Z"} {
				if !strings.Contains(got, required) {
					t.Fatalf("explain output omits %q: %s", required, got)
				}
			}
			testutil.AssertGolden(t, "replay/topology-explain-"+format, got)
		})
	}
}
