package cmd

import (
	"strings"
	"testing"

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
