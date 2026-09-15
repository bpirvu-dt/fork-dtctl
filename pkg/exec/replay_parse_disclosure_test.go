package exec

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/output"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestRestrictedOriginalParseDiagnosticPreservesUserDQL(t *testing.T) {
	for _, fragment := range append(execreplay.DavisProblemsReconstructionFragments(), execreplay.DavisProblemsSnapshotTable) {
		t.Run(fragment, func(t *testing.T) {
			info := restrictedInfo()
			info.OriginalQuery = "fetch " + execreplay.DavisProblemsSnapshotTable + " | " + fragment + " | filter"
			detail := &sdkquery.QueryError{StatusCode: http.StatusBadRequest, ErrorType: "SYNTAX_ERROR", Message: "Unexpected token near " + fragment}
			for _, candidate := range []error{detail, fmt.Errorf("parse failed: %w", detail)} {
				if got := restrictedPreparationMessage(candidate, info); got != candidate.Error() {
					t.Fatalf("original diagnostic was hidden: %q", got)
				}
			}

			info.EffectiveQuery = "fetch logs | limit 1"
			if got := restrictedPreparationMessage(detail, info); got != restrictedQueryInvalidMessage {
				t.Fatalf("rewritten-query diagnostic was exposed: %q", got)
			}
			info.EffectiveQuery = ""
			info.Output = &output.ReplayMetadata{Sources: []output.ReplaySourceMetadata{{DavisProblemsMapping: &output.DavisProblemsMappingMetadata{Eligible: true}}}}
			if got := restrictedPreparationMessage(detail, info); got != restrictedQueryInvalidMessage {
				t.Fatalf("recorded mapping detail was exposed: %q", got)
			}

			// The parser exception must not widen disclosure of authored guidance.
			info.Output = nil
			compilerErr := &execreplay.ReplayError{Code: execreplay.ErrorTimeframe, PublicMessage: detail.Message}
			if got := restrictedPreparationMessage(compilerErr, info); got != restrictedQueryInvalidMessage {
				t.Fatalf("compiler guidance exposed reconstruction text: %q", got)
			}
			validationErr := &execreplay.ResultContractError{PublicReason: detail.Message}
			if got := restrictedValidationMessage(validationErr, info); got != restrictedValidationFallbackMessage {
				t.Fatalf("validation guidance exposed reconstruction text: %q", got)
			}
		})
	}
}

func TestRestrictedOriginalParseDiagnosticStillProtectsInternals(t *testing.T) {
	info := restrictedInfo()
	info.OriginalQuery = "fetch " + execreplay.DavisProblemsSnapshotTable + " | filter"
	info.Output = &output.ReplayMetadata{
		VirtualNow: "2026-06-14T10:00:00Z", GrailCanonicalEffectiveQuery: "data record(value=1)",
	}
	for _, protected := range []string{
		"replay", "virtual", "session", "clock", "effective",
		"2026-06-14T10:00:00Z", "data record(value=1)",
	} {
		detail := &sdkquery.QueryError{StatusCode: http.StatusBadRequest, Message: "Unexpected token near " + execreplay.DavisProblemsSnapshotTable + ": " + protected}
		if got := restrictedPreparationMessage(detail, info); got != restrictedQueryInvalidMessage {
			t.Errorf("protected parser detail was exposed: %q", got)
		}
	}
}

func TestDQLExecutorRestrictedOriginalSnapshotParseError(t *testing.T) {
	const original = "fetch dt.davis.problems.snapshots | filter"
	const diagnostic = "Unexpected token after dt.davis.problems.snapshots"
	api := newReplayMockAPI(t)
	api.isOriginal = func(query string) bool { return query == original }
	api.parseStatus = http.StatusBadRequest
	api.parseErrorMessage = diagnostic
	sink := &replayTestSink{}
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted,
		replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, func(string) session.ProvenanceSink { return sink })
	result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{AgentMode: true})
	var queryErr *sdkquery.QueryError
	if result != nil || !errors.As(err, &queryErr) || !strings.Contains(err.Error(), diagnostic) {
		t.Fatalf("result=%v error=%v, want the original parser diagnostic", result, err)
	}
	if parses, executes, polls := api.counts(); parses != 1 || executes != 0 || polls != 0 {
		t.Fatalf("parser rejection changed execution: parses=%d executes=%d polls=%d", parses, executes, polls)
	}
	_, _, records := sink.snapshot()
	if len(records) == 0 || !strings.Contains(fmt.Sprint(records[len(records)-1].Fields["detail"]), diagnostic) {
		t.Fatalf("private parser detail was lost: %#v", records)
	}
}
