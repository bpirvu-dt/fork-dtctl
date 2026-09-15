package exec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/output"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func TestRestrictedRemoteDiagnosticRequiresActualMapping(t *testing.T) {
	for _, token := range append(execreplay.DavisProblemsReconstructionLexemes(), execreplay.DavisProblemsSnapshotTable) {
		t.Run(token, func(t *testing.T) {
			info := restrictedInfo()
			info.OriginalQuery = "fetch logs | filter isNotNull(timestamp) | sort timestamp | fields " + token
			info.EffectiveQuery = info.OriginalQuery + ` | filter timestamp < toTimestamp("2026-06-14T10:00:00Z")`
			detail := &sdkquery.QueryError{StatusCode: http.StatusBadRequest, ErrorType: "REMOTE_ERROR", Message: "Invalid argument: " + token}
			if got := newReplayAttemptError(replayErrorRemote, detail, info, false, 0, true); got.Error() != detail.Error() || !ReplayPreservesRemoteError(got) {
				t.Fatalf("ordinary diagnostic was hidden: %v", got)
			}
			info.Output = &output.ReplayMetadata{Sources: []output.ReplaySourceMetadata{{DavisProblemsMapping: &output.DavisProblemsMappingMetadata{Eligible: true}}}}
			if got := newReplayAttemptError(replayErrorRemote, detail, info, false, 0, true); got.Error() != restrictedQueryInvalidMessage || ReplayPreservesRemoteError(got) {
				t.Fatalf("mapped diagnostic was exposed: %v", got)
			}
		})
	}
	info := restrictedInfo()
	info.OriginalQuery = "fetch " + execreplay.DavisProblemsSnapshotTable
	info.EffectiveQuery = info.OriginalQuery + `, from:toTimestamp("2026-06-14T08:00:00Z")`
	detail := &sdkquery.QueryError{StatusCode: http.StatusForbidden, Message: "Cannot read " + execreplay.DavisProblemsSnapshotTable}
	if !restrictedRemoteTextMayPass(detail, info) {
		t.Fatal("direct snapshot-table diagnostic was hidden")
	}
	for _, message := range []string{"invalid bound 2026-06-14T08:00:00Z", info.EffectiveQuery} {
		detail.Message = message
		if restrictedRemoteTextMayPass(detail, info) {
			t.Fatalf("generated content was exposed: %q", message)
		}
	}
}

func TestRestrictedTypedValidationRequiresApprovedReason(t *testing.T) {
	start := mustReplayTestTime("2026-06-14T10:00:00Z")
	declared := 5 * time.Minute
	contract := execreplay.ReplayResultContract{
		LogicalWindow:  execreplay.Interval{Start: start, End: start.Add(time.Hour)},
		BoundaryPolicy: execreplay.BoundaryMetricBucket, NaturalIntervalRequired: true,
		DeclaredNaturalInterval: &declared,
	}
	_, actual := execreplay.ValidateResultContract(contract, execreplay.ObservedResultMetadata{NaturalInterval: time.Minute})
	var contractErr *execreplay.ResultContractError
	if !errors.As(actual, &contractErr) || contractErr.PublicReason == "" {
		t.Fatalf("real interval mismatch lacks public guidance: %v", actual)
	}
	info := restrictedInfo()
	for _, wrapped := range []error{actual, fmt.Errorf("private wrapper: %w", actual)} {
		want := "The query result failed a consistency check: the observed natural interval 1m0s does not match the declared interval 5m0s."
		if got := restrictedValidationMessage(wrapped, info); got != want {
			t.Fatalf("validation = %q, want %q", got, want)
		}
	}
	for _, reason := range []string{"", "replay state mismatch", "invalid bound 2026-06-14T10:00:00Z", "generated query: fetch logs | limit 3"} {
		candidate := &execreplay.ResultContractError{Reason: "an otherwise safe detailed reason", PublicReason: reason}
		info.EffectiveQuery = "fetch logs | limit 3"
		info.Output = &output.ReplayMetadata{VirtualNow: "2026-06-14T10:00:00Z"}
		if got := restrictedValidationMessage(fmt.Errorf("wrapped: %w", candidate), info); got != restrictedValidationFallbackMessage {
			t.Fatalf("unapproved or protected reason was exposed: %q", got)
		}
	}
	// Internal reasons stay private even when the text itself passes the scanner.
	for _, mutate := range []func(*execreplay.ReplayResultContract, *execreplay.ObservedResultMetadata){
		func(c *execreplay.ReplayResultContract, _ *execreplay.ObservedResultMetadata) {
			c.NaturalIntervalRequired = false
		},
		func(_ *execreplay.ReplayResultContract, o *execreplay.ObservedResultMetadata) { o.Source.Ordinal = 1 },
		func(_ *execreplay.ReplayResultContract, o *execreplay.ObservedResultMetadata) {
			o.Provenance.NaturalInterval = time.Second
		},
	} {
		candidate := contract
		observed := execreplay.ObservedResultMetadata{NaturalInterval: declared}
		mutate(&candidate, &observed)
		_, detail := execreplay.ValidateResultContract(candidate, observed)
		if !errors.As(detail, &contractErr) || contractErr.PublicReason != "" || restrictedValidationMessage(detail, restrictedInfo()) != restrictedValidationFallbackMessage {
			t.Fatalf("internal validation failure was exposed: %v", detail)
		}
	}
}

func TestRestrictedOriginalCalendarParseDiagnostic(t *testing.T) {
	body := replayFixtureBody(t, "phase0b/fixtures/metrics/06b-calendar-month/parse.json")
	var response sdkquery.ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	detail := &sdkquery.QueryError{
		StatusCode: http.StatusBadRequest, Message: response.Error.Message,
		ErrorType: response.Error.Details.ErrorType, Detail: response.Error.Details.ErrorMessage,
		Arguments: response.Error.Details.Arguments,
	}
	if !strings.Contains(detail.Error(), "The parameter `interval` doesn't support calendar durations.") {
		t.Fatalf("fixture did not yield the recorded diagnostic: %v", detail)
	}
	info := restrictedInfo()
	if got := restrictedPreparationMessage(detail, info); got != detail.Error() {
		t.Fatalf("original parser diagnostic hidden: %q", got)
	}
	info.EffectiveQuery = "timeseries metric_value=avg(dt.host.cpu.usage), interval:1M"
	if got := restrictedPreparationMessage(detail, info); got != restrictedQueryInvalidMessage {
		t.Fatalf("rewritten parser diagnostic exposed: %q", got)
	}
	info.EffectiveQuery = ""
	info.Output = &output.ReplayMetadata{VirtualNow: "2026-06-14T10:00:00Z"}
	for _, protected := range []string{"replay", "virtual", "effective", "2026-06-14T10:00:00Z"} {
		copy := *detail
		copy.Detail += " " + protected
		if got := restrictedPreparationMessage(&copy, info); got != restrictedQueryInvalidMessage {
			t.Fatalf("parser diagnostic exposed protected content: %q", got)
		}
	}
}

func TestDQLExecutorRestrictedTypedValidationWithholdsResult(t *testing.T) {
	for _, disclosure := range []string{session.ReplayDisclosureRestricted, session.ReplayDisclosureFull} {
		t.Run(disclosure, func(t *testing.T) {
			api := newReplayMetricMockAPI(t)
			api.executeResponse.Result.Records[0]["timeframe"] = map[string]interface{}{
				"start": "2026-08-07T10:30:00Z", "end": "2026-08-08T11:00:00Z",
			}
			sink := &replayTestSink{}
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure,
				replayMetricDataStart, replayMetricDataEnd, replayMetricDataEnd, func(string) session.ProvenanceSink { return sink })
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), replayMetricOriginal, DQLExecuteOptions{AgentMode: true})
			var contractErr *execreplay.ResultContractError
			if result != nil || !errors.As(err, &contractErr) {
				t.Fatalf("result=%v error=%v, want withheld typed failure", result, err)
			}
			if disclosure == session.ReplayDisclosureFull {
				if err.Error() != contractErr.Error() {
					t.Fatalf("full error changed: %v", err)
				}
				return
			}
			if err.Error() != "The query result failed a consistency check: bucket 0 does not intersect the requested timeframe." {
				t.Fatalf("public result error = %q", err.Error())
			}
			_, _, records := sink.snapshot()
			if len(records) == 0 || fmt.Sprint(records[len(records)-1].Fields["detail"]) != contractErr.Error() {
				t.Fatalf("private validation detail changed: %#v", records)
			}
		})
	}
}

func TestDQLExecutorRestrictedCalendarParseRouting(t *testing.T) {
	for _, rewritten := range []bool{false, true} {
		t.Run(fmt.Sprintf("rewritten=%t", rewritten), func(t *testing.T) {
			api := newReplayMockAPI(t)
			api.parseErrorBody = replayFixtureBody(t, "phase0b/fixtures/metrics/06b-calendar-month/parse.json")
			original := `timeseries metric_value=avg(dt.host.cpu.usage), interval:1M, from:toTimestamp("2026-05-02T08:36:06Z"), to:toTimestamp("2026-08-10T12:46:06Z")`
			if rewritten {
				original = replayRecordOriginal
				api.effectiveStatus = http.StatusBadRequest
			} else {
				api.parseStatus = http.StatusBadRequest
			}
			api.isOriginal = func(query string) bool { return query == original }
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted,
				replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{AgentMode: true})
			var queryErr *sdkquery.QueryError
			if result != nil || !errors.As(err, &queryErr) {
				t.Fatalf("result=%v error=%v, want typed parser rejection", result, err)
			}
			if rewritten {
				if err.Error() != restrictedQueryInvalidMessage {
					t.Fatalf("rewritten-query parser detail was exposed: %v", err)
				}
			} else if !strings.Contains(err.Error(), "The parameter `interval` doesn't support calendar durations.") {
				t.Fatalf("original-query parser detail was hidden: %v", err)
			}
			if _, executes, polls := api.counts(); executes != 0 || polls != 0 {
				t.Fatal("query executed after parser rejection")
			}
		})
	}
}

func TestDQLExecutorRestrictedTimeframeGuidance(t *testing.T) {
	for _, test := range []struct {
		name, fixture, original, timezone, want string
		replaceFrom, replaceTo                  string
		start, now, end                         time.Time
	}{
		{
			name: "unsupported expression", fixture: "phase0/fixtures/02-fetch-implicit-duration/parse.json",
			original:    "fetch logs, from:+1h",
			replaceFrom: `"canonicalString": "-1"`, replaceTo: `"canonicalString": "+1"`,
			want: "The query's timeframe could not be interpreted. Use an absolute start and end timestamp.",
		},
		{
			name: "non UTC alignment", fixture: "phase0/fixtures/03-fetch-aligned-duration/parse.json",
			original: "fetch logs, from:-1d@d", timezone: "Europe/Vienna",
			want: "The query's time alignment is not supported in this timezone. Use an absolute start and end timestamp.",
		},
		{
			name: "reversed requested range", fixture: "phase0b/fixtures/records/logs/01-to-at-t/parse.json",
			original:    strings.ReplaceAll(replayRecordOriginal, "10:45:02.718012207", "11:15:02.718012207"),
			replaceFrom: "10:45:02.718012207", replaceTo: "11:15:02.718012207",
			want: "The query's timeframe could not be interpreted. Use a start that is earlier than the end.",
		},
		{
			name: "one nanosecond intersection", fixture: "phase0b/fixtures/records/logs/01-to-at-t/parse.json",
			original: replayRecordOriginal,
			start:    mustReplayTestTime("2026-08-10T11:05:02.718012206Z"),
			now:      mustReplayTestTime("2026-08-10T11:30:00Z"), end: mustReplayTestTime("2026-08-10T12:00:00Z"),
			want: "No data can be returned for the requested timeframe.",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			api := newReplayMockAPI(t)
			api.originalBody = replayFixtureBody(t, test.fixture)
			if test.replaceFrom != "" {
				api.originalBody = json.RawMessage(strings.ReplaceAll(string(api.originalBody), test.replaceFrom, test.replaceTo))
			}
			api.isOriginal = func(query string) bool { return query == test.original }
			if test.start.IsZero() {
				test.start, test.now, test.end = replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd
			}
			sink := &replayTestSink{}
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted,
				test.start, test.now, test.end, func(string) session.ProvenanceSink { return sink })
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), test.original, DQLExecuteOptions{AgentMode: true, Timezone: test.timezone})
			var compilerErr *execreplay.ReplayError
			var attemptErr *ReplayAttemptError
			if result != nil || !errors.As(err, &compilerErr) || compilerErr.Code != execreplay.ErrorTimeframe || err.Error() != test.want {
				t.Fatalf("result=%v error=%v, want %q", result, err, test.want)
			}
			if !errors.As(err, &attemptErr) || attemptErr.category != replayErrorPrepare || attemptErr.retryable {
				t.Fatalf("preparation category or retry behavior changed: %#v", attemptErr)
			}
			if _, executes, polls := api.counts(); executes != 0 || polls != 0 {
				t.Fatalf("rejected timeframe executed: executes=%d polls=%d", executes, polls)
			}
			_, _, records := sink.snapshot()
			if len(records) == 0 || !strings.Contains(fmt.Sprint(records[len(records)-1].Fields["detail"]), compilerErr.Error()) {
				t.Fatalf("private timeframe cause changed: %#v", records)
			}
		})
	}
}
