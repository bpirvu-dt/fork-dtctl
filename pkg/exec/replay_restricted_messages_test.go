package exec

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/httpclient"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func restrictedInfo() ReplayExecutionInfo {
	return ReplayExecutionInfo{Active: true, Disclosure: session.ReplayDisclosureRestricted}
}

func TestPreparationHintFromReplayError(t *testing.T) {
	for _, test := range []struct {
		name string
		err  *execreplay.ReplayError
		want string
	}{
		{
			name: "unsupported form names the element and remedy",
			err:  &execreplay.ReplayError{Code: execreplay.ErrorUnsupportedForm, Construct: "timeseries", Remedy: "Use the exact tested metric form."},
			want: "The query uses an unsupported element: timeseries. Use the exact tested metric form.",
		},
		{
			name: "unsupported source names the element and remedy",
			err:  &execreplay.ReplayError{Code: execreplay.ErrorUnsupportedSource, Construct: "custom.table", Remedy: "Use one of the seven approved historical record tables."},
			want: "The query uses an unsupported element: custom.table. Use one of the seven approved historical record tables.",
		},
		{
			name: "unsupported form without remedy omits trailing space",
			err:  &execreplay.ReplayError{Code: execreplay.ErrorUnsupportedForm, Construct: "join"},
			want: "The query uses an unsupported element: join.",
		},
		{
			name: "timeframe uses a fixed base plus remedy",
			err:  &execreplay.ReplayError{Code: execreplay.ErrorTimeframe, Construct: "timeframe", Remedy: "Use an absolute RFC 3339 timeframe."},
			want: "The query's timeframe could not be interpreted. Use an absolute RFC 3339 timeframe.",
		},
		{
			// The real compiler remedy names "replay"; the authored shift hint
			// must ignore it and stay actionable rather than dropping to generic.
			name: "shift authors a safe remedy and ignores the replay-worded compiler remedy",
			err:  &execreplay.ReplayError{Code: execreplay.ErrorShift, Construct: "shift", Remedy: "Remove shift; replay currently supports only unshifted timeseries."},
			want: "The query uses an unsupported time shift. Remove the time shift.",
		},
		{name: "audit yields no hint", err: &execreplay.ReplayError{Code: execreplay.ErrorAudit}, want: ""},
		{name: "ast contract yields no hint", err: &execreplay.ReplayError{Code: execreplay.ErrorASTContract}, want: ""},
		{name: "result contract yields no hint", err: &execreplay.ReplayError{Code: execreplay.ErrorResultContract}, want: ""},
		{name: "current state yields no hint", err: &execreplay.ReplayError{Code: execreplay.ErrorCurrentState}, want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := preparationHintFromReplayError(test.err); got != test.want {
				t.Fatalf("hint = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRestrictedPreparationMessage(t *testing.T) {
	info := restrictedInfo()
	t.Run("cadence surfaces the floor and requested rate", func(t *testing.T) {
		detail := &ReplayCadenceError{Requested: 4 * time.Second, Minimum: 5 * time.Second}
		got := restrictedPreparationMessage(detail, info)
		want := "The query is being run too frequently; the minimum time between runs is 5s (requested 4s)."
		if got != want {
			t.Fatalf("cadence message = %q, want %q", got, want)
		}
	})

	t.Run("user-fault code surfaces a scanned hint", func(t *testing.T) {
		detail := &execreplay.ReplayError{Code: execreplay.ErrorUnsupportedForm, Construct: "timeseries", Remedy: "Use the exact tested metric form."}
		got := restrictedPreparationMessage(detail, info)
		if got != "The query uses an unsupported element: timeseries. Use the exact tested metric form." {
			t.Fatalf("hint message = %q", got)
		}
	})

	t.Run("internal timeframe mismatch with a disclosing remedy stays generic", func(t *testing.T) {
		detail := &execreplay.ReplayError{Code: execreplay.ErrorTimeframe, Construct: "virtual now", Remedy: "Use the current session clock value at or after virtual_start."}
		if got := restrictedPreparationMessage(detail, info); got != restrictedQueryInvalidMessage {
			t.Fatalf("disclosing remedy leaked: %q", got)
		}
	})

	t.Run("internal-mismatch code stays generic", func(t *testing.T) {
		detail := &execreplay.ReplayError{Code: execreplay.ErrorAudit, Message: "audit failed"}
		if got := restrictedPreparationMessage(detail, info); got != restrictedQueryInvalidMessage {
			t.Fatalf("audit message = %q", got)
		}
	})

	t.Run("user's own parse error passes through before any rewrite", func(t *testing.T) {
		detail := &sdkquery.QueryError{StatusCode: http.StatusBadRequest, ErrorType: "SYNTAX_ERROR", Message: "unexpected token 'form' at position 12"}
		got := restrictedPreparationMessage(detail, info)
		if got != detail.Error() {
			t.Fatalf("user parse error was not passed through: %q", got)
		}
	})

	t.Run("effective-query parse error is masked", func(t *testing.T) {
		rewritten := info
		rewritten.EffectiveQuery = `fetch logs, from:toTimestamp("2026-06-14T03:00:00.000000000Z")`
		detail := &sdkquery.QueryError{StatusCode: http.StatusServiceUnavailable, Message: "query request failed"}
		if got := restrictedPreparationMessage(detail, rewritten); got != restrictedQueryInvalidMessage {
			t.Fatalf("effective-query parse error was not masked: %q", got)
		}
	})

	t.Run("untyped preparation error stays generic", func(t *testing.T) {
		if got := restrictedPreparationMessage(errors.New("some internal failure"), info); got != restrictedQueryInvalidMessage {
			t.Fatalf("untyped error = %q", got)
		}
	})
}

func TestRestrictedValidationMessage(t *testing.T) {
	info := restrictedInfo()
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "metric interval reason is surfaced",
			err:  errors.New("metric result record 0 has an invalid timeframe or natural interval"),
			want: "The query result failed a consistency check: metric result record 0 has an invalid timeframe or natural interval.",
		},
		{
			name: "natural bucket reason is surfaced",
			err:  errors.New("metric result exposes no auditable natural buckets"),
			want: "The query result failed a consistency check: metric result exposes no auditable natural buckets.",
		},
		{
			name: "reason with a guard word falls back",
			err:  errors.New("the replay session result mismatched"),
			want: restrictedValidationFallbackMessage,
		},
		{name: "nil detail falls back", err: nil, want: restrictedValidationFallbackMessage},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := restrictedValidationMessage(test.err, info); got != test.want {
				t.Fatalf("validation message = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRestrictedSinkMessageSplitsConfigFromTransient(t *testing.T) {
	info := restrictedInfo()
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{"missing provenance path is a config fault", ErrReplayProvenancePathMissing, restrictedSinkConfigMessage},
		{"nil sink factory is a config fault", ErrReplayProvenanceSinkUnavailable, restrictedSinkConfigMessage},
		{"transient sink I/O is generic", errors.New("disk write failed"), restrictedGenericExecutionMessage},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := restrictedMessageForInfo(replayErrorSink, test.err, info, false); got != test.want {
				t.Fatalf("sink message = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRestrictedRemoteFallbackSplitsOnStatus(t *testing.T) {
	info := restrictedInfo()
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "masked permanent 4xx is query-invalid",
			err:  fmt.Errorf("effective query rejected: %w", &sdkquery.QueryError{StatusCode: http.StatusBadRequest, ErrorType: "REMOTE_ERROR", Message: "bad request"}),
			want: restrictedQueryInvalidMessage,
		},
		{
			name: "masked transient 5xx is generic",
			err:  fmt.Errorf("effective request wrapper: %w", httpclient.NewAPIError(http.StatusBadGateway, "Bad Gateway", "boom")),
			want: restrictedGenericExecutionMessage,
		},
		{
			name: "masked status-less failure is generic",
			err:  errors.New("effective request failed"),
			want: restrictedGenericExecutionMessage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := restrictedMessageForInfo(replayErrorRemote, test.err, info, false); got != test.want {
				t.Fatalf("remote fallback = %q, want %q", got, test.want)
			}
		})
	}

	t.Run("replay-free remote error passes through", func(t *testing.T) {
		detail := &sdkquery.QueryError{StatusCode: http.StatusBadRequest, ErrorType: "REMOTE_ERROR", Message: "table does not exist"}
		if got := restrictedMessageForInfo(replayErrorRemote, detail, info, false); got != detail.Error() {
			t.Fatalf("replay-free remote error was not passed through: %q", got)
		}
	})

	// A non-Davis remote error that echoes a generated virtual timestamp must be
	// masked even without any Davis mapping in the info.
	t.Run("non-mapped remote error echoing a virtual timestamp is masked", func(t *testing.T) {
		virtualInfo := restrictedInfo()
		virtualInfo.EffectiveQuery = `fetch logs, from:toTimestamp("2026-08-10T10:50:02.718012207Z")`
		detail := &sdkquery.QueryError{
			StatusCode: http.StatusBadRequest, ErrorType: "REMOTE_ERROR",
			Message: `invalid bound 2026-08-10T10:50:02.718012207Z`,
		}
		got := restrictedMessageForInfo(replayErrorRemote, detail, virtualInfo, false)
		if got == detail.Error() || strings.Contains(got, "2026-08-10T10:50:02") {
			t.Fatalf("virtual timestamp leaked through a non-mapped remote error: %q", got)
		}
		if got != restrictedQueryInvalidMessage {
			t.Fatalf("masked non-mapped 4xx = %q, want query-invalid", got)
		}
	})
}

// TestRestrictedMessagesNeverLeakReplayInternals is the leak-guard: authored
// constants carry no guard word, and a corpus of replay-worded details routed
// through the authored categories never surfaces a hard tell, the reconstruction
// table, a generated timestamp, or the raw detail verbatim.
func TestRestrictedMessagesNeverLeakReplayInternals(t *testing.T) {
	for _, constant := range []string{
		restrictedNoDataMessage, restrictedTemporaryNoDataMessage, restrictedReadinessMessage,
		restrictedQueryInvalidMessage, restrictedValidationFallbackMessage,
		restrictedGenericExecutionMessage, restrictedSinkConfigMessage,
	} {
		if containsRestrictedGeneratedWord(constant, false) {
			t.Errorf("authored constant leaks a guard word: %q", constant)
		}
	}

	info := restrictedInfo()
	info.EffectiveQuery = `fetch logs, from:toTimestamp("2026-06-14T03:00:00.000000000Z")`
	leakyDetails := []error{
		errors.New("replay session clock reached virtual now"),
		errors.New("the effective query is unsupported"),
		&execreplay.ReplayError{Code: execreplay.ErrorAudit, Construct: "validation AST", Message: "The effective-query audit failed.", Remedy: "report the replay compiler mismatch."},
		&execreplay.ReplayError{Code: execreplay.ErrorTimeframe, Construct: "virtual now", Message: "Virtual now is outside the replay interval.", Remedy: "Use the current session clock value at or after virtual_start."},
		&sdkquery.QueryError{StatusCode: http.StatusBadRequest, ErrorType: "REMOTE_ERROR", Message: `source ` + execreplay.DavisProblemsSnapshotTable + ` at toTimestamp("2026-06-14T03:00:00.000000000Z")`},
		errors.New(`generated bound toTimestamp("2026-06-14T03:00:00.000000000Z")`),
	}
	// Categories whose restricted output is always authored (never a deliberate
	// verbatim passthrough of real remote API content).
	categories := []replayErrorCategory{
		replayErrorReadiness, replayErrorNonOverlap, replayErrorPrepare,
		replayErrorValidation, replayErrorSink, replayErrorFinalize,
	}
	for _, detail := range leakyDetails {
		for _, category := range categories {
			got := restrictedMessageForInfo(category, detail, info, false)
			if containsRestrictedGeneratedWord(got, false) {
				t.Errorf("category %s leaked a guard word for %q: %q", category, detail, got)
			}
			if strings.Contains(got, execreplay.DavisProblemsSnapshotTable) || strings.Contains(got, "toTimestamp") {
				t.Errorf("category %s leaked a reconstruction token for %q: %q", category, detail, got)
			}
			if got == detail.Error() {
				t.Errorf("category %s surfaced the raw replay-worded detail verbatim: %q", category, got)
			}
		}
	}
}
