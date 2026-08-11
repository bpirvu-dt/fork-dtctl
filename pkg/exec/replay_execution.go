package exec

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/httpclient"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

// ReplayExecutionMode tells preparation whether a proven temporary
// non-overlap may be retried by a realtime loop.
type ReplayExecutionMode string

const (
	ReplayExecutionOneShot ReplayExecutionMode = "one-shot"
	ReplayExecutionWait    ReplayExecutionMode = "wait"
	ReplayExecutionLive    ReplayExecutionMode = "live"
	ReplayExecutionExplain ReplayExecutionMode = "explain"
)

// MinReplayExecutionInterval is the evidence-backed floor for distinct parse
// and execute attempts in a replay wait/live loop.
const MinReplayExecutionInterval = 5 * time.Second

// PrepareInput contains the already-expanded query and the per-execution API
// semantics needed by an optional QueryPreparer.
type PrepareInput struct {
	OriginalQuery string
	Options       DQLExecuteOptions
	Mode          ReplayExecutionMode
	Parse         QueryParseFunc
	OriginalASTs  OriginalASTProvider
}

// PreparedQuery is the audited text and typed replay contract for exactly one
// execution. I/O-only fields remain private to pkg/exec.
type PreparedQuery struct {
	OriginalQuery  string
	EffectiveQuery string
	Compilation    execreplay.CompileResult
	Audit          execreplay.AuditResult
	Session        session.ReplaySession
	VirtualNow     time.Time
	HostNow        time.Time
	Terminal       bool
	Disclosure     string
	Explain        execreplay.ExplainData

	executeDefaultTimeframe *execreplay.Interval
	sink                    session.ProvenanceSink
	provenance              ReplayExecutionProvenance
}

// QueryPreparer is optional on DQLExecutor. Its absence preserves the normal
// query path and performs no query:parse call.
type QueryPreparer interface {
	Prepare(context.Context, PrepareInput) (PreparedQuery, error)
}

type queryFinalizer interface {
	Finalize(context.Context, PreparedQuery) (session.ReplaySession, session.CompletionDisposition, error)
}

type replayWaiter interface {
	Wait(context.Context, time.Duration) error
}

type replayCadenceValidator interface {
	ValidateCadence(context.Context, time.Duration) error
}

type replayDisclosureReader interface {
	Disclosure(context.Context) (string, bool)
}

// ReplayExecutionInfo is safe loop-control metadata returned beside a query
// response. It contains no token or returned telemetry.
type ReplayExecutionInfo struct {
	Active                bool
	Disclosure            string
	SessionID             string
	ClockMode             string
	VirtualNow            time.Time
	DataStart             time.Time
	DataEnd               time.Time
	Terminal              bool
	CompletionDisposition session.CompletionDisposition
	OriginalQuery         string
	EffectiveQuery        string
}

// DQLExecutionResult keeps replay loop control out of the SDK response type.
type DQLExecutionResult struct {
	Response *DQLQueryResponse
	Replay   *ReplayExecutionInfo
}

type replayErrorCategory string

const (
	replayErrorReadiness  replayErrorCategory = "readiness"
	replayErrorPrepare    replayErrorCategory = "preparation"
	replayErrorNonOverlap replayErrorCategory = "non_overlap"
	replayErrorValidation replayErrorCategory = "result_validation"
	replayErrorFinalize   replayErrorCategory = "finalization"
	replayErrorRemote     replayErrorCategory = "remote_execution"
	replayErrorSink       replayErrorCategory = "provenance_sink"
)

const (
	restrictedNoDataMessage            = "No data is available for the requested timeframe. The query was not executed."
	restrictedTemporaryNoDataMessage   = "no data yet for the requested timeframe; retrying"
	restrictedReadinessMessage         = "this context is not ready for queries"
	restrictedPreparationMessage       = "The query could not be prepared. It was not executed."
	restrictedValidationMessage        = "The returned data could not be validated. No result was returned."
	restrictedFinalizationMessage      = "The result could not be finalized. No result was returned."
	restrictedPreflightSinkMessage     = "Required local recording is unavailable. The query was not executed."
	restrictedPostExecutionSinkMessage = "Required local recording failed. No result was returned."
	restrictedRemoteExecutionMessage   = "The query failed. No result was returned."
	fullTemporaryNonOverlapMessage     = "no visible overlap yet"
)

// ReplayAttemptError retains the detailed cause for policy and provenance
// while Error exposes only the disclosure-selected ordinary text.
type ReplayAttemptError struct {
	category   replayErrorCategory
	public     string
	detail     error
	info       ReplayExecutionInfo
	retryable  bool
	retryAfter time.Duration
}

func (e *ReplayAttemptError) Error() string { return e.public }
func (e *ReplayAttemptError) Unwrap() error { return e.detail }

// ReplayTemporaryNonOverlap reports whether a loop may skip this attempt and
// recompute it after its normal realtime cadence.
func ReplayTemporaryNonOverlap(err error) bool {
	var replayErr *ReplayAttemptError
	return errors.As(err, &replayErr) && replayErr.category == replayErrorNonOverlap && replayErr.retryable
}

// ReplayLoopHardFailure reports replay-layer failures that cannot become safe
// merely by rerunning the same loop attempt. Remote failures keep the normal
// wait/live transient classification based on their wrapped HTTP error.
func ReplayLoopHardFailure(err error) bool {
	var replayErr *ReplayAttemptError
	if !errors.As(err, &replayErr) {
		return false
	}
	if replayErr.retryable || replayErr.retryAfter > 0 {
		return false
	}
	if _, rateLimited := sdkquery.RetryAfter(replayErr.detail); rateLimited {
		return false
	}
	if replayErr.category == replayErrorRemote {
		return replayRemoteFailurePermanent(replayErr.detail)
	}
	return true
}

func replayRemoteFailurePermanent(err error) bool {
	statusCode := 0
	var queryErr *sdkquery.QueryError
	if errors.As(err, &queryErr) {
		statusCode = queryErr.StatusCode
	}
	var apiErr *httpclient.APIError
	if statusCode == 0 && errors.As(err, &apiErr) {
		statusCode = apiErr.StatusCode
	}
	if statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests {
		return false
	}
	return statusCode >= 400 && statusCode < 500
}

// ReplayRetryAfter returns a Retry-After delay preserved from the first 429.
func ReplayRetryAfter(err error) time.Duration {
	var replayErr *ReplayAttemptError
	if errors.As(err, &replayErr) && replayErr.retryAfter > 0 {
		return replayErr.retryAfter
	}
	if retryAfter, ok := sdkquery.RetryAfter(err); ok {
		return retryAfter
	}
	return 0
}

// ReplayErrorInfo returns loop scheduling facts retained on a replay error.
func ReplayErrorInfo(err error) (ReplayExecutionInfo, bool) {
	var replayErr *ReplayAttemptError
	if !errors.As(err, &replayErr) || !replayErr.info.Active {
		return ReplayExecutionInfo{}, false
	}
	return replayErr.info, true
}

// ReplayPreservesRemoteError reports that a replay attempt intentionally uses
// the ordinary non-replay remote error unchanged. CLI serializers use this to
// retain the normal status code, stable error code, and suggestions.
func ReplayPreservesRemoteError(err error) bool {
	var replayErr *ReplayAttemptError
	return errors.As(err, &replayErr) && replayErr.category == replayErrorRemote &&
		replayErr.detail != nil && replayErr.public == replayErr.detail.Error()
}

// ReplayExecutionProvenance is the complete in-memory record accumulated for
// one attempt. It deliberately excludes returned telemetry and credentials.
type ReplayExecutionProvenance struct {
	Session               session.ReplaySession
	HostNow               time.Time
	VirtualNow            time.Time
	OriginalDQL           string
	EffectiveDQL          string
	CanonicalEffectiveDQL string
	Compilation           *execreplay.CompileResult
	Audit                 *execreplay.AuditResult
	Validated             []execreplay.ValidatedResultContract
	Outcome               string
	Detail                string
	Completion            session.CompletionDisposition
}

func replayInfoFromPrepared(prepared PreparedQuery) ReplayExecutionInfo {
	return ReplayExecutionInfo{
		Active: true, Disclosure: prepared.Disclosure, SessionID: prepared.Session.SessionID,
		ClockMode: prepared.Session.ClockMode, VirtualNow: prepared.VirtualNow,
		DataStart: prepared.Session.DataStart, DataEnd: prepared.Session.DataEnd,
		Terminal: prepared.Terminal, OriginalQuery: prepared.OriginalQuery,
		EffectiveQuery: prepared.EffectiveQuery,
	}
}

func restrictedMessage(category replayErrorCategory, detail error, retryable, postExecution bool) string {
	switch category {
	case replayErrorNonOverlap:
		if retryable {
			return restrictedTemporaryNoDataMessage
		}
		return restrictedNoDataMessage
	case replayErrorReadiness:
		return restrictedReadinessMessage
	case replayErrorValidation:
		return restrictedValidationMessage
	case replayErrorFinalize:
		return restrictedFinalizationMessage
	case replayErrorSink:
		if postExecution {
			return restrictedPostExecutionSinkMessage
		}
		return restrictedPreflightSinkMessage
	case replayErrorRemote:
		if detail != nil && restrictedRemoteTextMayPass(detail) {
			return detail.Error()
		}
		return restrictedRemoteExecutionMessage
	default:
		return restrictedPreparationMessage
	}
}

func restrictedRemoteTextMayPass(detail error) bool {
	generated := detail.Error()
	// QueryError fields are verbatim remote API content wrapped in the normal
	// non-replay formatter. Remove that exact normal-format portion before
	// checking any outer dtctl-generated wrapper text.
	var queryErr *sdkquery.QueryError
	if errors.As(detail, &queryErr) {
		generated = strings.Replace(generated, queryErr.Error(), "", 1)
		return !containsRestrictedGeneratedWord(generated)
	}
	// Poll errors use APIError. Its formatter contributes only the fixed
	// normal error shape; remove it as a unit for the same reason. Any outer
	// wrapper remains subject to the generated-word check.
	var apiErr *httpclient.APIError
	if errors.As(detail, &apiErr) {
		generated = strings.Replace(generated, apiErr.Error(), "", 1)
		return !containsRestrictedGeneratedWord(generated)
	}
	return !containsRestrictedGeneratedWord(generated)
}

func containsRestrictedGeneratedWord(value string) bool {
	value = strings.ToLower(value)
	for _, word := range []string{"replay", "virtual", "session", "clock", "interval", "effective"} {
		if strings.Contains(value, word) {
			return true
		}
	}
	return false
}

func newReplayAttemptError(category replayErrorCategory, detail error, info ReplayExecutionInfo, retryable bool, retryAfter time.Duration, postExecution bool) *ReplayAttemptError {
	public := ""
	switch {
	case info.Disclosure == session.ReplayDisclosureRestricted:
		public = restrictedMessage(category, detail, retryable, postExecution)
	case category == replayErrorNonOverlap && retryable:
		public = fullTemporaryNonOverlapMessage
	case detail != nil:
		public = detail.Error()
	}
	if public == "" {
		public = fmt.Sprintf("query %s failed", category)
	}
	return &ReplayAttemptError{category: category, public: public, detail: detail, info: info, retryable: retryable, retryAfter: retryAfter}
}
