package exec

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/output"
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
	ReplayExecutionVerify  ReplayExecutionMode = "verify"
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
	// OriginalDQLValid is set only by verify query. It lets restricted
	// disclosure persist compatibility facts in the same pre-execution record
	// that already captures a compiler rejection.
	OriginalDQLValid *bool
	Parse            QueryParseFunc
	OriginalASTs     OriginalASTProvider
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

type replaySessionReader interface {
	ReplaySessionActive(context.Context) bool
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
	Output                *output.ReplayMetadata

	verificationOriginalValid *bool
}

// ReplayVerification reports execution-free compatibility checks performed
// for verify query while a replay session is active.
type ReplayVerification struct {
	Active                bool     `json:"active" yaml:"active"`
	OriginalDQLValid      bool     `json:"original_dql_valid" yaml:"original_dql_valid"`
	CompilerSupported     bool     `json:"compiler_supported" yaml:"compiler_supported"`
	EffectiveQueryValid   bool     `json:"effective_query_valid" yaml:"effective_query_valid"`
	UnsupportedConstructs []string `json:"unsupported_constructs" yaml:"unsupported_constructs"`
	EffectiveQuery        string   `json:"effective_query,omitempty" yaml:"effective_query,omitempty"`
	CoverageVerified      *bool    `json:"coverage_verified,omitempty" yaml:"coverage_verified,omitempty"`
	CoverageMessage       string   `json:"coverage_message,omitempty" yaml:"coverage_message,omitempty"`

	Disclosure string `json:"-" yaml:"-"`
}

// FullDisclosure reports whether compatibility details may appear in ordinary
// output. Restricted callers still receive the value internally so they can
// prove the checks ran, but must serialize only the normal verification result.
func (v *ReplayVerification) FullDisclosure() bool {
	return v != nil && v.Disclosure == session.ReplayDisclosureFull
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
// one attempt. It excludes returned records and credentials. A mapped
// restricted attempt retains Grail contributions here because their table
// field may identify the private reconstruction source.
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
	Notifications         []QueryNotification
	GrailContributions    *Contributions
	Outcome               string
	Detail                string
	Completion            session.CompletionDisposition
	DavisCoverage         *DavisSnapshotCoverageProvenance
}

// DavisSnapshotCoverageProvenance contains only the bounded probe's typed,
// non-identifying facts. Explain and verify use Status=not_checked with no
// observation timestamp or reuse disposition.
type DavisSnapshotCoverageProvenance struct {
	Status         string
	Verified       bool
	OldestSnapshot time.Time
	ObservedAt     time.Time
	Reuse          DavisSnapshotCoverageReuse
	Failure        string
}

func replayInfoFromPrepared(prepared PreparedQuery) ReplayExecutionInfo {
	return ReplayExecutionInfo{
		Active: true, Disclosure: prepared.Disclosure, SessionID: prepared.Session.SessionID,
		ClockMode: prepared.Session.ClockMode, VirtualNow: prepared.VirtualNow,
		DataStart: prepared.Session.DataStart, DataEnd: prepared.Session.DataEnd,
		Terminal: prepared.Terminal, OriginalQuery: prepared.OriginalQuery,
		EffectiveQuery: prepared.EffectiveQuery, Output: replayOutputMetadata(prepared, nil, "", nil),
	}
}

func restrictedMessage(category replayErrorCategory, detail error, retryable, postExecution bool) string {
	return restrictedMessageForInfo(category, detail, ReplayExecutionInfo{}, retryable, postExecution)
}

func restrictedMessageForInfo(category replayErrorCategory, detail error, info ReplayExecutionInfo, retryable, postExecution bool) string {
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
		if detail != nil && restrictedRemoteTextMayPass(detail, info) {
			return detail.Error()
		}
		return restrictedRemoteExecutionMessage
	default:
		return restrictedPreparationMessage
	}
}

func restrictedRemoteTextMayPass(detail error, info ReplayExecutionInfo) bool {
	generated := detail.Error()
	if replayInfoHasDavisProblemsMapping(info) && containsDavisMappingText(generated, info) {
		return false
	}
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

func replayInfoHasDavisProblemsMapping(info ReplayExecutionInfo) bool {
	if info.Output == nil {
		return false
	}
	for _, source := range info.Output.Sources {
		if source.DavisProblemsMapping != nil {
			return true
		}
	}
	return false
}

func containsDavisMappingText(value string, info ReplayExecutionInfo) bool {
	generated := value
	// A complete verbatim original query is ordinary remote content. Isolated
	// token or timestamp collisions are ambiguous because the reconstruction
	// also generated them, so those remain subject to the fail-closed scan.
	if info.OriginalQuery != "" {
		generated = strings.ReplaceAll(generated, info.OriginalQuery, "")
	}
	generated = strings.ReplaceAll(generated, `\"`, `"`)
	lower := strings.ToLower(generated)
	for _, fragment := range []string{
		strings.ToLower(execreplay.DavisProblemsSnapshotTable),
		"sort timestamp desc",
		"dedup event.id",
		"filter event.start",
		"coalesce(event.end",
	} {
		if strings.Contains(lower, fragment) {
			return true
		}
	}
	for _, lexeme := range []string{"sort", "timestamp", "dedup", "event.id", "filter", "event.start", "coalesce", "event.end"} {
		if containsDQLLexeme(lower, lexeme) && containsDQLLexeme(strings.ToLower(info.EffectiveQuery), lexeme) {
			return true
		}
	}
	remoteTimestamps := rfc3339Instants(generated)
	for instant := range dqlToTimestampInstants(info.EffectiveQuery) {
		if _, disclosed := remoteTimestamps[instant]; disclosed {
			return true
		}
	}
	for _, query := range []string{info.EffectiveQuery, info.Output.GrailCanonicalEffectiveQuery} {
		if query != "" && query != info.OriginalQuery && strings.Contains(value, query) {
			return true
		}
	}
	return false
}

func containsDQLLexeme(value, lexeme string) bool {
	for searchFrom := 0; searchFrom < len(value); {
		index := strings.Index(value[searchFrom:], lexeme)
		if index < 0 {
			return false
		}
		index += searchFrom
		end := index + len(lexeme)
		leftBoundary := index == 0 || !isDQLWordByte(value[index-1])
		rightBoundary := end == len(value) || !isDQLWordByte(value[end])
		if leftBoundary && rightBoundary {
			return true
		}
		searchFrom = index + 1
	}
	return false
}

func isDQLWordByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' ||
		value >= '0' && value <= '9' || value == '_'
}

var dqlToTimestampLiteralPattern = regexp.MustCompile(`(?i)\btotimestamp[[:space:]]*\([[:space:]]*(?:value[[:space:]]*:[[:space:]]*)?"([^"]+)"[[:space:]]*\)`)
var rfc3339Pattern = regexp.MustCompile(`(?i)\b[0-9]{4}-[0-9]{2}-[0-9]{2}t[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:z|[+-][0-9]{2}:[0-9]{2})\b`)

// dqlToTimestampInstants normalizes rendered timestamp calls before comparing
// remote text with generated bounds. Grail errors may change whitespace,
// precision, case, or the UTC offset without changing the disclosed instant.
func dqlToTimestampInstants(query string) map[string]struct{} {
	query = strings.ReplaceAll(query, `\"`, `"`)
	result := make(map[string]struct{})
	for _, match := range dqlToTimestampLiteralPattern.FindAllStringSubmatch(query, -1) {
		value, err := time.Parse(time.RFC3339Nano, match[1])
		if err != nil {
			continue
		}
		result[value.UTC().Format(time.RFC3339Nano)] = struct{}{}
	}
	return result
}

// rfc3339Instants also recognizes a bare rendered bound. Query API errors can
// identify an invalid argument by value without repeating its toTimestamp()
// expression, so wrapper-only matching would disclose the generated instant.
func rfc3339Instants(value string) map[string]struct{} {
	result := make(map[string]struct{})
	for _, match := range rfc3339Pattern.FindAllString(value, -1) {
		parsed, err := time.Parse(time.RFC3339Nano, strings.ToUpper(match))
		if err != nil {
			continue
		}
		result[parsed.UTC().Format(time.RFC3339Nano)] = struct{}{}
	}
	return result
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
		public = restrictedMessageForInfo(category, detail, info, retryable, postExecution)
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
