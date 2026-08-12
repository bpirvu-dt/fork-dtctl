package exec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/term"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/output"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

// VerifyReplayCompatibilityWithContext performs the same original parse,
// transformation, effective parse, and audit as execution, but never submits a
// query and never completes a terminal session. A nil result means no active
// replay session exists and verify query must preserve its ordinary response.
func (e *DQLExecutor) VerifyReplayCompatibilityWithContext(ctx context.Context, query string, opts DQLVerifyOptions, originalValid bool) (*ReplayVerification, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if e.preparer == nil || e.originalASTs == nil {
		return nil, nil
	}
	reader, ok := e.preparer.(replaySessionReader)
	if !ok || !reader.ReplaySessionActive(ctx) {
		return nil, nil
	}

	parseHandler := e.sdkHandler(opts.ClientContext).WithFirstRateLimitResponse()
	prepared, err := e.preparer.Prepare(ctx, PrepareInput{
		OriginalQuery:    query,
		Options:          DQLExecuteOptions{Timezone: opts.Timezone, Locale: opts.Locale, ClientContext: opts.ClientContext},
		Mode:             ReplayExecutionVerify,
		OriginalDQLValid: &originalValid,
		OriginalASTs:     e.originalASTs,
		Parse: func(parseCtx context.Context, request sdkquery.ParseRequest) (*sdkquery.ParseResponse, error) {
			return parseHandler.Parse(parseCtx, request)
		},
	})
	if err != nil {
		var attempt *ReplayAttemptError
		if errors.As(err, &attempt) && attempt.category == replayErrorSink {
			return nil, err
		}
		verification := replayVerificationFromError(ReplayExecutionInfo{
			Active: true, OriginalQuery: query, verificationOriginalValid: &originalValid,
		}, err)
		return &verification, nil
	}

	verification := ReplayVerification{
		Active: true, OriginalDQLValid: originalValid, CompilerSupported: true,
		EffectiveQueryValid: true, UnsupportedConstructs: []string{}, Disclosure: prepared.Disclosure,
	}
	if prepared.sink != nil {
		provenance := prepared.provenance
		provenance.Outcome = "verified"
		info := replayInfoFromPrepared(prepared)
		if err := prepared.sink.Append(ctx, provenanceRecord("query_verification", provenance, map[string]any{
			"verification": replayVerificationFields(verification),
		})); err != nil {
			return nil, newReplayAttemptError(replayErrorSink, err, info, false, 0, false)
		}
	}
	return &verification, nil
}

func replayVerificationFromError(fallback ReplayExecutionInfo, err error) ReplayVerification {
	info := fallback
	detail := err
	var attempt *ReplayAttemptError
	if errors.As(err, &attempt) {
		if attempt.info.Active {
			info = attempt.info
		}
		if attempt.detail != nil {
			detail = attempt.detail
		}
	}

	originalValid := false
	if info.verificationOriginalValid != nil {
		originalValid = *info.verificationOriginalValid
	}
	result := ReplayVerification{
		Active: true, OriginalDQLValid: originalValid,
		UnsupportedConstructs: replayUnsupportedConstructs(detail),
		Disclosure:            info.Disclosure,
	}
	var compilerErr *execreplay.ReplayError
	if errors.As(detail, &compilerErr) && compilerErr.Code == execreplay.ErrorAudit && info.EffectiveQuery != "" {
		result.EffectiveQueryValid = true
	}
	return result
}

func replayUnsupportedConstructs(err error) []string {
	if err == nil {
		return []string{}
	}
	var compilerErr *execreplay.ReplayError
	if errors.As(err, &compilerErr) && compilerErr.Construct != "" {
		return []string{compilerErr.Construct}
	}
	var davisErr *execreplay.DavisCurrentViewError
	if errors.As(err, &davisErr) && davisErr.View != "" {
		return []string{davisErr.View}
	}
	return []string{}
}

func replayVerificationFields(value ReplayVerification) map[string]any {
	return map[string]any{
		"original_dql_valid":     value.OriginalDQLValid,
		"compiler_supported":     value.CompilerSupported,
		"effective_query_valid":  value.EffectiveQueryValid,
		"unsupported_constructs": append([]string(nil), value.UnsupportedConstructs...),
	}
}

// ExecuteQueryWithContext executes a DQL query with a cancellable context.
// If ctx is cancelled while the query is polling, a best-effort cancel request
// is sent to the Grail backend before returning.
func (e *DQLExecutor) ExecuteQueryWithContext(ctx context.Context, query string, opts DQLExecuteOptions) (*DQLQueryResponse, error) {
	result, err := e.ExecuteQueryDetailedWithContext(ctx, query, opts)
	if err != nil || result == nil {
		return nil, err
	}
	return result.Response, nil
}

// ExecuteQueryDetailedWithContext returns replay loop-control facts beside the
// ordinary SDK response without changing its output schema.
func (e *DQLExecutor) ExecuteQueryDetailedWithContext(ctx context.Context, query string, opts DQLExecuteOptions) (*DQLExecutionResult, error) {
	if e.preparer == nil {
		response, err := e.executeQueryRequestWithContext(ctx, query, opts, false)
		if err != nil {
			return nil, err
		}
		return &DQLExecutionResult{Response: response}, nil
	}
	if e.originalASTs == nil {
		return nil, fmt.Errorf("replay executor has no invocation-local original-parse provider")
	}
	mode := opts.ReplayMode
	if mode == "" {
		mode = ReplayExecutionOneShot
	}
	parseHandler := e.sdkHandler(opts.ClientContext).WithFirstRateLimitResponse()
	prepared, err := e.preparer.Prepare(ctx, PrepareInput{
		OriginalQuery: query,
		Options:       opts,
		Mode:          mode,
		OriginalASTs:  e.originalASTs,
		Parse: func(parseCtx context.Context, request sdkquery.ParseRequest) (*sdkquery.ParseResponse, error) {
			return parseHandler.Parse(parseCtx, request)
		},
	})
	if err != nil {
		return nil, err
	}

	e.printReplayNoticeOnce(prepared, opts)
	executeOptions := replayExecuteOptions(opts, prepared)
	response, err := e.executeQueryRequestWithContext(ctx, prepared.EffectiveQuery, executeOptions, true)
	if err != nil {
		return nil, e.afterExecutionError(ctx, prepared, response, replayErrorRemote, err)
	}
	if response == nil {
		return &DQLExecutionResult{Response: nil, Replay: replayInfoPtr(prepared)}, nil
	}

	validated, err := validateReplayResult(prepared, response)
	if err != nil {
		return nil, e.afterExecutionError(ctx, prepared, response, replayErrorValidation, err)
	}
	prepared.provenance.Validated = validated
	prepared.provenance.Outcome = "succeeded"
	prepared.provenance.CanonicalEffectiveDQL = canonicalEffectiveQuery(response)

	info := replayInfoFromPrepared(prepared)
	if prepared.sink != nil {
		if err := prepared.sink.Append(ctx, provenanceRecord("query_execution", prepared.provenance, nil)); err != nil {
			return nil, newReplayAttemptError(replayErrorSink, err, info, false, 0, true)
		}
	}

	if prepared.Terminal {
		finalizer, ok := e.preparer.(queryFinalizer)
		if !ok {
			return nil, e.afterFinalizationError(ctx, prepared, fmt.Errorf("replay query preparer cannot finalize a terminal execution"))
		}
		_, disposition, err := finalizer.Finalize(ctx, prepared)
		if err != nil {
			return nil, e.afterFinalizationError(ctx, prepared, err)
		}
		info.CompletionDisposition = disposition
		if prepared.sink != nil {
			completion := prepared.provenance
			completion.Outcome = "terminal_completion"
			completion.Completion = disposition
			if err := prepared.sink.Append(ctx, provenanceRecord("query_completion", completion, nil)); err != nil {
				return nil, newReplayAttemptError(replayErrorSink, err, info, false, 0, true)
			}
		} else if disposition == session.CompletionSessionReplaced {
			fmt.Fprintln(os.Stderr, "Replay session changed before terminal completion; the query result is valid, but the current session was not changed.")
		}
	}
	return &DQLExecutionResult{Response: response, Replay: &info}, nil
}

// ExplainReplayWithContext performs the complete preparation and validation
// parse but deliberately sends no query:execute request and performs no
// terminal completion write.
func (e *DQLExecutor) ExplainReplayWithContext(ctx context.Context, query string, opts DQLExecuteOptions) (execreplay.ExplainData, error) {
	if e.preparer == nil || e.originalASTs == nil {
		return execreplay.ExplainData{}, fmt.Errorf("--explain-replay requires a configured replay query path")
	}
	parseHandler := e.sdkHandler(opts.ClientContext).WithFirstRateLimitResponse()
	prepared, err := e.preparer.Prepare(ctx, PrepareInput{
		OriginalQuery: query, Options: opts, Mode: ReplayExecutionExplain,
		OriginalASTs: e.originalASTs,
		Parse: func(parseCtx context.Context, request sdkquery.ParseRequest) (*sdkquery.ParseResponse, error) {
			return parseHandler.Parse(parseCtx, request)
		},
	})
	if err != nil {
		return execreplay.ExplainData{}, err
	}
	return prepared.Explain, nil
}

// ValidateReplayCadence rejects unsupported replay request rates before a
// wait/live loop performs its first parse or execute attempt.
func (e *DQLExecutor) ValidateReplayCadence(interval time.Duration) error {
	return e.ValidateReplayCadenceWithContext(context.Background(), interval)
}

// ValidateReplayCadenceWithContext performs disclosure-safe routing for a
// cadence rejection before a wait/live loop emits progress or prepares DQL.
func (e *DQLExecutor) ValidateReplayCadenceWithContext(ctx context.Context, interval time.Duration) error {
	if e.preparer != nil && interval < MinReplayExecutionInterval {
		if validator, ok := e.preparer.(replayCadenceValidator); ok {
			return validator.ValidateCadence(ctx, interval)
		}
		return fmt.Errorf("replay query interval %s is faster than the supported minimum of %s", interval, MinReplayExecutionInterval)
	}
	return nil
}

// WaitForNextAttempt applies Retry-After and terminal scheduling without
// silently changing an otherwise valid requested cadence.
func (e *DQLExecutor) WaitForNextAttempt(ctx context.Context, interval time.Duration, info *ReplayExecutionInfo, attemptErr error) error {
	delay := interval
	if retryAfter := ReplayRetryAfter(attemptErr); retryAfter > delay {
		delay = retryAfter
	}
	if info != nil && info.Active && info.ClockMode == session.ReplayClockRealtime && info.VirtualNow.Before(info.DataEnd) {
		untilEnd := info.DataEnd.Sub(info.VirtualNow)
		if interval > untilEnd && ReplayRetryAfter(attemptErr) <= untilEnd {
			delay = untilEnd
		}
	}
	if waiter, ok := e.preparer.(replayWaiter); ok {
		return waiter.Wait(ctx, delay)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *DQLExecutor) afterExecutionError(ctx context.Context, prepared PreparedQuery, response *DQLQueryResponse, category replayErrorCategory, detail error) error {
	info := replayInfoFromPrepared(prepared)
	provenance := prepared.provenance
	provenance.Outcome = string(category)
	provenance.Detail = detail.Error()
	provenance.CanonicalEffectiveDQL = canonicalEffectiveQuery(response)
	if prepared.sink != nil {
		if err := prepared.sink.Append(ctx, provenanceRecord("query_execution", provenance, nil)); err != nil {
			return newReplayAttemptError(replayErrorSink, err, info, false, 0, true)
		}
	}
	return newReplayAttemptError(category, detail, info, false, sdkRetryAfter(detail), true)
}

func (e *DQLExecutor) afterFinalizationError(ctx context.Context, prepared PreparedQuery, detail error) error {
	info := replayInfoFromPrepared(prepared)
	if prepared.sink != nil {
		provenance := prepared.provenance
		provenance.Outcome = string(replayErrorFinalize)
		provenance.Detail = detail.Error()
		if err := prepared.sink.Append(ctx, provenanceRecord("query_completion", provenance, nil)); err != nil {
			return newReplayAttemptError(replayErrorSink, err, info, false, 0, true)
		}
	}
	return newReplayAttemptError(replayErrorFinalize, detail, info, false, 0, true)
}

func (e *DQLExecutor) printReplayNoticeOnce(prepared PreparedQuery, opts DQLExecuteOptions) {
	if prepared.Disclosure != session.ReplayDisclosureFull || opts.AgentMode {
		return
	}
	e.replayNotice.Do(func() {
		if term.IsTerminal(int(os.Stderr.Fd())) {
			fmt.Fprintf(os.Stderr, "Replay active: virtual now %s; replay interval %s–%s\n",
				replayDisplayTime(prepared.VirtualNow), replayDisplayTime(prepared.Session.DataStart), replayDisplayTime(prepared.Session.DataEnd))
		}
		for _, notice := range prepared.Compilation.Notices {
			output.PrintWarning("%s", notice.Message)
		}
	})
}

func replayExecuteOptions(options DQLExecuteOptions, prepared PreparedQuery) DQLExecuteOptions {
	options.Timezone = prepared.Compilation.Explain.Clock.Timezone
	options.DefaultTimeframeStart = ""
	options.DefaultTimeframeEnd = ""
	if prepared.executeDefaultTimeframe != nil {
		options.DefaultTimeframeStart = replayDisplayTime(prepared.executeDefaultTimeframe.Start)
		options.DefaultTimeframeEnd = replayDisplayTime(prepared.executeDefaultTimeframe.End)
	}
	return options
}

func canonicalEffectiveQuery(response *DQLQueryResponse) string {
	if response == nil || response.GetMetadata() == nil {
		return ""
	}
	return response.GetMetadata().CanonicalQuery
}

func replayDisplayTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func replayInfoPtr(prepared PreparedQuery) *ReplayExecutionInfo {
	value := replayInfoFromPrepared(prepared)
	return &value
}
