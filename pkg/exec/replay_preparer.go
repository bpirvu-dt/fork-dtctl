package exec

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

// ReplayPreparerConfig contains invocation-stable replay identities and the
// I/O edges used by one top-level DQL executor.
type ReplayPreparerConfig struct {
	Store                    session.ReplayStore
	Clock                    session.Clock
	Locator                  session.ReplayLocator
	ContextName              string
	ExpectedContextInputHash string
	ExpectedEnvironmentHash  string
	EnvironmentID            string
	ClientIdentity           string
	FallbackDisclosure       string
	FallbackProvenancePath   string
	SourcePolicy             execreplay.SourcePolicy
	RetentionInspector       RetentionInspector
	DavisCoverage            DavisSnapshotCoverageProvider
	SinkFactory              func(string) session.ProvenanceSink
	WaitFunc                 func(context.Context, time.Duration) error
}

// ReplayQueryPreparer owns replay state orchestration for one command
// invocation. It contains no process-global memo or mutable replay plan.
type ReplayQueryPreparer struct {
	config              ReplayPreparerConfig
	retentionOnce       sync.Once
	retentionInspection RetentionInspection
	retentionErr        error
}

// NewReplayQueryPreparer validates invocation-stable dependencies. Dynamic
// readiness and context drift are deliberately checked afresh per execution.
func NewReplayQueryPreparer(config ReplayPreparerConfig) (*ReplayQueryPreparer, error) {
	if config.Store == nil || config.Locator.ContextKey == "" || config.Locator.ContextIdentityHash == "" {
		return nil, fmt.Errorf("replay query preparer requires a state store and complete context locator")
	}
	if config.Clock == nil {
		config.Clock = session.SystemClock{}
	}
	if config.ContextName == "" || config.EnvironmentID == "" || config.ClientIdentity == "" || config.ExpectedContextInputHash == "" || config.ExpectedEnvironmentHash == "" {
		return nil, fmt.Errorf("replay query preparer requires complete non-secret context identities")
	}
	if config.FallbackDisclosure == "" {
		config.FallbackDisclosure = session.ReplayDisclosureFull
	}
	if config.SourcePolicy.RecordTables == nil {
		config.SourcePolicy = execreplay.Milestone1SourcePolicy()
	}
	if config.SinkFactory == nil {
		config.SinkFactory = func(path string) session.ProvenanceSink {
			return session.NewReplayProvenanceSink(path)
		}
	}
	return &ReplayQueryPreparer{config: config}, nil
}

// Disclosure resolves only the authoritative route. It performs no parse,
// execution, state write, or provenance I/O.
func (p *ReplayQueryPreparer) Disclosure(_ context.Context) (string, bool) {
	state, err := p.config.Store.Status(p.config.Locator)
	if err == nil {
		return state.Disclosure, true
	}
	return p.config.FallbackDisclosure, true
}

// ReplaySessionActive performs the lock-free state read used by verify query
// to decide whether compatibility checks apply. State-read failures are not
// exposed here; a command that executes data still reaches Prepare and fails
// closed through the normal disclosure router.
func (p *ReplayQueryPreparer) ReplaySessionActive(_ context.Context) bool {
	state, err := p.config.Store.Status(p.config.Locator)
	if err != nil {
		return false
	}
	return state.Status == session.ReplayStatusActive || state.Status == session.ReplayStatusTerminalReady
}

// Prepare performs the fail-closed sequence from plan section 11.1. The only
// reusable artifact is the immutable original parse supplied by input.
func (p *ReplayQueryPreparer) Prepare(ctx context.Context, input PrepareInput) (PreparedQuery, error) {
	if input.Parse == nil || input.OriginalASTs == nil {
		return PreparedQuery{}, fmt.Errorf("replay preparation requires parse and original-AST providers")
	}
	if input.OriginalQuery == "" {
		return PreparedQuery{}, fmt.Errorf("replay preparation requires expanded DQL text")
	}

	state, stateErr := p.config.Store.Status(p.config.Locator)
	disclosure, provenancePath := p.authoritativeRoute(state, stateErr)
	info := ReplayExecutionInfo{Active: true, Disclosure: disclosure, OriginalQuery: input.OriginalQuery}
	info.verificationOriginalValid = input.OriginalDQLValid
	if stateErr == nil {
		info = replayInfoFromSession(state, input.OriginalQuery, disclosure)
		info.verificationOriginalValid = input.OriginalDQLValid
	}

	sink, err := p.preflightSink(ctx, disclosure, provenancePath, info)
	if err != nil {
		return PreparedQuery{}, err
	}

	if stateErr != nil {
		detail := stateErr
		if errors.Is(stateErr, session.ErrReplaySessionNotFound) {
			detail = fmt.Errorf("context %q is configured for replay, but no replay session is active;\nrun 'dtctl replay start --context %s'", p.config.ContextName, p.config.ContextName)
		}
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorReadiness, detail, info, nil, false)
	}
	candidateProvenance := ReplayExecutionProvenance{Session: state, OriginalDQL: input.OriginalQuery}
	if state.Status != session.ReplayStatusActive && state.Status != session.ReplayStatusTerminalReady {
		detail := fmt.Errorf("replay session for context %q is %s", state.ContextName, state.Status)
		if state.Status == session.ReplayStatusCompleted {
			detail = fmt.Errorf("replay session for context %q completed at data_end; restart it before running more DQL", state.ContextName)
		}
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorReadiness, detail, info, &candidateProvenance, false)
	}
	if state.ContextInputHash != p.config.ExpectedContextInputHash || state.EnvironmentHash != p.config.ExpectedEnvironmentHash {
		detail := fmt.Errorf("the replay context changed after this session started; restart the replay session to use the new configuration")
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorReadiness, detail, info, &candidateProvenance, false)
	}

	timezoneName, timezone, err := replayTimezone(input.Options.Timezone)
	if err != nil {
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &candidateProvenance, false)
	}
	parseRequest := sdkquery.ParseRequest{
		Query:        input.OriginalQuery,
		Locale:       input.Options.Locale,
		Timezone:     timezoneName,
		QueryOptions: cloneQueryOptions(input.Options.ParserOptions),
	}
	key, err := NewOriginalParseKey(
		input.OriginalQuery, p.config.EnvironmentID, p.config.ClientIdentity,
		parseRequest.Locale, parseRequest.Timezone, ReplayQueryAPIVersion, parseRequest.QueryOptions,
	)
	if err != nil {
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &candidateProvenance, false)
	}
	view, err := input.OriginalASTs.OriginalAST(ctx, key, parseRequest, input.Parse)
	if err != nil {
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &candidateProvenance, false)
	}
	serverAST, err := view.AST()
	if err != nil {
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &candidateProvenance, false)
	}
	adapted, err := execreplay.Adapt(serverAST)
	if err != nil {
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &candidateProvenance, false)
	}
	// Classification and any bounded coverage inspection deliberately precede
	// the single virtual-clock capture.
	mappingPolicy := replayDavisMappingPolicy(disclosure, input.Mode)
	descriptors, err := execreplay.ClassifySourcesWithMapping(adapted, p.config.SourcePolicy, mappingPolicy)
	if err != nil {
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &candidateProvenance, false)
	}
	candidate := firstDavisProblemsCandidate(descriptors)
	var coverageProvenance *DavisSnapshotCoverageProvenance
	if candidate != nil {
		switch mappingPolicy.Mode {
		case execreplay.DavisProblemsMappingInspection:
			coverageProvenance = &DavisSnapshotCoverageProvenance{Status: "not_checked", Verified: false}
		case execreplay.DavisProblemsMappingExecution:
			key := DavisSnapshotCoverageKey{
				SessionID: state.SessionID, EnvironmentIdentity: p.config.EnvironmentID,
				PrincipalIdentity: p.config.ClientIdentity, Table: execreplay.DavisProblemsSnapshotTable,
				ProbeUpperBound: state.DataEnd.UTC(),
			}
			coverage, reuse, coverageErr := p.inspectDavisCoverage(ctx, key)
			coverageProvenance = davisCoverageProvenance(coverage, reuse, coverageErr)
			candidateProvenance.DavisCoverage = cloneDavisCoverageProvenance(coverageProvenance)
			if coverageErr != nil {
				detail := execreplay.DavisProblemsCoverageError(candidate, execreplay.DavisCoverageInspectionFailed)
				return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, detail, info, &candidateProvenance, false)
			}
			mappingPolicy.Coverage = &coverage
		}
		candidateProvenance.DavisCoverage = cloneDavisCoverageProvenance(coverageProvenance)
	}

	hostNow := p.config.Clock.Now().UTC()
	virtualNow := session.VirtualNow(state, hostNow).UTC()
	candidateProvenance.HostNow = hostNow
	candidateProvenance.VirtualNow = virtualNow
	visibleEnd := session.VisibleEnd(state, hostNow).UTC()
	terminal := virtualNow.Equal(state.DataEnd)
	info.VirtualNow = virtualNow
	info.Terminal = terminal

	globalDefault, err := replayGlobalDefault(input.Options)
	if err != nil {
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &candidateProvenance, false)
	}
	compilation, err := execreplay.Compile(execreplay.CompileInput{
		OriginalDQL:  input.OriginalQuery,
		AST:          adapted,
		VirtualNow:   virtualNow,
		VirtualStart: state.VirtualStart,
		ReplayInterval: execreplay.Interval{
			Start: state.DataStart.UTC(), End: state.DataEnd.UTC(),
		},
		VisibleInterval: execreplay.Interval{
			Start: state.DataStart.UTC(), End: visibleEnd,
		},
		Locale:        input.Options.Locale,
		TimezoneName:  timezoneName,
		Timezone:      timezone,
		GlobalDefault: globalDefault,
		SourcePolicy:  p.config.SourcePolicy,
		DavisMapping:  mappingPolicy,
	})
	if err != nil {
		var nonOverlap *execreplay.NonOverlapError
		if errors.As(err, &nonOverlap) {
			retryable := nonOverlap.Classification == execreplay.OverlapTemporary &&
				state.ClockMode == session.ReplayClockRealtime && !terminal &&
				(input.Mode == ReplayExecutionWait || input.Mode == ReplayExecutionLive)
			prov := p.baseProvenance(state, input.OriginalQuery, hostNow, virtualNow, coverageProvenance)
			prov.Compilation = &compilation
			return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorNonOverlap, err, info, &prov, retryable)
		}
		if coverageProvenance != nil {
			coverageProvenance.Verified = false
			var davisErr *execreplay.DavisCurrentViewError
			if errors.As(err, &davisErr) && davisErr.CoverageFailure == execreplay.DavisCoverageInsufficient {
				coverageProvenance.Status = "insufficient"
				coverageProvenance.Failure = string(execreplay.DavisCoverageInsufficient)
			}
			candidateProvenance.DavisCoverage = cloneDavisCoverageProvenance(coverageProvenance)
		}
		candidateProvenance.Compilation = &compilation
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &candidateProvenance, false)
	}
	if coverageProvenance != nil && mappingPolicy.Mode == execreplay.DavisProblemsMappingExecution {
		coverageProvenance.Status = "verified"
		coverageProvenance.Verified = true
		candidateProvenance.DavisCoverage = cloneDavisCoverageProvenance(coverageProvenance)
	}
	p.addRetentionNotices(ctx, input.Mode, &compilation, state, hostNow)
	info.EffectiveQuery = compilation.EffectiveDQL

	effectiveRequest := sdkquery.ParseRequest{
		Query:        compilation.EffectiveDQL,
		Locale:       input.Options.Locale,
		Timezone:     timezoneName,
		QueryOptions: cloneQueryOptions(input.Options.ParserOptions),
	}
	validationRoot, err := input.Parse(ctx, effectiveRequest)
	if err != nil {
		prov := p.baseProvenance(state, input.OriginalQuery, hostNow, virtualNow, coverageProvenance)
		prov.EffectiveDQL, prov.Compilation = compilation.EffectiveDQL, &compilation
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &prov, false)
	}
	validationAST, err := execreplay.Adapt(validationRoot)
	if err != nil {
		prov := p.baseProvenance(state, input.OriginalQuery, hostNow, virtualNow, coverageProvenance)
		prov.EffectiveDQL, prov.Compilation = compilation.EffectiveDQL, &compilation
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &prov, false)
	}
	audit, err := execreplay.Audit(execreplay.AuditInput{
		ValidationAST: validationAST,
		Compilation:   compilation,
		SourcePolicy:  p.config.SourcePolicy,
		Timezone:      timezone,
	})
	if err != nil {
		prov := p.baseProvenance(state, input.OriginalQuery, hostNow, virtualNow, coverageProvenance)
		prov.EffectiveDQL, prov.Compilation, prov.Audit = compilation.EffectiveDQL, &compilation, &audit
		return PreparedQuery{}, p.failBeforeExecute(ctx, sink, replayErrorPrepare, err, info, &prov, false)
	}

	provenance := p.baseProvenance(state, input.OriginalQuery, hostNow, virtualNow, coverageProvenance)
	provenance.EffectiveDQL, provenance.Compilation, provenance.Audit = compilation.EffectiveDQL, &compilation, &audit
	prepared := PreparedQuery{
		OriginalQuery: input.OriginalQuery, EffectiveQuery: compilation.EffectiveDQL,
		Compilation: compilation, Audit: audit, Session: state, VirtualNow: virtualNow,
		HostNow: hostNow, Terminal: terminal, Disclosure: disclosure,
		Explain: compilation.Explain, sink: sink, provenance: provenance,
	}
	prepared.executeDefaultTimeframe = intersectGlobalDefault(globalDefault, execreplay.Interval{Start: state.DataStart.UTC(), End: visibleEnd})
	if sink != nil && len(compilation.Notices) > 0 {
		record := provenanceRecord("query_notice", provenance, map[string]any{
			"notices": replayNotices(compilation.Notices),
		})
		if err := sink.Append(ctx, record); err != nil {
			return PreparedQuery{}, newReplayAttemptError(replayErrorSink, err, info, false, 0, false)
		}
	}
	return prepared, nil
}

// Finalize performs the sole replay-state write in query execution.
func (p *ReplayQueryPreparer) Finalize(_ context.Context, prepared PreparedQuery) (session.ReplaySession, session.CompletionDisposition, error) {
	return p.config.Store.MarkCompleted(p.config.Locator, prepared.Session.SessionID, p.config.Clock.Now().UTC())
}

// Wait blocks through an injectable edge used by wait/live scheduling tests.
func (p *ReplayQueryPreparer) Wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	if p.config.WaitFunc != nil {
		return p.config.WaitFunc(ctx, delay)
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

// ValidateCadence routes a pre-loop cadence failure through the same
// restricted sink preflight and generic-error boundary as DQL preparation.
func (p *ReplayQueryPreparer) ValidateCadence(ctx context.Context, interval time.Duration) error {
	if interval >= MinReplayExecutionInterval {
		return nil
	}
	detail := fmt.Errorf("replay query interval %s is faster than the supported minimum of %s", interval, MinReplayExecutionInterval)
	state, stateErr := p.config.Store.Status(p.config.Locator)
	disclosure, provenancePath := p.authoritativeRoute(state, stateErr)
	info := ReplayExecutionInfo{Active: true, Disclosure: disclosure}
	var provenance *ReplayExecutionProvenance
	if stateErr == nil {
		info = replayInfoFromSession(state, "", disclosure)
		value := ReplayExecutionProvenance{Session: state}
		provenance = &value
	}
	sink, err := p.preflightSink(ctx, disclosure, provenancePath, info)
	if err != nil {
		return err
	}
	return p.failBeforeExecute(ctx, sink, replayErrorPrepare, detail, info, provenance, false)
}

func (p *ReplayQueryPreparer) preflightSink(ctx context.Context, disclosure, provenancePath string, info ReplayExecutionInfo) (session.ProvenanceSink, error) {
	if disclosure != session.ReplayDisclosureRestricted {
		return nil, nil
	}
	if provenancePath == "" {
		return nil, newReplayAttemptError(replayErrorSink, fmt.Errorf("restricted disclosure has no provenance path"), info, false, 0, false)
	}
	sink := p.config.SinkFactory(provenancePath)
	if sink == nil {
		return nil, newReplayAttemptError(replayErrorSink, fmt.Errorf("restricted provenance sink is unavailable"), info, false, 0, false)
	}
	// This is intentionally before readiness, drift, parse, adaptation, or
	// any other preparation that could disclose replay-specific detail.
	if err := sink.Preflight(ctx); err != nil {
		return nil, newReplayAttemptError(replayErrorSink, err, info, false, 0, false)
	}
	return sink, nil
}

func (p *ReplayQueryPreparer) authoritativeRoute(state session.ReplaySession, stateErr error) (string, string) {
	if stateErr == nil {
		return state.Disclosure, state.ProvenancePath
	}
	return p.config.FallbackDisclosure, p.config.FallbackProvenancePath
}

func (p *ReplayQueryPreparer) failBeforeExecute(ctx context.Context, sink session.ProvenanceSink, category replayErrorCategory, detail error, info ReplayExecutionInfo, provenance *ReplayExecutionProvenance, retryable bool) error {
	if sink != nil {
		if provenance == nil {
			value := ReplayExecutionProvenance{HostNow: p.config.Clock.Now().UTC(), OriginalDQL: info.OriginalQuery, VirtualNow: info.VirtualNow, Outcome: string(category), Detail: detail.Error()}
			provenance = &value
		} else {
			provenance.Outcome, provenance.Detail = string(category), detail.Error()
			if provenance.HostNow.IsZero() {
				provenance.HostNow = p.config.Clock.Now().UTC()
			}
		}
		additional := map[string]any(nil)
		if info.verificationOriginalValid != nil {
			additional = map[string]any{"verification": replayVerificationFields(
				replayVerificationFromError(info, detail),
			)}
		}
		if err := sink.Append(ctx, provenanceRecord("query_pre_execution", *provenance, additional)); err != nil {
			return newReplayAttemptError(replayErrorSink, err, info, false, 0, false)
		}
	}
	return newReplayAttemptError(category, detail, info, retryable, sdkRetryAfter(detail), false)
}

func (p *ReplayQueryPreparer) baseProvenance(state session.ReplaySession, query string, hostNow, virtualNow time.Time, coverage *DavisSnapshotCoverageProvenance) ReplayExecutionProvenance {
	return ReplayExecutionProvenance{
		Session: state, HostNow: hostNow, VirtualNow: virtualNow, OriginalDQL: query,
		DavisCoverage: cloneDavisCoverageProvenance(coverage),
	}
}

func replayDavisMappingPolicy(disclosure string, mode ReplayExecutionMode) execreplay.DavisProblemsMappingPolicy {
	if disclosure != session.ReplayDisclosureFull {
		return execreplay.DavisProblemsMappingPolicy{Mode: execreplay.DavisProblemsMappingDisabled}
	}
	if mode == ReplayExecutionExplain || mode == ReplayExecutionVerify {
		return execreplay.DavisProblemsMappingPolicy{Mode: execreplay.DavisProblemsMappingInspection}
	}
	return execreplay.DavisProblemsMappingPolicy{Mode: execreplay.DavisProblemsMappingExecution}
}

func firstDavisProblemsCandidate(sources []execreplay.SourceDescriptor) *execreplay.DavisProblemsMappingCandidate {
	for _, source := range sources {
		if source.DavisProblems == nil {
			continue
		}
		candidate := *source.DavisProblems
		if source.DavisProblems.Span != nil {
			span := *source.DavisProblems.Span
			candidate.Span = &span
		}
		return &candidate
	}
	return nil
}

func (p *ReplayQueryPreparer) inspectDavisCoverage(ctx context.Context, key DavisSnapshotCoverageKey) (DavisSnapshotCoverage, DavisSnapshotCoverageReuse, error) {
	if p.config.DavisCoverage == nil {
		observedAt := time.Now().UTC()
		return DavisSnapshotCoverage{ObservedAt: observedAt}, DavisSnapshotCoverageMiss,
			&DavisSnapshotCoverageError{Failure: DavisCoverageUnavailable, ObservedAt: observedAt}
	}
	return p.config.DavisCoverage.Coverage(ctx, key)
}

func davisCoverageProvenance(coverage DavisSnapshotCoverage, reuse DavisSnapshotCoverageReuse, err error) *DavisSnapshotCoverageProvenance {
	value := &DavisSnapshotCoverageProvenance{
		Status: "observed", Verified: false, OldestSnapshot: coverage.OldestSnapshot.UTC(),
		ObservedAt: coverage.ObservedAt.UTC(), Reuse: reuse,
	}
	if err == nil {
		return value
	}
	value.Status = "failed"
	value.Failure = string(DavisCoverageUnavailable)
	var coverageErr *DavisSnapshotCoverageError
	if errors.As(err, &coverageErr) {
		value.Failure = string(coverageErr.Failure)
		if value.ObservedAt.IsZero() {
			value.ObservedAt = coverageErr.ObservedAt.UTC()
		}
	}
	return value
}

func cloneDavisCoverageProvenance(value *DavisSnapshotCoverageProvenance) *DavisSnapshotCoverageProvenance {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func replayInfoFromSession(state session.ReplaySession, query, disclosure string) ReplayExecutionInfo {
	return ReplayExecutionInfo{
		Active: true, Disclosure: disclosure, SessionID: state.SessionID,
		ClockMode: state.ClockMode, DataStart: state.DataStart, DataEnd: state.DataEnd,
		OriginalQuery: query,
	}
}

func replayTimezone(name string) (string, *time.Location, error) {
	if name == "" || name == "UTC" || name == "Z" {
		return "UTC", time.UTC, nil
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return "", nil, fmt.Errorf("load replay query timezone %q: %w", name, err)
	}
	return name, location, nil
}

func replayGlobalDefault(options DQLExecuteOptions) (*execreplay.Interval, error) {
	if (options.DefaultTimeframeStart == "") != (options.DefaultTimeframeEnd == "") {
		return nil, fmt.Errorf("replay queries require both default-timeframe-start and default-timeframe-end when either is set")
	}
	if options.DefaultTimeframeStart == "" {
		return nil, nil
	}
	start, err := time.Parse(time.RFC3339Nano, options.DefaultTimeframeStart)
	if err != nil {
		return nil, fmt.Errorf("parse default-timeframe-start as RFC 3339: %w", err)
	}
	end, err := time.Parse(time.RFC3339Nano, options.DefaultTimeframeEnd)
	if err != nil {
		return nil, fmt.Errorf("parse default-timeframe-end as RFC 3339: %w", err)
	}
	value := &execreplay.Interval{Start: start.UTC(), End: end.UTC()}
	if !value.Valid() {
		return nil, fmt.Errorf("default timeframe start must be earlier than end")
	}
	return value, nil
}

func intersectGlobalDefault(global *execreplay.Interval, visible execreplay.Interval) *execreplay.Interval {
	if global == nil {
		return nil
	}
	intersection, ok := global.Intersect(visible)
	if !ok {
		return nil
	}
	return &intersection
}

func cloneQueryOptions(options sdkquery.QueryOptions) sdkquery.QueryOptions {
	if len(options) == 0 {
		return nil
	}
	clone := make(sdkquery.QueryOptions, len(options))
	for key, value := range options {
		clone[key] = value
	}
	return clone
}

func sdkRetryAfter(err error) time.Duration {
	delay, _ := sdkquery.RetryAfter(err)
	return delay
}
