package replay

import (
	"fmt"
	"strings"
	"time"
)

// ErrorCode is a stable replay preparation error category.
type ErrorCode string

const (
	ErrorASTContract       ErrorCode = "ast_contract"
	ErrorUnsupportedSource ErrorCode = "unsupported_source"
	ErrorUnsupportedForm   ErrorCode = "unsupported_form"
	ErrorCurrentState      ErrorCode = "current_state"
	ErrorTimeframe         ErrorCode = "timeframe"
	ErrorShift             ErrorCode = "shift"
	ErrorNonOverlap        ErrorCode = "non_overlap"
	ErrorAudit             ErrorCode = "audit"
	ErrorResultContract    ErrorCode = "result_contract"
)

// ReplayError is a typed, full-disclosure compiler rejection. Later phases can
// map Code to a restricted message without discarding the detailed reason.
type ReplayError struct {
	Code      ErrorCode
	Message   string
	Remedy    string
	Construct string
	Path      string
	Span      *Span
}

func (e *ReplayError) Error() string {
	parts := []string{e.Message, "The query was not executed."}
	if e.Remedy != "" {
		parts = append(parts, e.Remedy)
	}
	return strings.Join(parts, "\n")
}

func replayError(code ErrorCode, node *Node, construct, message, remedy string) *ReplayError {
	err := &ReplayError{Code: code, Construct: construct, Message: message, Remedy: remedy}
	if node != nil {
		err.Path = node.Path
		if node.Span != nil {
			span := *node.Span
			err.Span = &span
		}
	}
	return err
}

// DavisCurrentViewError rejects a mutable Davis view while carrying the safe,
// author-owned snapshot reconstruction pattern needed by full disclosure and
// provenance routing.
type DavisCurrentViewError struct {
	View               string
	SnapshotTable      string
	IdentityKind       string
	IdentityField      string
	LatestPerIDPattern string
	CoverageFailure    DavisCoverageFailure
	Path               string
	Span               *Span
}

// NoticeKind lets later phases route compiler facts without reinterpreting
// prose. Routing is deliberately outside this package.
type NoticeKind string

const (
	NoticeNotification NoticeKind = "notification"
	NoticeWarning      NoticeKind = "warning"
)

// NoticeCode is a stable compiler notice category.
type NoticeCode string

const (
	NoticeFixedDayInterval               NoticeCode = "fixed_1d_means_24h"
	NoticeDavisWarmup                    NoticeCode = "davis_snapshot_warmup"
	NoticeDavisProblemsMapping           NoticeCode = "davis_problems_view_mapping"
	NoticeRetentionBoundary              NoticeCode = "retention_boundary"
	NoticeRetentionNotVerified           NoticeCode = "retention_not_verified"
	NoticeHistoricalResolutionUnverified NoticeCode = "historical_metric_resolution_not_verified"
)

// Notice is full-disclosure compiler data. Phase 4 decides whether it goes to
// ordinary output or private provenance.
type Notice struct {
	Kind          NoticeKind
	Code          NoticeCode
	Message       string
	SourceOrdinal int
	PerExecution  bool
}

// CompileInput contains every value that can affect one deterministic replay
// compilation. VirtualStart is needed only for Davis warm-up guidance; when it
// is zero it has the session-model default of ReplayInterval.Start.
type CompileInput struct {
	OriginalDQL     string
	AST             *AST
	VirtualNow      time.Time
	VirtualStart    time.Time
	ReplayInterval  Interval
	VisibleInterval Interval
	Locale          string
	TimezoneName    string
	Timezone        *time.Location
	GlobalDefault   *Interval
	SourcePolicy    SourcePolicy
	DavisMapping    DavisProblemsMappingPolicy
	// PrecomputedAnalysis may be supplied only by source analysis performed for
	// this same AST revision and preparation.
	PrecomputedAnalysis *PrecomputedSourceAnalysis
}

// ResultSourceIdentity binds post-execution metadata to one compiled metric
// source without containing telemetry or credentials.
type ResultSourceIdentity struct {
	Ordinal int
	Path    string
	Name    string
	Shape   MetricShape
}

// ReplayResultContract contains only facts established before execution.
type ReplayResultContract struct {
	Source                  ResultSourceIdentity
	LogicalWindow           Interval
	BoundaryPolicy          BoundaryPolicy
	DeclaredNaturalInterval *time.Duration
	NaturalIntervalRequired bool
}

// SourceCompilation is one independently classified source in source order.
type SourceCompilation struct {
	Source          SourceDescriptor
	Requested       *RequestedRange
	Effective       *Interval
	Overlap         OverlapProof
	ResultContract  *ReplayResultContract
	PhysicalRange   *Interval
	PhysicalPending bool
	DavisMapping    *DavisProblemsMappingCompilation
}

// ClockExplain contains only caller-supplied clock facts.
type ClockExplain struct {
	VirtualNow      time.Time
	VirtualStart    time.Time
	ReplayInterval  Interval
	VisibleInterval Interval
	Locale          string
	Timezone        string
	GlobalDefault   *Interval
}

// SourceExplain is the typed per-source data consumed by --explain-replay in
// Phase 4. A nil effective range denotes a proven non-overlap, never a fake
// source window.
type SourceExplain struct {
	Ordinal         int
	Path            string
	Class           SourceClass
	Name            string
	BoundaryPolicy  BoundaryPolicy
	Requested       *RequestedRange
	Effective       *Interval
	Classification  OverlapClassification
	Proof           OverlapProof
	RecordTimeField string
	DavisMapping    *DavisProblemsMappingCompilation
}

// ExplainData is the complete pure compiler explanation payload.
type ExplainData struct {
	Clock            ClockExplain
	Sources          []SourceExplain
	Notices          []Notice
	EffectiveDQL     string
	CoverageVerified *bool
	CoverageMessage  string
}

// CompileResult is returned even with NonOverlapError so callers can inspect
// the proof and decide whether a realtime loop may retry.
type CompileResult struct {
	OriginalDQL     string
	EffectiveDQL    string
	Sources         []SourceCompilation
	ResultContracts []ReplayResultContract
	Notices         []Notice
	Explain         ExplainData
	AuditPlan       AuditPlan
	AuditRequired   bool
	InspectionOnly  bool
}

// NonOverlapError is the typed whole-query decision when any telemetry source
// has no visible intersection.
type NonOverlapError struct {
	Classification OverlapClassification
	Sources        []SourceExplain
}

func (e *NonOverlapError) Error() string {
	return "The requested source timeframe does not overlap the currently visible replay interval.\nThe query was not executed."
}

// Compile transforms one fresh internal AST view into effective DQL. It reads
// no clock, environment, configuration, files, terminal, or network.
func Compile(input CompileInput) (CompileResult, error) {
	input, err := validateCompileInput(input)
	if err != nil {
		return CompileResult{}, err
	}
	policy := cloneSourcePolicy(input.SourcePolicy)
	working, sources, err := sourceAnalysesForCompile(input, policy)
	if err != nil {
		return CompileResult{}, err
	}
	result := newCompileResult(input)
	context := timeframeContext{
		VirtualNow: input.VirtualNow, ReplayInterval: input.ReplayInterval,
		VisibleInterval: input.VisibleInterval, Timezone: input.Timezone,
		GlobalDefault: cloneInterval(input.GlobalDefault), DefaultLookback: policy.DefaultLookback,
	}
	analysesByOrdinal := make(map[int]*sourceAnalysis, len(sources))
	for _, source := range sources {
		analysesByOrdinal[source.Ordinal] = source
		compiled, compileErr := compileSource(source, context, input)
		result.Sources = append(result.Sources, compiled)
		result.Explain.Sources = append(result.Explain.Sources, explainSource(compiled))
		if compileErr != nil {
			return result, compileErr
		}
		if compiled.DavisMapping != nil {
			mapping := compiled.DavisMapping
			result.AuditPlan.DavisProblemsMappings = append(result.AuditPlan.DavisProblemsMappings, DavisProblemsMappingExpectation{
				SourceOrdinal: source.Ordinal, Candidate: mapping.Candidate,
				Logical: mapping.Logical, Physical: mapping.Physical, Coverage: mapping.Coverage,
			})
			verified := mapping.Coverage.Verified
			result.Explain.CoverageVerified = &verified
			if !verified {
				result.Explain.CoverageMessage = DavisCoverageNotVerifiedMessage
				result.InspectionOnly = true
			}
		}
		if compiled.ResultContract != nil {
			contract := *compiled.ResultContract
			result.ResultContracts = append(result.ResultContracts, contract)
		}
		notices, noticeErr := sourceNotices(source, compiled, input)
		if noticeErr != nil {
			return result, noticeErr
		}
		result.Notices = append(result.Notices, notices...)
	}
	result.Explain.Notices = append([]Notice(nil), result.Notices...)
	if nonOverlap := classifyWholeQueryNonOverlap(result.Explain.Sources); nonOverlap != nil {
		return result, nonOverlap
	}
	fingerprint, err := semanticFingerprintWithMappings(working, sources, input.VirtualNow, result.AuditPlan.DavisProblemsMappings, nil)
	if err != nil {
		return result, err
	}
	result.AuditPlan.UntouchedSemanticFingerprint = fingerprint
	edits, err := buildReplayEdits(working, input.OriginalDQL, analysesByOrdinal, result.Sources, input.VirtualNow)
	if err != nil {
		return result, err
	}
	effective, err := ApplyPositionEdits(input.OriginalDQL, edits)
	if err != nil {
		return result, err
	}
	result.EffectiveDQL = effective
	result.Explain.EffectiveDQL = effective
	result.AuditRequired = true
	return result, nil
}

func validateCompileInput(input CompileInput) (CompileInput, error) {
	if input.AST == nil || input.AST.Root == nil {
		return input, replayError(ErrorASTContract, nil, "query", "The compiler input has no adapted DQL AST.", "Parse and adapt the original DQL before compiling it.")
	}
	if input.OriginalDQL == "" {
		return input, replayError(ErrorASTContract, input.AST.Root, "query", "The original DQL text is empty.", "Provide the exact source text that produced the AST.")
	}
	if !input.ReplayInterval.Valid() {
		return input, replayError(ErrorTimeframe, nil, "replay interval", "The replay interval is empty or invalid.", "Provide data_start earlier than data_end.")
	}
	input.ReplayInterval.Start = input.ReplayInterval.Start.UTC()
	input.ReplayInterval.End = input.ReplayInterval.End.UTC()
	input.VirtualNow = input.VirtualNow.UTC()
	if input.VirtualNow.Before(input.ReplayInterval.Start) || input.VirtualNow.After(input.ReplayInterval.End) {
		return input, replayError(ErrorTimeframe, nil, "virtual now", "Virtual now is outside the replay interval.", "Use a virtual time from data_start through data_end.")
	}
	if input.VirtualStart.IsZero() {
		input.VirtualStart = input.ReplayInterval.Start
	} else {
		input.VirtualStart = input.VirtualStart.UTC()
	}
	if input.VirtualStart.Before(input.ReplayInterval.Start) || input.VirtualStart.After(input.ReplayInterval.End) {
		return input, replayError(ErrorTimeframe, nil, "virtual start", "Virtual start is outside the replay interval.", "Use a virtual start from data_start through data_end.")
	}
	if input.VirtualNow.Before(input.VirtualStart) {
		return input, replayError(ErrorTimeframe, nil, "virtual now", "Virtual now is earlier than virtual start.", "Use the current session clock value at or after virtual_start.")
	}
	wantVisible := Interval{Start: input.ReplayInterval.Start, End: input.VirtualNow}
	if input.VirtualNow.After(input.ReplayInterval.End) {
		wantVisible.End = input.ReplayInterval.End
	}
	input.VisibleInterval.Start = input.VisibleInterval.Start.UTC()
	input.VisibleInterval.End = input.VisibleInterval.End.UTC()
	if !input.VisibleInterval.Start.Equal(wantVisible.Start) || !input.VisibleInterval.End.Equal(wantVisible.End) {
		return input, replayError(ErrorTimeframe, nil, "visible replay interval", "The supplied visible replay interval does not match the replay clock values.", "Use [data_start, min(virtual_now, data_end)).")
	}
	if input.Timezone == nil {
		input.Timezone = time.UTC
	}
	if input.TimezoneName == "" {
		input.TimezoneName = input.Timezone.String()
	} else if input.TimezoneName != input.Timezone.String() {
		return input, replayError(ErrorTimeframe, nil, "replay timezone", "The supplied replay timezone name does not identify the supplied timezone rules.", "Pass one canonical timezone value for both compilation and replay provenance.")
	}
	if input.GlobalDefault != nil {
		if !input.GlobalDefault.Valid() {
			return input, replayError(ErrorTimeframe, nil, "global default timeframe", "The global default timeframe is empty or invalid.", "Provide both default-timeframe endpoints with start before end.")
		}
		input.GlobalDefault = &Interval{Start: input.GlobalDefault.Start.UTC(), End: input.GlobalDefault.End.UTC()}
	}
	if len(input.SourcePolicy.RecordTables) == 0 {
		return input, replayError(ErrorUnsupportedSource, nil, "source policy", "The replay source policy is empty.", "Pass the explicit milestone 1 source policy.")
	}
	if err := validateDavisMappingPolicy(input.DavisMapping); err != nil {
		return input, err
	}
	return input, nil
}

func sourceAnalysesForCompile(input CompileInput, policy SourcePolicy) (*AST, []*sourceAnalysis, error) {
	precomputed := input.PrecomputedAnalysis
	if precomputed == nil {
		working := input.AST.Clone()
		sources, err := analyzeSources(working, policy, input.DavisMapping)
		return working, sources, err
	}
	if precomputed.ast == nil || precomputed.ast.Root == nil {
		return nil, nil, replayError(ErrorASTContract, input.AST.Root, "query", "The precomputed source analysis has no adapted DQL AST.", "Analyze this AST again before compiling it.")
	}
	revision, err := replayASTRevision(input.AST)
	if err != nil || revision != precomputed.astRevision {
		return nil, nil, replayError(ErrorASTContract, input.AST.Root, "query", "The precomputed source analysis does not match this DQL AST revision.", "Analyze this exact adapted AST again before compiling it.")
	}
	if !sameSourcePolicy(policy, precomputed.policy) {
		return nil, nil, replayError(ErrorAudit, input.AST.Root, "source policy", "The precomputed source analysis used a different source policy.", "Analyze and compile with the same explicit source policy.")
	}
	if input.DavisMapping.Mode != precomputed.mappingMode {
		return nil, nil, replayError(ErrorAudit, input.AST.Root, davisProblemsView, "The precomputed source analysis used a different Davis mapping mode.", "Analyze and compile with the same mapping mode.")
	}
	return precomputed.ast, precomputed.sources, nil
}

func sameSourcePolicy(left, right SourcePolicy) bool {
	if left.DefaultLookback != right.DefaultLookback || len(left.RecordTables) != len(right.RecordTables) {
		return false
	}
	for table, policy := range left.RecordTables {
		if right.RecordTables[table] != policy {
			return false
		}
	}
	return true
}

func newCompileResult(input CompileInput) CompileResult {
	clock := ClockExplain{
		VirtualNow: input.VirtualNow, VirtualStart: input.VirtualStart,
		ReplayInterval: input.ReplayInterval, VisibleInterval: input.VisibleInterval,
		Locale: input.Locale, Timezone: input.TimezoneName, GlobalDefault: cloneInterval(input.GlobalDefault),
	}
	return CompileResult{OriginalDQL: input.OriginalDQL, Explain: ExplainData{Clock: clock}}
}

func cloneSourcePolicy(policy SourcePolicy) SourcePolicy {
	clone := SourcePolicy{DefaultLookback: policy.DefaultLookback, RecordTables: make(map[string]RecordSourcePolicy, len(policy.RecordTables))}
	for key, value := range policy.RecordTables {
		clone.RecordTables[key] = value
	}
	return clone
}

func cloneInterval(value *Interval) *Interval {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func newMetricResultContract(source *sourceAnalysis, logical Interval) ReplayResultContract {
	contract := ReplayResultContract{
		Source: ResultSourceIdentity{
			Ordinal: source.Ordinal, Path: source.Path, Name: source.Name, Shape: source.Metric.Shape,
		},
		LogicalWindow: logical, BoundaryPolicy: BoundaryMetricBucket,
		NaturalIntervalRequired: true,
	}
	if source.Metric.DeclaredInterval != nil {
		value := *source.Metric.DeclaredInterval
		contract.DeclaredNaturalInterval = &value
	}
	return contract
}

func (e *DavisCurrentViewError) Error() string {
	reduction := fmt.Sprintf("then reduce to the latest snapshot per %s ID", e.IdentityKind)
	if e.View == "dt.davis.problems" {
		reduction += ", and keep only problems whose lifetime overlaps the visible interval"
	}
	message := fmt.Sprintf(
		"%s is a current-state Davis view. dtctl cannot reproduce its value at virtual now.\n"+
			"The query was not executed. Fetch %s over the visible replay interval, %s.\n"+
			"Pattern: %s",
		e.View, e.SnapshotTable, reduction, e.LatestPerIDPattern,
	)
	if e.View == "dt.davis.problems" {
		message += "\n" + davisProblemsWarmupCaveat
	}
	switch e.CoverageFailure {
	case DavisCoverageInspectionFailed:
		message += "\n" + DavisCoverageInspectionFailedMessage
	case DavisCoverageInsufficient:
		message += "\n" + DavisCoverageInsufficientMessage
	}
	return message
}
