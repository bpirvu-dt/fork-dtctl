package replay

import (
	"fmt"
	"strings"
	"time"
)

const davisProblemsWarmupCaveat = "When the visible window is short, the snapshot read may need to start before <visible-start> while the lifetime filter bounds stay at <visible-start> and <visible-end>; open problem snapshots have a documented six-hour refresh cadence, so retain at least six hours of warm-up."

const davisProblemsReconstructionTemplate = `
| sort timestamp desc
| dedup event.id
| filter event.start < %[2]s and coalesce(event.end, %[2]s) >= %[1]s`

const (
	davisProblemsView          = "dt.davis.problems"
	davisProblemsSnapshotTable = "dt.davis.problems.snapshots"
	davisProblemsWarmup        = 6 * time.Hour

	// DavisCoverageNotVerifiedMessage is the exact execution-time-only caveat
	// shown by probe-free explain and verify operations.
	DavisCoverageNotVerifiedMessage = "Snapshot coverage was not verified. The coverage gate runs only when the query executes."
	// DavisCoverageInspectionFailedMessage is the stable full-disclosure suffix
	// for every bounded-inspection failure mode.
	DavisCoverageInspectionFailedMessage = "Snapshot coverage could not be proven because the bounded oldest-snapshot inspection failed."
	// DavisCoverageInsufficientMessage is the stable full-disclosure suffix when
	// the oldest observed snapshot is later than W.
	DavisCoverageInsufficientMessage = "Snapshot coverage could not be proven because the oldest available snapshot is later than the required snapshot-read start."
)

const (
	// DavisProblemsView is the sole exact current-view token eligible for the
	// v9 full-disclosure mapping.
	DavisProblemsView = davisProblemsView
	// DavisProblemsSnapshotTable is the fixed source used by the mapping and
	// its bounded oldest-snapshot inspection.
	DavisProblemsSnapshotTable = davisProblemsSnapshotTable
)

// DavisProblemsMappingMode keeps the current-view exception separate from the
// ordinary seven-table record allowlist.
type DavisProblemsMappingMode string

const (
	DavisProblemsMappingDisabled   DavisProblemsMappingMode = ""
	DavisProblemsMappingExecution  DavisProblemsMappingMode = "execution"
	DavisProblemsMappingInspection DavisProblemsMappingMode = "inspection"
)

// DavisSnapshotCoverage is the copied, telemetry-free result supplied to the
// pure compiler after one bounded inspection.
type DavisSnapshotCoverage struct {
	OldestSnapshot time.Time
	ObservedAt     time.Time
}

// DavisProblemsMappingPolicy is the complete typed mapping authorization for
// one pure compilation. Inspection explicitly carries no coverage result.
type DavisProblemsMappingPolicy struct {
	Mode     DavisProblemsMappingMode
	Coverage *DavisSnapshotCoverage
}

// DavisProblemsMappingCandidate records the exact original AST token selected
// for the separately evidenced transformation.
type DavisProblemsMappingCandidate struct {
	OriginalToken  string
	SnapshotToken  string
	DataObjectPath string
	SourcePath     string
	Span           *Span
}

// Clone returns an independent candidate, including its optional source span.
func (c DavisProblemsMappingCandidate) Clone() DavisProblemsMappingCandidate {
	clone := c
	if c.Span != nil {
		span := *c.Span
		clone.Span = &span
	}
	return clone
}

// DavisLogicalViewRange is [F,T). It is intentionally a different type from
// DavisPhysicalSnapshotRange so compiler and audit code cannot interchange the
// lifetime window and the snapshot-read window accidentally.
type DavisLogicalViewRange struct {
	F time.Time
	T time.Time
}

func (r DavisLogicalViewRange) interval() Interval { return Interval{Start: r.F, End: r.T} }

// DavisPhysicalSnapshotRange is [W,T), where W=max(data_start,F-6h).
type DavisPhysicalSnapshotRange struct {
	W time.Time
	T time.Time
}

func (r DavisPhysicalSnapshotRange) interval() Interval { return Interval{Start: r.W, End: r.T} }

// DavisMappingCoverage is the compiler's explicit execution/inspection fact.
// False marks probe-free inspection or a partial compilation rejected by the
// coverage gate; only a successful compilation can carry Verified=true.
type DavisMappingCoverage struct {
	Verified       bool
	OldestSnapshot time.Time
	ObservedAt     time.Time
}

// DavisProblemsMappingCompilation carries independently typed F/T and W/T
// facts from emission through explanation, provenance, and audit.
type DavisProblemsMappingCompilation struct {
	Candidate     DavisProblemsMappingCandidate
	Logical       DavisLogicalViewRange
	Physical      DavisPhysicalSnapshotRange
	WarmupClamped bool
	Coverage      DavisMappingCoverage
}

// DavisProblemsMappingExpectation is compiled from the original AST and
// consumed by the fresh validation-AST audit.
type DavisProblemsMappingExpectation struct {
	SourceOrdinal int
	Candidate     DavisProblemsMappingCandidate
	Logical       DavisLogicalViewRange
	Physical      DavisPhysicalSnapshotRange
	Coverage      DavisMappingCoverage
}

// DavisCoverageFailure selects one of the two stable section-18 suffixes.
type DavisCoverageFailure string

const (
	DavisCoverageInspectionFailed DavisCoverageFailure = "inspection_failed"
	DavisCoverageInsufficient     DavisCoverageFailure = "insufficient"
)

func validateDavisMappingPolicy(policy DavisProblemsMappingPolicy) error {
	switch policy.Mode {
	case DavisProblemsMappingDisabled:
		if policy.Coverage != nil {
			return replayError(ErrorAudit, nil, davisProblemsView, "A disabled Davis problems mapping policy carries a coverage result.", "Do not reuse mapping coverage outside an eligible full-disclosure compilation.")
		}
	case DavisProblemsMappingExecution:
	case DavisProblemsMappingInspection:
		if policy.Coverage != nil {
			return replayError(ErrorAudit, nil, davisProblemsView, "Probe-free Davis problems inspection received a coverage result.", "Explain and verify must not consult or reuse the coverage memo.")
		}
	default:
		return replayError(ErrorUnsupportedForm, nil, davisProblemsView, fmt.Sprintf("The Davis problems mapping mode %q is unsupported.", policy.Mode), "Use disabled, execution, or probe-free inspection mode.")
	}
	return nil
}

func davisProblemsReconstruction(logicalF, logicalT string) string {
	return fmt.Sprintf(davisProblemsReconstructionTemplate, logicalF, logicalT)
}

func mappingCandidateFor(table string, dataObject, command *Node, policy DavisProblemsMappingPolicy) (*DavisProblemsMappingCandidate, bool) {
	if table != davisProblemsView || dataObject == nil || dataObject.Canonical != davisProblemsView {
		return nil, false
	}
	if policy.Mode != DavisProblemsMappingExecution && policy.Mode != DavisProblemsMappingInspection {
		return nil, false
	}
	candidate := &DavisProblemsMappingCandidate{
		OriginalToken: davisProblemsView, SnapshotToken: davisProblemsSnapshotTable,
		DataObjectPath: dataObject.Path, SourcePath: command.Path,
	}
	if dataObject.Span != nil {
		span := *dataObject.Span
		candidate.Span = &span
	}
	return candidate, true
}

func davisCoverageError(candidate *DavisProblemsMappingCandidate, failure DavisCoverageFailure) *DavisCurrentViewError {
	node := &Node{}
	if candidate != nil {
		node.Path = candidate.SourcePath
		if candidate.Span != nil {
			span := *candidate.Span
			node.Span = &span
		}
	}
	err, _ := currentDavisView(davisProblemsView, node)
	err.CoverageFailure = failure
	return err
}

// DavisProblemsCoverageError returns the shipped current-view guidance plus
// one stable, sanitized coverage suffix.
func DavisProblemsCoverageError(candidate *DavisProblemsMappingCandidate, failure DavisCoverageFailure) *DavisCurrentViewError {
	return davisCoverageError(candidate, failure)
}

func currentDavisView(table string, node *Node) (*DavisCurrentViewError, bool) {
	var snapshot, identity, pattern string
	switch table {
	case davisProblemsView:
		snapshot, identity = davisProblemsSnapshotTable, "problem"
		reconstruction := strings.ReplaceAll(davisProblemsReconstruction("<visible-start>", "<visible-end>"), "\n", " ")
		pattern = fmt.Sprintf("fetch %s, from:<visible-start>, to:<visible-end>%s", snapshot, reconstruction)
	case "dt.davis.events":
		snapshot, identity = "dt.davis.events.snapshots", "event"
		// Event-view equivalence is unverified pending its own spike; do not
		// assume the problem lifetime fields or filter apply to events.
		pattern = fmt.Sprintf("fetch %s, from:<visible-start>, to:<visible-end> | sort timestamp desc | dedup event.id", snapshot)
	default:
		return nil, false
	}
	err := &DavisCurrentViewError{
		View:               table,
		SnapshotTable:      snapshot,
		IdentityKind:       identity,
		IdentityField:      "event.id",
		LatestPerIDPattern: pattern,
		Path:               node.Path,
	}
	if node.Span != nil {
		span := *node.Span
		err.Span = &span
	}
	return err, true
}
