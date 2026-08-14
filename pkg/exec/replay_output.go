package exec

import (
	"time"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/pkg/output"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func replayOutputMetadata(prepared PreparedQuery, validated []execreplay.ValidatedResultContract, canonical string, notifications []QueryNotification) *output.ReplayMetadata {
	visibleEnd := session.VisibleEnd(prepared.Session, prepared.HostNow).UTC()
	metadata := &output.ReplayMetadata{
		Active:                       true,
		SessionID:                    prepared.Session.SessionID,
		SessionStartedAt:             replayOutputTime(prepared.Session.SessionStartedAt),
		ClockMode:                    prepared.Session.ClockMode,
		AnchorHost:                   replayOutputTime(prepared.Session.AnchorHost),
		AnchorVirtual:                replayOutputTime(prepared.Session.AnchorVirtual),
		VirtualNow:                   replayOutputTime(prepared.VirtualNow),
		DataStart:                    replayOutputTime(prepared.Session.DataStart),
		DataEnd:                      replayOutputTime(prepared.Session.DataEnd),
		VisibleEnd:                   replayOutputTime(visibleEnd),
		State:                        prepared.Session.Status,
		OriginalQuery:                prepared.OriginalQuery,
		EffectiveQuery:               prepared.EffectiveQuery,
		GrailCanonicalEffectiveQuery: canonical,
		Sources:                      make([]output.ReplaySourceMetadata, 0, len(prepared.Compilation.Sources)),
		Warnings:                     make([]string, 0, len(prepared.Compilation.Notices)+len(notifications)),
	}
	for _, source := range prepared.Compilation.Sources {
		item := output.ReplaySourceMetadata{
			Ordinal: source.Source.Ordinal, Path: source.Source.Path, Name: source.Source.Name,
			Class: string(source.Source.Class), BoundaryPolicy: string(source.Source.BoundaryPolicy),
		}
		if source.Requested != nil {
			item.RequestedFrom = replayOutputTime(source.Requested.Range.Start)
			item.RequestedTo = replayOutputTime(source.Requested.Range.End)
		}
		if source.Effective != nil {
			item.EffectiveFrom = replayOutputTime(source.Effective.Start)
			item.EffectiveTo = replayOutputTime(source.Effective.End)
		}
		if source.PhysicalRange != nil {
			item.PhysicalFrom = replayOutputTime(source.PhysicalRange.Start)
			item.PhysicalTo = replayOutputTime(source.PhysicalRange.End)
		}
		if mapping := source.DavisMapping; mapping != nil {
			item.DavisProblemsMapping = &output.DavisProblemsMappingMetadata{
				Eligible: true, OriginalView: mapping.Candidate.OriginalToken,
				EffectiveSnapshotTable: mapping.Candidate.SnapshotToken,
				LogicalF:               replayOutputTime(mapping.Logical.F), LogicalT: replayOutputTime(mapping.Logical.T),
				PhysicalW: replayOutputTime(mapping.Physical.W), PhysicalT: replayOutputTime(mapping.Physical.T),
				WarmupClamped: mapping.WarmupClamped,
			}
		}
		if actual, ok := replayValidatedSource(validated, source.Source.Ordinal); ok {
			if actual.PhysicalRange != nil {
				item.PhysicalFrom = replayOutputTime(actual.PhysicalRange.Start)
				item.PhysicalTo = replayOutputTime(actual.PhysicalRange.End)
			}
			item.NaturalIntervalNS = actual.NaturalInterval.Nanoseconds()
			item.LowerSpillNS = actual.LowerSpill.Nanoseconds()
			item.UpperSpillNS = actual.UpperSpill.Nanoseconds()
		} else if source.ResultContract != nil && source.ResultContract.DeclaredNaturalInterval != nil {
			item.NaturalIntervalNS = source.ResultContract.DeclaredNaturalInterval.Nanoseconds()
		}
		metadata.Sources = append(metadata.Sources, item)
	}
	if coverage := prepared.provenance.DavisCoverage; coverage != nil {
		metadata.DavisSnapshotCoverage = &output.DavisSnapshotCoverageMetadata{
			Status: coverage.Status, Verified: coverage.Verified,
			OldestSnapshot: replayOutputTime(coverage.OldestSnapshot), ObservedAt: replayOutputTime(coverage.ObservedAt),
			Reuse: string(coverage.Reuse), Failure: coverage.Failure,
		}
	}
	metadata.DavisMappingsAudited = prepared.Audit.DavisMappingsAudited
	for _, notice := range prepared.Compilation.Notices {
		metadata.Warnings = append(metadata.Warnings, notice.Message)
	}
	for _, notification := range notifications {
		if notification.Message != "" {
			metadata.Warnings = append(metadata.Warnings, notification.Message)
		}
	}
	return metadata
}

func fullReplayOutput(info *ReplayExecutionInfo) *output.ReplayMetadata {
	if info == nil || !info.Active || info.Disclosure != session.ReplayDisclosureFull || info.Output == nil {
		return nil
	}
	value := *info.Output
	value.Sources = append([]output.ReplaySourceMetadata{}, info.Output.Sources...)
	for index := range value.Sources {
		if info.Output.Sources[index].DavisProblemsMapping != nil {
			mapping := *info.Output.Sources[index].DavisProblemsMapping
			value.Sources[index].DavisProblemsMapping = &mapping
		}
	}
	value.Warnings = append([]string{}, info.Output.Warnings...)
	if info.Output.DavisSnapshotCoverage != nil {
		coverage := *info.Output.DavisSnapshotCoverage
		value.DavisSnapshotCoverage = &coverage
	}
	return &value
}

// ReplayOutputMetadata returns an independent full-disclosure output record.
// Restricted and non-replay execution intentionally return nil so generic
// agent printers retain the ordinary envelope schema.
func ReplayOutputMetadata(info *ReplayExecutionInfo) *output.ReplayMetadata {
	return fullReplayOutput(info)
}

func replayOutputTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}
