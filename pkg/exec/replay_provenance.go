package exec

import (
	"time"

	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

func provenanceRecord(event string, provenance ReplayExecutionProvenance, additional map[string]any) session.ReplayProvenanceRecord {
	recordedAt := provenance.HostNow.UTC()
	if recordedAt.IsZero() {
		recordedAt = time.Now().UTC()
	}
	fields := map[string]any{
		"outcome":       provenance.Outcome,
		"detail":        provenance.Detail,
		"original_dql":  provenance.OriginalDQL,
		"effective_dql": provenance.EffectiveDQL,
	}
	if provenance.CanonicalEffectiveDQL != "" {
		fields["grail_canonical_effective_dql"] = provenance.CanonicalEffectiveDQL
	}
	if provenance.Session.SessionID != "" {
		fields["session"] = replaySessionProvenance(provenance.Session, provenance.VirtualNow)
	}
	if provenance.Compilation != nil {
		fields["sources"] = replaySources(provenance.Compilation.Sources, provenance.Validated)
		fields["notices"] = replayNotices(provenance.Compilation.Notices)
	}
	if provenance.DavisCoverage != nil {
		fields["davis_snapshot_coverage"] = replayDavisCoverage(*provenance.DavisCoverage)
	}
	if provenance.Audit != nil {
		audit := map[string]any{
			"ok":                  provenance.Audit.OK,
			"source_count":        provenance.Audit.SourceCount,
			"no_semantic_now":     provenance.Audit.NoSemanticNow,
			"all_sources_bounded": provenance.Audit.AllSourcesBounded,
			"structure_matches":   provenance.Audit.StructureMatches,
			"rules":               append([]string(nil), provenance.Audit.Rules...),
		}
		if provenance.Audit.DavisMappingsAudited {
			audit["davis_mappings_audited"] = true
		}
		fields["audit"] = audit
	}
	if provenance.Validated != nil {
		fields["validated_result_contracts"] = replayValidatedContracts(provenance.Validated)
	}
	if provenance.Notifications != nil {
		fields["query_notifications"] = replayQueryNotifications(provenance.Notifications)
	}
	if provenance.GrailContributions != nil {
		fields["grail_contributions"] = replayGrailContributions(*provenance.GrailContributions)
	}
	if provenance.Completion != "" {
		fields["completion_disposition"] = provenance.Completion
	}
	for key, value := range additional {
		fields[key] = value
	}
	return session.ReplayProvenanceRecord{
		SchemaVersion: session.ReplayProvenanceSchemaVersion,
		RecordedAt:    recordedAt,
		Event:         event,
		SessionID:     provenance.Session.SessionID,
		Fields:        fields,
	}
}

func replayGrailContributions(value Contributions) map[string]any {
	buckets := make([]map[string]any, 0, len(value.Buckets))
	for _, bucket := range value.Buckets {
		buckets = append(buckets, map[string]any{
			"name": bucket.Name, "table": bucket.Table, "scanned_bytes": bucket.ScannedBytes,
			"matched_records_ratio": bucket.MatchedRecordsRatio,
		})
	}
	return map[string]any{"buckets": buckets}
}

func replayQueryNotifications(values []QueryNotification) []map[string]any {
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		result = append(result, map[string]any{
			"severity": value.Severity, "type": value.NotificationType,
			"message": value.Message,
		})
	}
	return result
}

func replaySessionProvenance(value session.ReplaySession, virtualNow time.Time) map[string]any {
	visibleEnd := virtualNow.UTC()
	if visibleEnd.Before(value.DataStart) {
		visibleEnd = value.DataStart.UTC()
	}
	return map[string]any{
		"id":                 value.SessionID,
		"state":              value.Status,
		"started_at":         replayProvenanceTime(value.SessionStartedAt),
		"clock_mode":         value.ClockMode,
		"anchor_host":        replayProvenanceTime(value.AnchorHost),
		"anchor_virtual":     replayProvenanceTime(value.AnchorVirtual),
		"virtual_now":        replayProvenanceTime(virtualNow),
		"data_start":         replayProvenanceTime(value.DataStart),
		"data_end":           replayProvenanceTime(value.DataEnd),
		"visible_data_start": replayProvenanceTime(value.DataStart),
		"visible_data_end":   replayProvenanceTime(visibleEnd),
	}
}

func replaySources(values []execreplay.SourceCompilation, validated []execreplay.ValidatedResultContract) []map[string]any {
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		item := map[string]any{
			"ordinal":           value.Source.Ordinal,
			"path":              value.Source.Path,
			"class":             value.Source.Class,
			"name":              value.Source.Name,
			"boundary_policy":   value.Source.BoundaryPolicy,
			"record_time_field": value.Source.RecordTimeField,
			"overlap":           replayOverlapProof(value.Overlap),
			"physical_pending":  value.PhysicalPending,
		}
		if value.Requested != nil {
			item["requested_range"] = replayInterval(value.Requested.Range)
			item["requested_basis"] = value.Requested.Basis
		}
		if value.Effective != nil {
			item["logical_effective_range"] = replayInterval(*value.Effective)
		}
		if value.PhysicalRange != nil {
			item["physical_range"] = replayInterval(*value.PhysicalRange)
		}
		if actual, ok := replayValidatedSource(validated, value.Source.Ordinal); ok {
			if actual.PhysicalRange != nil {
				item["physical_range"] = replayInterval(*actual.PhysicalRange)
			}
			item["actual_natural_interval_ns"] = actual.NaturalInterval.Nanoseconds()
			item["maximum_lower_spill_ns"] = actual.LowerSpill.Nanoseconds()
			item["maximum_upper_spill_ns"] = actual.UpperSpill.Nanoseconds()
		}
		if mapping := value.DavisMapping; mapping != nil {
			item["davis_problems_mapping"] = replayDavisProblemsMapping(value.Source.Ordinal, *mapping)
		}
		result = append(result, item)
	}
	return result
}

func replayDavisProblemsMapping(ordinal int, value execreplay.DavisProblemsMappingCompilation) map[string]any {
	coverage := map[string]any{"coverage_verified": value.Coverage.Verified}
	if !value.Coverage.OldestSnapshot.IsZero() {
		coverage["oldest_snapshot"] = replayProvenanceTime(value.Coverage.OldestSnapshot)
	}
	if !value.Coverage.ObservedAt.IsZero() {
		coverage["observed_at"] = replayProvenanceTime(value.Coverage.ObservedAt)
	}
	return map[string]any{
		"eligible":                 true,
		"original_view":            value.Candidate.OriginalToken,
		"effective_snapshot_table": value.Candidate.SnapshotToken,
		"logical_view_range": map[string]string{
			"f": replayProvenanceTime(value.Logical.F), "t": replayProvenanceTime(value.Logical.T),
		},
		"physical_snapshot_range": map[string]string{
			"w": replayProvenanceTime(value.Physical.W), "t": replayProvenanceTime(value.Physical.T),
		},
		"warmup_clamped": value.WarmupClamped,
		"coverage":       coverage,
		"audit_expectation": map[string]any{
			"source_ordinal":   ordinal,
			"source_path":      value.Candidate.SourcePath,
			"data_object_path": value.Candidate.DataObjectPath,
			"original_token":   value.Candidate.OriginalToken,
			"effective_token":  value.Candidate.SnapshotToken,
			"logical_f":        replayProvenanceTime(value.Logical.F),
			"logical_t":        replayProvenanceTime(value.Logical.T),
			"physical_w":       replayProvenanceTime(value.Physical.W),
			"physical_t":       replayProvenanceTime(value.Physical.T),
		},
	}
}

func replayDavisCoverage(value DavisSnapshotCoverageProvenance) map[string]any {
	result := map[string]any{"status": value.Status, "coverage_verified": value.Verified}
	if !value.OldestSnapshot.IsZero() {
		result["oldest_snapshot"] = replayProvenanceTime(value.OldestSnapshot)
	}
	if !value.ObservedAt.IsZero() {
		result["observed_at"] = replayProvenanceTime(value.ObservedAt)
	}
	if value.Reuse != "" {
		result["reuse"] = value.Reuse
	}
	if value.Failure != "" {
		result["failure"] = value.Failure
	}
	return result
}

func replayValidatedSource(values []execreplay.ValidatedResultContract, ordinal int) (execreplay.ValidatedResultContract, bool) {
	for _, value := range values {
		if value.Source.Ordinal == ordinal {
			return value, true
		}
	}
	return execreplay.ValidatedResultContract{}, false
}

func replayOverlapProof(value execreplay.OverlapProof) map[string]any {
	result := map[string]any{
		"classification":       value.Classification,
		"reason":               value.Reason,
		"requested_now":        replayInterval(value.RequestedNow),
		"visible_now":          replayInterval(value.VisibleNow),
		"replay_interval":      replayInterval(value.ReplayInterval),
		"terminal_virtual_now": replayProvenanceTime(value.TerminalVirtualNow),
	}
	if value.TerminalRequested != nil {
		result["terminal_requested"] = replayInterval(*value.TerminalRequested)
	}
	return result
}

func replayNotices(values []execreplay.Notice) []map[string]any {
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		result = append(result, map[string]any{
			"kind": value.Kind, "code": value.Code, "message": value.Message,
			"source_ordinal": value.SourceOrdinal,
		})
	}
	return result
}

func replayValidatedContracts(values []execreplay.ValidatedResultContract) []map[string]any {
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		item := map[string]any{
			"source": map[string]any{
				"ordinal": value.Source.Ordinal, "path": value.Source.Path,
				"name": value.Source.Name, "shape": value.Source.Shape,
			},
			"logical_window":             replayInterval(value.LogicalWindow),
			"natural_interval_ns":        value.NaturalInterval.Nanoseconds(),
			"lower_spill_ns":             value.LowerSpill.Nanoseconds(),
			"upper_spill_ns":             value.UpperSpill.Nanoseconds(),
			"lower_boundary_buckets":     value.LowerBoundaryBuckets,
			"upper_boundary_buckets":     value.UpperBoundaryBuckets,
			"measurements_inspected":     value.MeasurementsInspected,
			"within_bucket_lookahead_ok": value.WithinBucketLookaheadOK,
		}
		if value.PhysicalRange != nil {
			item["physical_range"] = replayInterval(*value.PhysicalRange)
		}
		result = append(result, item)
	}
	return result
}

func replayInterval(value execreplay.Interval) map[string]string {
	return map[string]string{
		"start": replayProvenanceTime(value.Start),
		"end":   replayProvenanceTime(value.End),
	}
}

func replayProvenanceTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}
