package replay

import (
	"errors"
	"testing"
	"time"
)

func TestResultValidatorAcceptsOneNaturalBucketPerBoundary(t *testing.T) {
	contract := testMetricContract(time.Hour)
	buckets := []MetricBucket{
		{Range: mustResultInterval(t, "2026-06-14T10:00:00Z", "2026-06-14T11:00:00Z")},
		{Range: mustResultInterval(t, "2026-06-14T11:00:00Z", "2026-06-14T12:00:00Z")},
	}
	provenance := provenanceFor(contract, time.Hour, buckets)
	validated, err := ValidateResultContract(contract, ObservedResultMetadata{
		Source: contract.Source, NaturalInterval: time.Hour, Buckets: buckets, Provenance: provenance,
	})
	if err != nil {
		t.Fatal(err)
	}
	if validated.LowerBoundaryBuckets != 1 || validated.UpperBoundaryBuckets != 1 ||
		validated.LowerSpill != 15*time.Minute || validated.UpperSpill != 15*time.Minute {
		t.Fatalf("validated = %#v", validated)
	}
	if validated.MeasurementsInspected || !validated.WithinBucketLookaheadOK {
		t.Fatalf("validator overclaimed measurement inspection: %#v", validated)
	}
}

func TestResultValidatorAcceptsEmptyMetricResultWithKnownInterval(t *testing.T) {
	contract := testMetricContract(time.Hour)
	provenance := ResultProvenance{
		Source: contract.Source, LogicalWindow: contract.LogicalWindow,
		NaturalInterval: time.Hour, BoundaryPolicy: BoundaryMetricBucket,
	}
	validated, err := ValidateResultContract(contract, ObservedResultMetadata{
		Source: contract.Source, NaturalInterval: time.Hour, Provenance: provenance,
	})
	if err != nil {
		t.Fatal(err)
	}
	if validated.PhysicalRange != nil || validated.LowerSpill != 0 || validated.UpperSpill != 0 {
		t.Fatalf("validated empty result = %#v", validated)
	}
}

func TestResultValidatorRejectsContractViolations(t *testing.T) {
	contract := testMetricContract(time.Hour)
	validBuckets := []MetricBucket{
		{Range: mustResultInterval(t, "2026-06-14T10:00:00Z", "2026-06-14T11:00:00Z")},
		{Range: mustResultInterval(t, "2026-06-14T11:00:00Z", "2026-06-14T12:00:00Z")},
	}
	tests := []struct {
		name     string
		observed ObservedResultMetadata
	}{
		{
			name: "incomplete pre-execution contract",
			observed: ObservedResultMetadata{Source: contract.Source, NaturalInterval: time.Hour,
				Provenance: provenanceFor(contract, time.Hour, nil)},
		},
		{
			name: "unknown natural interval",
			observed: ObservedResultMetadata{Source: contract.Source, NaturalInterval: 0,
				Provenance: provenanceFor(contract, 0, nil)},
		},
		{
			name: "declared interval mismatch",
			observed: ObservedResultMetadata{Source: contract.Source, NaturalInterval: 5 * time.Minute,
				Provenance: provenanceFor(contract, 5*time.Minute, nil)},
		},
		{
			name: "source identity mismatch",
			observed: ObservedResultMetadata{Source: ResultSourceIdentity{Ordinal: 99}, NaturalInterval: time.Hour,
				Provenance: provenanceFor(contract, time.Hour, nil)},
		},
		{
			name: "bucket outside logical window",
			observed: ObservedResultMetadata{Source: contract.Source, NaturalInterval: time.Hour,
				Buckets:    []MetricBucket{{Range: mustResultInterval(t, "2026-06-14T09:00:00Z", "2026-06-14T10:00:00Z")}},
				Provenance: provenanceFor(contract, time.Hour, nil)},
		},
		{
			name: "two lower boundary buckets",
			observed: ObservedResultMetadata{Source: contract.Source, NaturalInterval: time.Hour,
				Buckets: []MetricBucket{
					{Range: mustResultInterval(t, "2026-06-14T09:30:00Z", "2026-06-14T10:30:00Z")},
					{Range: mustResultInterval(t, "2026-06-14T10:00:00Z", "2026-06-14T11:00:00Z")},
				}, Provenance: provenanceFor(contract, time.Hour, nil)},
		},
		{
			name: "bucket duration mismatch",
			observed: ObservedResultMetadata{Source: contract.Source, NaturalInterval: time.Hour,
				Buckets:    []MetricBucket{{Range: mustResultInterval(t, "2026-06-14T10:00:00Z", "2026-06-14T10:30:00Z")}},
				Provenance: provenanceFor(contract, time.Hour, nil)},
		},
		{
			name: "provenance mismatch",
			observed: ObservedResultMetadata{Source: contract.Source, NaturalInterval: time.Hour,
				Buckets: validBuckets, Provenance: ResultProvenance{
					Source: contract.Source, LogicalWindow: contract.LogicalWindow,
					NaturalInterval: time.Hour, BoundaryPolicy: BoundaryMetricBucket,
				}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := contract
			if test.name == "incomplete pre-execution contract" {
				candidate.NaturalIntervalRequired = false
			}
			_, err := ValidateResultContract(candidate, test.observed)
			var contractErr *ResultContractError
			if !errors.As(err, &contractErr) || contractErr.Reason == "" {
				t.Fatalf("error = %T %v, want ResultContractError", err, err)
			}
		})
	}
}

func TestResultValidatorSupportsFixedTwentyFourHourNaturalBucket(t *testing.T) {
	contract := testMetricContract(24 * time.Hour)
	contract.LogicalWindow = mustResultInterval(t, "2026-06-14T06:00:00Z", "2026-06-15T18:00:00Z")
	buckets := []MetricBucket{
		{Range: mustResultInterval(t, "2026-06-14T00:00:00Z", "2026-06-15T00:00:00Z")},
		{Range: mustResultInterval(t, "2026-06-15T00:00:00Z", "2026-06-16T00:00:00Z")},
	}
	_, err := ValidateResultContract(contract, ObservedResultMetadata{
		Source: contract.Source, NaturalInterval: 24 * time.Hour, Buckets: buckets,
		Provenance: provenanceFor(contract, 24*time.Hour, buckets),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testMetricContract(interval time.Duration) ReplayResultContract {
	return ReplayResultContract{
		Source:         ResultSourceIdentity{Ordinal: 0, Path: "root.children[0]", Name: "timeseries", Shape: MetricBaseline},
		LogicalWindow:  mustStaticInterval("2026-06-14T10:15:00Z", "2026-06-14T11:45:00Z"),
		BoundaryPolicy: BoundaryMetricBucket, DeclaredNaturalInterval: &interval, NaturalIntervalRequired: true,
	}
}

func provenanceFor(contract ReplayResultContract, interval time.Duration, buckets []MetricBucket) ResultProvenance {
	var physical *Interval
	for _, bucket := range buckets {
		physical = extendPhysicalRange(physical, bucket.Range)
	}
	provenance := ResultProvenance{
		Source: contract.Source, LogicalWindow: contract.LogicalWindow, PhysicalRange: cloneInterval(physical),
		NaturalInterval: interval, BoundaryPolicy: contract.BoundaryPolicy,
	}
	if physical != nil && physical.Start.Before(contract.LogicalWindow.Start) {
		provenance.LowerSpill = contract.LogicalWindow.Start.Sub(physical.Start)
	}
	if physical != nil && physical.End.After(contract.LogicalWindow.End) {
		provenance.UpperSpill = physical.End.Sub(contract.LogicalWindow.End)
	}
	return provenance
}

func mustStaticInterval(start, end string) Interval {
	from, err := time.Parse(time.RFC3339Nano, start)
	if err != nil {
		panic(err)
	}
	to, err := time.Parse(time.RFC3339Nano, end)
	if err != nil {
		panic(err)
	}
	return Interval{Start: from, End: to}
}

func mustResultInterval(t *testing.T, start, end string) Interval {
	t.Helper()
	return Interval{Start: mustResultTime(t, start), End: mustResultTime(t, end)}
}

func mustResultTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
