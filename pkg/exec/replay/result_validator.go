package replay

import (
	"fmt"
	"time"
)

// MetricBucket is neutral result metadata for one returned natural bucket. It
// contains placement only, never measurements or aggregate values.
type MetricBucket struct {
	Range Interval
}

// ResultProvenance is the placement record that the caller intends to expose
// or persist. The validator requires it to match independently computed facts.
type ResultProvenance struct {
	Source          ResultSourceIdentity
	LogicalWindow   Interval
	PhysicalRange   *Interval
	NaturalInterval time.Duration
	LowerSpill      time.Duration
	UpperSpill      time.Duration
	BoundaryPolicy  BoundaryPolicy
}

// ObservedResultMetadata is supplied by Phase 4 after execution. Buckets must
// enumerate the returned metric bucket ranges for this source.
type ObservedResultMetadata struct {
	Source          ResultSourceIdentity
	NaturalInterval time.Duration
	Buckets         []MetricBucket
	Provenance      ResultProvenance
}

// ValidatedResultContract contains placement facts only. The explicit false
// field prevents callers from implying that values within a bucket were
// inspected.
type ValidatedResultContract struct {
	Source                  ResultSourceIdentity
	LogicalWindow           Interval
	PhysicalRange           *Interval
	NaturalInterval         time.Duration
	LowerSpill              time.Duration
	UpperSpill              time.Duration
	LowerBoundaryBuckets    int
	UpperBoundaryBuckets    int
	MeasurementsInspected   bool
	WithinBucketLookaheadOK bool
}

// ResultContractError is a typed compatibility failure. Normal replay output
// must be suppressed when this error is returned.
type ResultContractError struct {
	Source ResultSourceIdentity
	Reason string
}

func (e *ResultContractError) Error() string {
	return fmt.Sprintf("The returned metric data violates the replay result contract for source %d: %s\nNo result was returned.", e.Source.Ordinal, e.Reason)
}

// ValidateResultContract validates bucket placement and provenance only. It
// cannot inspect which stored measurements contributed to an aggregate.
func ValidateResultContract(contract ReplayResultContract, observed ObservedResultMetadata) (ValidatedResultContract, error) {
	if contract.BoundaryPolicy != BoundaryMetricBucket || !contract.LogicalWindow.Valid() || !contract.NaturalIntervalRequired {
		return ValidatedResultContract{}, resultError(contract.Source, "the pre-execution metric contract is invalid")
	}
	if observed.Source != contract.Source {
		return ValidatedResultContract{}, resultError(contract.Source, "the observed source identity does not match the compiled source")
	}
	if contract.NaturalIntervalRequired && observed.NaturalInterval <= 0 {
		return ValidatedResultContract{}, resultError(contract.Source, "the natural metric interval is unknown")
	}
	if contract.DeclaredNaturalInterval != nil && observed.NaturalInterval != *contract.DeclaredNaturalInterval {
		return ValidatedResultContract{}, resultError(contract.Source, fmt.Sprintf("the observed natural interval %s does not match the declared interval %s", observed.NaturalInterval, *contract.DeclaredNaturalInterval))
	}
	validated := ValidatedResultContract{
		Source: contract.Source, LogicalWindow: contract.LogicalWindow,
		NaturalInterval: observed.NaturalInterval, MeasurementsInspected: false,
		WithinBucketLookaheadOK: true,
	}
	for index, bucket := range observed.Buckets {
		if !bucket.Range.Valid() {
			return ValidatedResultContract{}, resultError(contract.Source, fmt.Sprintf("bucket %d is empty or reversed", index))
		}
		if bucket.Range.End.Sub(bucket.Range.Start) != observed.NaturalInterval {
			return ValidatedResultContract{}, resultError(contract.Source, fmt.Sprintf("bucket %d does not have the observed natural interval", index))
		}
		if _, intersects := bucket.Range.Intersect(contract.LogicalWindow); !intersects {
			return ValidatedResultContract{}, resultError(contract.Source, fmt.Sprintf("bucket %d does not intersect the logical window", index))
		}
		if bucket.Range.Start.Before(contract.LogicalWindow.Start) {
			validated.LowerBoundaryBuckets++
		}
		if bucket.Range.End.After(contract.LogicalWindow.End) {
			validated.UpperBoundaryBuckets++
		}
		validated.PhysicalRange = extendPhysicalRange(validated.PhysicalRange, bucket.Range)
	}
	if validated.LowerBoundaryBuckets > 1 || validated.UpperBoundaryBuckets > 1 {
		return ValidatedResultContract{}, resultError(contract.Source, "more than one natural bucket spills across a logical boundary")
	}
	if validated.PhysicalRange != nil {
		if validated.PhysicalRange.Start.Before(contract.LogicalWindow.Start) {
			validated.LowerSpill = contract.LogicalWindow.Start.Sub(validated.PhysicalRange.Start)
		}
		if validated.PhysicalRange.End.After(contract.LogicalWindow.End) {
			validated.UpperSpill = validated.PhysicalRange.End.Sub(contract.LogicalWindow.End)
		}
	}
	if validated.LowerSpill > observed.NaturalInterval || validated.UpperSpill > observed.NaturalInterval {
		return ValidatedResultContract{}, resultError(contract.Source, "boundary spill exceeds one complete natural interval")
	}
	expectedProvenance := ResultProvenance{
		Source: contract.Source, LogicalWindow: contract.LogicalWindow,
		PhysicalRange: cloneInterval(validated.PhysicalRange), NaturalInterval: observed.NaturalInterval,
		LowerSpill: validated.LowerSpill, UpperSpill: validated.UpperSpill,
		BoundaryPolicy: contract.BoundaryPolicy,
	}
	if !equalResultProvenance(observed.Provenance, expectedProvenance) {
		return ValidatedResultContract{}, resultError(contract.Source, "the reported provenance does not match the observed interval and bounds")
	}
	return validated, nil
}

func extendPhysicalRange(current *Interval, bucket Interval) *Interval {
	if current == nil {
		return cloneInterval(&bucket)
	}
	if bucket.Start.Before(current.Start) {
		current.Start = bucket.Start
	}
	if bucket.End.After(current.End) {
		current.End = bucket.End
	}
	return current
}

func equalResultProvenance(left, right ResultProvenance) bool {
	return left.Source == right.Source && equalInterval(left.LogicalWindow, right.LogicalWindow) &&
		equalOptionalInterval(left.PhysicalRange, right.PhysicalRange) &&
		left.NaturalInterval == right.NaturalInterval && left.LowerSpill == right.LowerSpill &&
		left.UpperSpill == right.UpperSpill && left.BoundaryPolicy == right.BoundaryPolicy
}

func equalOptionalInterval(left, right *Interval) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return equalInterval(*left, *right)
}

func equalInterval(left, right Interval) bool {
	return left.Start.Equal(right.Start) && left.End.Equal(right.End)
}

func resultError(source ResultSourceIdentity, reason string) *ResultContractError {
	return &ResultContractError{Source: source, Reason: reason}
}
