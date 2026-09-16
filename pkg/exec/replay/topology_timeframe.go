package replay

import (
	"fmt"
	"time"
)

// ClassifyTopologyWidth proves whether a currently sub-minute topology window
// can reach the minimum width before replay ends. Empty intersections continue
// to use ClassifyOverlap instead.
func ClassifyTopologyWidth(requested RequestedRange, visible, replay Interval, virtualNow time.Time) OverlapProof {
	proof := OverlapProof{
		RequestedNow: requested.Range, VisibleNow: visible, ReplayInterval: replay,
		TerminalVirtualNow: replay.End,
	}
	terminal, known := requestedRangeAt(requested, replay.End)
	if !known {
		proof.Classification = OverlapUnknown
		proof.Reason = "the source range uses time semantics whose future width is not proven"
		return proof
	}
	proof.TerminalRequested = &terminal
	if !virtualNow.Before(replay.End) {
		proof.Classification = OverlapPermanent
		proof.Reason = "virtual now is at data_end, so the effective topology window cannot reach 60 seconds later"
		return proof
	}

	// width(t) = min(to(t), t) - max(from(t), data_start). For proven
	// absolute/relative endpoints it is affine between these breakpoints.
	// A relative start with an absolute end can grow and then shrink, so
	// testing data_end alone would miss its interior maximum.
	candidates := []time.Time{replay.End}
	if requested.From.Dependency == EndpointRelative {
		// Split the subtraction to handle even the minimum time.Duration.
		crossing := replay.Start.Add(-(requested.From.Offset + 1)).Add(time.Nanosecond)
		candidates = append(candidates, crossing)
	}
	if requested.To.Dependency == EndpointAbsolute {
		candidates = append(candidates, requested.To.Value)
	}
	for _, candidate := range candidates {
		if !candidate.After(virtualNow) || candidate.After(replay.End) {
			continue
		}
		future, _ := requestedRangeAt(requested, candidate)
		effective, ok := future.Intersect(Interval{Start: replay.Start, End: candidate})
		if ok && effective.End.Sub(effective.Start) >= topologyMinimumWindow {
			proof.Classification = OverlapTemporary
			proof.Reason = fmt.Sprintf("the effective topology window is proven to reach at least 60 seconds at virtual time %s: [%s, %s)",
				candidate.Format(time.RFC3339Nano), effective.Start.Format(time.RFC3339Nano), effective.End.Format(time.RFC3339Nano))
			return proof
		}
	}
	proof.Classification = OverlapPermanent
	proof.Reason = "the effective topology window is proven to stay under 60 seconds through data_end at every absolute/relative endpoint breakpoint"
	return proof
}
