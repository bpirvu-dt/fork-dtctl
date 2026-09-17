package replay

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestClassifyTopologyWidthEndpointProofs(t *testing.T) {
	base := time.Date(2026, 6, 14, 8, 0, 0, 0, time.UTC)
	type endpointModel struct {
		dependency EndpointDependency
		offset     time.Duration
	}
	abs := func(offset time.Duration) endpointModel { return endpointModel{EndpointAbsolute, offset} }
	rel := func(offset time.Duration) endpointModel { return endpointModel{EndpointRelative, offset} }
	tests := []struct {
		name     string
		from, to endpointModel
		now, end time.Duration
		want     OverlapClassification
	}{
		{"empty intersection future thirty seconds", abs(120 * time.Second), abs(150 * time.Second), 90 * time.Second, 600 * time.Second, OverlapPermanent},
		{"empty intersection future wide window", abs(120 * time.Second), abs(300 * time.Second), 90 * time.Second, 600 * time.Second, OverlapTemporary},
		{"empty intersection unknown start", endpointModel{EndpointUnknown, 120 * time.Second}, abs(300 * time.Second), 90 * time.Second, 600 * time.Second, OverlapUnknown},
		{"absolute absolute widens", abs(70 * time.Second), abs(700 * time.Second), 90 * time.Second, 600 * time.Second, OverlapTemporary},
		{"absolute fixed subminute span", abs(70 * time.Second), abs(110 * time.Second), 90 * time.Second, 600 * time.Second, OverlapPermanent},
		{"absolute exact minimum", abs(70 * time.Second), abs(130 * time.Second), 90 * time.Second, 600 * time.Second, OverlapTemporary},
		{"absolute nanosecond below minimum", abs(70 * time.Second), abs(130*time.Second - time.Nanosecond), 90 * time.Second, 600 * time.Second, OverlapPermanent},
		{"absolute replay end clips to minimum", abs(70 * time.Second), abs(700 * time.Second), 90 * time.Second, 130 * time.Second, OverlapTemporary},
		{"absolute replay end clips below minimum", abs(70 * time.Second), abs(700 * time.Second), 90 * time.Second, 130*time.Second - time.Nanosecond, OverlapPermanent},
		{"absolute relative widens", abs(70 * time.Second), rel(0), 90 * time.Second, 130 * time.Second, OverlapTemporary},
		{"absolute relative negative end widens", abs(40 * time.Second), rel(-30 * time.Second), 90 * time.Second, 130 * time.Second, OverlapTemporary},
		{"absolute relative positive end clips to visible", abs(70 * time.Second), rel(30 * time.Second), 90 * time.Second, 130 * time.Second, OverlapTemporary},
		{"absolute relative replay end clips below minimum", abs(70 * time.Second), rel(30 * time.Second), 90 * time.Second, 130*time.Second - time.Nanosecond, OverlapPermanent},
		{"relative absolute interior peak grows then vanishes", rel(-60 * time.Second), abs(120 * time.Second), 20 * time.Second, 600 * time.Second, OverlapTemporary},
		{"relative absolute coincident breakpoints reach exact minimum", rel(-60 * time.Second), abs(60 * time.Second), 20 * time.Second, 600 * time.Second, OverlapTemporary},
		{"relative absolute interior peak below minimum", rel(-60*time.Second + time.Nanosecond), abs(120 * time.Second), 20 * time.Second, 600 * time.Second, OverlapPermanent},
		{"relative absolute end breakpoint before start breakpoint", rel(-120 * time.Second), abs(90 * time.Second), 20 * time.Second, 600 * time.Second, OverlapTemporary},
		{"relative absolute peak at replay end", rel(-60 * time.Second), abs(60 * time.Second), 20 * time.Second, 60 * time.Second, OverlapTemporary},
		{"relative absolute peak after replay end", rel(-60 * time.Second), abs(60 * time.Second), 20 * time.Second, 60*time.Second - time.Nanosecond, OverlapPermanent},
		{"relative absolute start breakpoint at now", rel(-20 * time.Second), abs(120 * time.Second), 20 * time.Second, 600 * time.Second, OverlapPermanent},
		{"relative absolute end breakpoint at now", rel(-20 * time.Second), abs(90 * time.Second), 90 * time.Second, 600 * time.Second, OverlapPermanent},
		{"relative relative exact span widens", rel(-90 * time.Second), rel(-30 * time.Second), 50 * time.Second, 600 * time.Second, OverlapTemporary},
		{"relative relative positive end clips to visible", rel(-60 * time.Second), rel(30 * time.Second), 20 * time.Second, 600 * time.Second, OverlapTemporary},
		{"relative relative five second span", rel(-5 * time.Second), rel(0), 90 * time.Second, 600 * time.Second, OverlapPermanent},
		{"relative relative nanosecond below minimum", rel(-90 * time.Second), rel(-30*time.Second - time.Nanosecond), 50 * time.Second, 600 * time.Second, OverlapPermanent},
		{"terminal absolute window", abs(70 * time.Second), abs(700 * time.Second), 90 * time.Second, 90 * time.Second, OverlapPermanent},
		{"unknown start", endpointModel{EndpointUnknown, 70 * time.Second}, abs(700 * time.Second), 90 * time.Second, 600 * time.Second, OverlapUnknown},
		{"unknown end", abs(70 * time.Second), endpointModel{EndpointUnknown, 700 * time.Second}, 90 * time.Second, 600 * time.Second, OverlapUnknown},
		{"unrecognized dependency", endpointModel{"aligned", 70 * time.Second}, rel(0), 90 * time.Second, 600 * time.Second, OverlapUnknown},
		{"terminal unknown remains unproven", endpointModel{EndpointUnknown, 70 * time.Second}, abs(700 * time.Second), 90 * time.Second, 90 * time.Second, OverlapUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := base.Add(tt.now)
			endpoint := func(model endpointModel) TimeEndpoint {
				value := base.Add(model.offset)
				if model.dependency == EndpointRelative {
					value = now.Add(model.offset)
				}
				return TimeEndpoint{Value: value, Dependency: model.dependency, Offset: model.offset}
			}
			from, to := endpoint(tt.from), endpoint(tt.to)
			requested := RequestedRange{Range: Interval{Start: from.Value, End: to.Value}, From: from, To: to}
			replay := Interval{Start: base, End: base.Add(tt.end)}
			visible := Interval{Start: base, End: now}
			effective, ok := requested.Range.Intersect(visible)
			if strings.HasPrefix(tt.name, "empty intersection ") {
				if ok {
					t.Fatalf("test must start with an empty intersection: %+v", effective)
				}
			} else if !ok || effective.End.Sub(effective.Start) >= time.Minute {
				t.Fatalf("test must start with a non-empty sub-minute window: %+v", effective)
			}
			proof := ClassifyTopologyWidth(requested, visible, replay, now)
			if proof.Classification != tt.want || proof.Reason == "" {
				t.Fatalf("proof = %+v, want %s with a reason", proof, tt.want)
			}
			if tt.want == OverlapUnknown && (!strings.Contains(proof.Reason, "not proven") || proof.TerminalRequested != nil) {
				t.Fatalf("unknown dependency requires an unproven range, even at data_end: %+v", proof)
			}
			if proof.RequestedNow != requested.Range || proof.VisibleNow != visible || proof.ReplayInterval != replay || !proof.TerminalVirtualNow.Equal(replay.End) {
				t.Fatalf("proof omitted or changed its inputs: %+v", proof)
			}
			if now.Before(replay.End) && tt.want != OverlapUnknown {
				terminal, known := requestedRangeAt(requested, replay.End)
				if !known || proof.TerminalRequested == nil || *proof.TerminalRequested != terminal {
					t.Fatalf("proof omitted the terminal range: %+v", proof)
				}
			}
		})
	}
}

func TestClassifyTopologyWidthDurationExtremes(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	const minDuration = time.Duration(-1 << 63)
	base := time.Date(2026, 6, 14, 8, 0, 0, 0, time.UTC)
	// The relative start crosses data_start at a distance one nanosecond
	// greater than MaxInt64. That instant is representable by time.Time.
	crossing := base.Add(maxDuration).Add(time.Nanosecond)
	now := crossing.Add(-40 * time.Second)
	for _, span := range []time.Duration{time.Minute, time.Minute - time.Nanosecond} {
		t.Run("minimum offset span "+span.String(), func(t *testing.T) {
			from, to := relativeEndpoint(now, minDuration), relativeEndpoint(now, minDuration+span)
			requested := RequestedRange{Range: Interval{Start: from.Value, End: to.Value}, From: from, To: to}
			visible := Interval{Start: base, End: now}
			replay := Interval{Start: base, End: crossing.Add(time.Minute)}
			want := OverlapPermanent
			if span == time.Minute {
				want = OverlapTemporary
			}
			if proof := ClassifyTopologyWidth(requested, visible, replay, now); proof.Classification != want {
				t.Fatalf("proof = %+v, want %s", proof, want)
			}
		})
	}
	t.Run("maximum positive end offset stays clipped", func(t *testing.T) {
		now := base.Add(90 * time.Second)
		from := TimeEndpoint{Value: base.Add(70 * time.Second), Dependency: EndpointAbsolute}
		to := relativeEndpoint(now, maxDuration)
		requested := RequestedRange{Range: Interval{Start: from.Value, End: to.Value}, From: from, To: to}
		visible := Interval{Start: base, End: now}
		for _, end := range []time.Duration{130 * time.Second, 130*time.Second - time.Nanosecond} {
			replay := Interval{Start: base, End: base.Add(end)}
			want := OverlapPermanent
			if end == 130*time.Second {
				want = OverlapTemporary
			}
			if proof := ClassifyTopologyWidth(requested, visible, replay, now); proof.Classification != want {
				t.Fatalf("end=%s: proof = %+v, want %s", end, proof, want)
			}
		}
	})
}

func TestClassifyTopologyWidthMatchesEnumeratedFutureWindows(t *testing.T) {
	base := time.Date(2026, 6, 14, 8, 0, 0, 0, time.UTC)
	now := base.Add(20 * time.Second)
	replay := Interval{Start: base, End: base.Add(240 * time.Second)}
	visible := Interval{Start: base, End: now}
	offsets := []time.Duration{-120, -61, -60, -59, -20, 0, 20, 59, 60, 61, 120, 240}
	checked, emptyChecked := 0, 0
	for _, fromKind := range []EndpointDependency{EndpointAbsolute, EndpointRelative} {
		for _, toKind := range []EndpointDependency{EndpointAbsolute, EndpointRelative} {
			for _, fromSeconds := range offsets {
				for _, toSeconds := range offsets {
					from := TimeEndpoint{Value: base.Add(fromSeconds * time.Second), Dependency: fromKind, Offset: fromSeconds * time.Second}
					to := TimeEndpoint{Value: base.Add(toSeconds * time.Second), Dependency: toKind, Offset: toSeconds * time.Second}
					if fromKind == EndpointRelative {
						from.Value = now.Add(from.Offset)
					}
					if toKind == EndpointRelative {
						to.Value = now.Add(to.Offset)
					}
					requested := RequestedRange{Range: Interval{Start: from.Value, End: to.Value}, From: from, To: to}
					if !requested.Range.Valid() {
						continue
					}
					effective, ok := requested.Range.Intersect(visible)
					if ok && effective.End.Sub(effective.Start) >= time.Minute {
						continue
					}
					checked++
					if !ok {
						emptyChecked++
					}
					// Every model changes slope at whole seconds. Enumerating
					// all seconds therefore includes every possible maximum,
					// independently of the classifier's breakpoint selection.
					want := OverlapPermanent
					for future := now.Add(time.Second); !future.After(replay.End); future = future.Add(time.Second) {
						start, end := from.Value, to.Value
						if fromKind == EndpointRelative {
							start = future.Add(from.Offset)
						}
						if toKind == EndpointRelative {
							end = future.Add(to.Offset)
						}
						if start.Before(base) {
							start = base
						}
						if end.After(future) {
							end = future
						}
						if end.Sub(start) >= time.Minute {
							want = OverlapTemporary
							break
						}
					}
					t.Run(fmt.Sprintf("%s_%s/from=%s/to=%s/empty=%t", fromKind, toKind, from.Offset, to.Offset, !ok), func(t *testing.T) {
						if proof := ClassifyTopologyWidth(requested, visible, replay, now); proof.Classification != want {
							t.Fatalf("proof = %+v, enumerated future windows prove %s", proof, want)
						}
					})
				}
			}
		}
	}
	if checked == 0 || emptyChecked == 0 || checked == emptyChecked {
		t.Fatalf("need empty and non-empty sub-minute models: checked=%d empty=%d", checked, emptyChecked)
	}
}

func TestTemporaryTopologyWidthDoesNotHideAnotherSourceHardFailure(t *testing.T) {
	base := time.Date(2026, 6, 14, 11, 0, 0, 0, time.UTC)
	effective := Interval{Start: base, End: base.Add(20 * time.Second)}
	narrow := SourceExplain{
		Ordinal: 1, Class: SourceTopology, Name: "smartscapeNodes", Effective: &effective,
		Classification: OverlapTemporary,
		Proof:          OverlapProof{Classification: OverlapTemporary, Reason: "the effective window will reach 60 seconds"},
	}
	for _, otherClass := range []OverlapClassification{OverlapPresent, OverlapTemporary, OverlapPermanent, OverlapUnknown, "unrecognized"} {
		for _, narrowFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("other=%s/narrow-first=%t", otherClass, narrowFirst), func(t *testing.T) {
				other := SourceExplain{Ordinal: 2, Classification: otherClass}
				sources := []SourceExplain{other, narrow}
				if narrowFirst {
					sources = []SourceExplain{narrow, other}
				}
				want := otherClass
				if otherClass == OverlapPresent || otherClass == OverlapTemporary {
					want = OverlapTemporary
				} else if otherClass == "unrecognized" {
					want = OverlapUnknown
				}
				result := classifyWholeQueryNonOverlap(sources)
				if result == nil || result.Classification != want {
					t.Fatalf("whole-query result = %+v, want %s", result, want)
				}
				found := false
				for _, source := range result.Sources {
					if source.Ordinal == narrow.Ordinal {
						found = true
						if source.Effective == nil || *source.Effective != effective || source.Proof != narrow.Proof {
							t.Fatalf("narrow-window evidence changed: %+v", source)
						}
					}
				}
				if !found {
					t.Fatal("narrow-window source omitted from whole-query result")
				}
			})
		}
	}
}
