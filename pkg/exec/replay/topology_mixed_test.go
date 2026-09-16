package replay

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCompileTopologyWidthPreservesOtherSourceClassifications(t *testing.T) {
	for _, test := range []struct {
		name           string
		fixture        string
		start          string
		now            string
		defaultStart   string
		defaultEnd     string
		wantRecord     OverlapClassification
		wantTopology   OverlapClassification
		wantRejections int
		wantQuery      OverlapClassification
		wantNarrow     bool
	}{
		{
			name: "temporary topology cannot hide permanently nonoverlapping logs", fixture: "old-nodes-control",
			start: "2026-08-10T11:00:00Z", now: "2026-08-10T11:01:20Z", defaultStart: "2026-08-10T11:01:00Z",
			defaultEnd: "2026-08-10T11:10:00Z", wantRecord: OverlapPermanent, wantTopology: OverlapTemporary, wantRejections: 2,
			wantQuery: OverlapPermanent,
		},
		{
			name: "overlapping logs cannot hide permanently narrow topology", fixture: "traverse-nested",
			start: "2026-08-10T11:00:00Z", now: "2026-08-10T11:01:20Z", defaultStart: "2026-08-10T11:01:00Z",
			defaultEnd: "2026-08-10T11:01:40Z", wantRecord: OverlapPresent, wantTopology: OverlapPermanent, wantRejections: 1,
			wantQuery: OverlapPermanent, wantNarrow: true,
		},
		{
			name: "temporary empty logs cannot hide permanently narrow topology", fixture: "old-nodes-control",
			start: "2026-08-10T09:00:00Z", now: "2026-08-10T09:01:20Z", defaultStart: "2026-08-10T09:01:00Z",
			defaultEnd: "2026-08-10T09:01:40Z", wantRecord: OverlapTemporary, wantTopology: OverlapPermanent, wantRejections: 2,
			wantQuery: OverlapPermanent, wantNarrow: true,
		},
		{
			name: "temporary empty logs and temporary narrow topology use ordinary retry", fixture: "old-nodes-control",
			start: "2026-08-10T09:00:00Z", now: "2026-08-10T09:01:20Z", defaultStart: "2026-08-10T09:01:00Z",
			defaultEnd: "2026-08-10T09:10:00Z", wantRecord: OverlapTemporary, wantTopology: OverlapTemporary, wantRejections: 2,
			wantQuery: OverlapTemporary,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Use the captured AST and original text without changing either.
			// Only session times and the normal request default vary.
			input := topologyFixtureInput(t, test.fixture)
			input.ReplayInterval = mustInterval(t, test.start, "2026-08-10T14:00:00Z")
			input.VirtualNow = mustTime(t, test.now)
			input.VirtualStart = input.VirtualNow
			input.VisibleInterval = Interval{Start: input.ReplayInterval.Start, End: input.VirtualNow}
			requested := mustInterval(t, test.defaultStart, test.defaultEnd)
			input.GlobalDefault = &requested

			result, err := Compile(input)
			var classified *NonOverlapError
			if !errors.As(err, &classified) || classified.Classification != test.wantQuery || (classified.NarrowWindow != nil) != test.wantNarrow ||
				len(classified.Sources) != test.wantRejections || result.AuditRequired || result.EffectiveDQL != "" {
				t.Fatalf("result=%#v error=%v classification=%#v", result, err, classified)
			}
			if !test.wantNarrow {
				if err.Error() != "The requested source timeframe does not overlap the currently visible replay interval.\nThe query was not executed." || classified.RetryMessage() != "" {
					t.Fatalf("ordinary non-overlap error=%v retry=%q", err, classified.RetryMessage())
				}
			}
			if len(result.Sources) != 2 || len(result.Explain.Sources) != 2 {
				t.Fatalf("not every source was classified: %#v", result.Sources)
			}
			record, topology := result.Sources[0], result.Sources[1]
			if record.Source.Name != "logs" || record.Overlap.Classification != test.wantRecord ||
				topology.Source.Class != SourceTopology || topology.Overlap.Classification != test.wantTopology {
				t.Fatalf("record=%#v topology=%#v", record, topology)
			}
			if topology.Effective == nil || topology.Effective.Start != requested.Start || topology.Effective.End != input.VirtualNow ||
				topology.Effective.End.Sub(topology.Effective.Start) != 20*time.Second || topology.PhysicalRange == nil ||
				*topology.PhysicalRange != *topology.Effective || topology.Overlap.Reason == "" {
				t.Fatalf("narrow topology evidence was lost or widened: %#v", topology)
			}
		})
	}
}

func TestDecidingNarrowWindowLattice(t *testing.T) {
	for _, otherClass := range []OverlapClassification{OverlapPresent, OverlapTemporary, OverlapPermanent, OverlapUnknown} {
		for _, narrowClass := range []OverlapClassification{OverlapTemporary, OverlapPermanent, OverlapUnknown} {
			for _, narrowFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("other=%s/topology=%s/narrow-first=%t", otherClass, narrowClass, narrowFirst), func(t *testing.T) {
					window := mustInterval(t, "2026-08-10T11:01:00Z", "2026-08-10T11:01:20Z")
					narrow := SourceExplain{Ordinal: 1, Class: SourceTopology, Effective: &window, Classification: narrowClass}
					other := SourceExplain{Ordinal: 0, Class: SourceRecord, Classification: otherClass}
					sources := []SourceExplain{other, narrow}
					if narrowFirst {
						sources = []SourceExplain{narrow, other}
					}
					decision := classifyWholeQueryNonOverlap(sources)
					cause := &ReplayError{Code: ErrorTimeframe}
					decision.NarrowWindow = decidingNarrowWindow(decision, map[int]*ReplayError{narrow.Ordinal: cause})
					wantNarrow := otherClass == OverlapPresent || (otherClass == OverlapTemporary && narrowClass != OverlapTemporary)
					if (decision.NarrowWindow == cause) != wantNarrow {
						t.Fatalf("decision=%#v, want narrow cause=%t", decision, wantNarrow)
					}
					if wantNarrow {
						if retry := decision.RetryMessage(); !strings.Contains(retry, "topology window") || !strings.Contains(retry, window.Start.Format(time.RFC3339Nano)) {
							t.Fatalf("retry notice lost narrow topology source: %q", retry)
						}
					} else if retry := decision.RetryMessage(); retry != "" {
						t.Fatalf("ordinary decision has topology retry notice: %q", retry)
					}
				})
			}
		}
	}
}

func TestDecidingNarrowWindowUsesFirstDecidingSource(t *testing.T) {
	for _, classification := range []OverlapClassification{OverlapTemporary, OverlapPermanent, OverlapUnknown} {
		t.Run(string(classification), func(t *testing.T) {
			first, second := &ReplayError{Code: ErrorTimeframe}, &ReplayError{Code: ErrorTimeframe}
			sources := []SourceExplain{
				{Ordinal: 0, Classification: OverlapTemporary},
				{Ordinal: 1, Classification: classification},
				{Ordinal: 2, Classification: classification},
			}
			causes := map[int]*ReplayError{0: first, 1: second, 2: &ReplayError{Code: ErrorTimeframe}}
			want := first
			if classification != OverlapTemporary {
				want = second
			}
			if got := decidingNarrowWindow(classifyWholeQueryNonOverlap(sources), causes); got != want {
				t.Fatalf("cause=%p, want first deciding source cause=%p", got, want)
			}
		})
	}
}
