package replay

import (
	"errors"
	"testing"
	"time"
)

func TestCompileTopologyWidthPreservesOtherSourceClassifications(t *testing.T) {
	for _, test := range []struct {
		name           string
		fixture        string
		defaultEnd     string
		wantRecord     OverlapClassification
		wantTopology   OverlapClassification
		wantRejections int
	}{
		{
			name: "temporary topology cannot hide permanently nonoverlapping logs", fixture: "old-nodes-control",
			defaultEnd: "2026-08-10T11:10:00Z", wantRecord: OverlapPermanent, wantTopology: OverlapTemporary, wantRejections: 2,
		},
		{
			name: "overlapping logs cannot hide permanently narrow topology", fixture: "traverse-nested",
			defaultEnd: "2026-08-10T11:01:40Z", wantRecord: OverlapPresent, wantTopology: OverlapPermanent, wantRejections: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Use the captured AST and original text without changing either.
			// Only session times and the normal request default vary.
			input := topologyFixtureInput(t, test.fixture)
			input.ReplayInterval = mustInterval(t, "2026-08-10T11:00:00Z", "2026-08-10T14:00:00Z")
			input.VirtualNow = mustTime(t, "2026-08-10T11:01:20Z")
			input.VirtualStart = input.VirtualNow
			input.VisibleInterval = Interval{Start: input.ReplayInterval.Start, End: input.VirtualNow}
			requested := mustInterval(t, "2026-08-10T11:01:00Z", test.defaultEnd)
			input.GlobalDefault = &requested

			result, err := Compile(input)
			var classified *NonOverlapError
			if !errors.As(err, &classified) || classified.Classification != OverlapPermanent || classified.NarrowWindow == nil ||
				len(classified.Sources) != test.wantRejections || result.AuditRequired || result.EffectiveDQL != "" {
				t.Fatalf("result=%#v error=%v classification=%#v", result, err, classified)
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
