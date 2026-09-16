package output

import (
	"bytes"
	"testing"
)

func TestGolden_ReplayAgentTraversals(t *testing.T) {
	const original = `smartscapeNodes "SERVICE" | traverse "calls", "SERVICE"`
	const effective = `smartscapeNodes from:toTimestamp("2026-08-01T11:59:00Z"), to:toTimestamp("2026-08-01T12:00:00Z"), "SERVICE" | traverse "calls", "SERVICE"`
	metadata := &ReplayMetadata{
		Active: true, SessionID: "synthetic-session", SessionStartedAt: "2026-08-02T09:00:00Z",
		ClockMode: "manual", AnchorHost: "2026-08-02T09:00:00Z", AnchorVirtual: "2026-08-01T12:00:00Z",
		VirtualNow: "2026-08-01T12:00:00Z", DataStart: "2026-08-01T10:00:00Z", DataEnd: "2026-08-01T14:00:00Z",
		VisibleEnd: "2026-08-01T12:00:00Z", State: "active", OriginalQuery: original,
		EffectiveQuery: effective, GrailCanonicalEffectiveQuery: effective, Warnings: []string{},
		Sources: []ReplaySourceMetadata{{
			Ordinal: 0, Path: "root.children[0]", Name: "smartscapeNodes", Class: "topology", BoundaryPolicy: "window_only",
			RequestedFrom: "2026-08-01T11:59:00Z", RequestedTo: "2026-08-01T12:00:00Z",
			EffectiveFrom: "2026-08-01T11:59:00Z", EffectiveTo: "2026-08-01T12:00:00Z",
			PhysicalFrom: "2026-08-01T11:59:00Z", PhysicalTo: "2026-08-01T12:00:00Z",
		}},
		Traversals: []ReplayTraversalMetadata{{
			Path: "root.children[4]", FeederPath: "root.children[0]", FeederOrdinal: 0,
			EffectiveFrom: "2026-08-01T11:59:00Z", EffectiveTo: "2026-08-01T12:00:00Z",
		}},
	}
	var buf bytes.Buffer
	if err := EncodeEnvelope(&buf, Response{
		OK: true, EnvelopeVersion: EnvelopeVersion,
		Result:  &InlineRecords{Kind: KindRecords, Records: []map[string]interface{}{{"id": "synthetic-node"}}},
		Context: &ResponseContext{Verb: "query", Resource: "dql", Decided: "inline"}, Replay: metadata,
	}); err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "query/replay-agent-traversals", buf.String())
}
