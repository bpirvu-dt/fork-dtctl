package exec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dynatrace-oss/dtctl/cmd/testutil"
	execreplay "github.com/dynatrace-oss/dtctl/pkg/exec/replay"
	sdkquery "github.com/dynatrace-oss/dtctl/sdk/api/query"
	"github.com/dynatrace-oss/dtctl/sdk/session"
)

var (
	replayTopologyStart = mustReplayTestTime("2026-08-01T10:00:00Z")
	replayTopologyNow   = mustReplayTestTime("2026-08-01T12:00:00Z")
	replayTopologyEnd   = mustReplayTestTime("2026-08-01T14:00:00Z")
)

func topologyFixtureQuery(t *testing.T, name, file string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "sdk", "api", "query", "testdata", "topology", "fixtures", name, file))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// Both trees are captured query:parse responses. In particular, do not use the
// record mock's timestamp rewriting: topology arguments can be reordered.
func newTopologyMockAPI(t *testing.T, name string, valid bool) (*replayMockAPI, string, string) {
	t.Helper()
	original := topologyFixtureQuery(t, name, "original.dql")
	api := newReplayMockAPI(t)
	api.originalBody = replayFixtureBody(t, "topology/fixtures/"+name+"/parse.json")
	api.disableDynamicValidation = true
	api.isOriginal = func(query string) bool { return query == original }
	effective := ""
	if valid {
		effective = topologyFixtureQuery(t, name, "effective.dql")
		api.validationBody = replayFixtureBody(t, "topology/fixtures/"+name+"/validation-parse.json")
	}
	api.executeResponse = sdkquery.Response{State: "SUCCEEDED", Result: &sdkquery.Result{
		// Later field values are deliberately retained. Topology has no result
		// timestamp contract and must not clamp lifetime.end to virtual now.
		Records:  []map[string]interface{}{{"id": "synthetic-node", "lifetime": map[string]interface{}{"end": "2026-08-01T14:00:00Z"}}},
		Metadata: &sdkquery.Metadata{Grail: &sdkquery.GrailMetadata{CanonicalQuery: effective}},
	}}
	return api, original, effective
}

func topologyExecutionRecord(t *testing.T, sink *replayTestSink) session.ReplayProvenanceRecord {
	t.Helper()
	_, _, records := sink.snapshot()
	for _, record := range records {
		if record.Event == "query_execution" {
			return record
		}
	}
	t.Fatalf("missing execution provenance: %#v", records)
	return session.ReplayProvenanceRecord{}
}

func TestDQLExecutorTopologyDisclosuresUseIdenticalDQLAndRecords(t *testing.T) {
	for _, name := range []string{"nodes-bare", "nodes-list", "nodes-wildcard", "nodes-window", "nodes-timeframe", "edges-bare", "edges-runs-on", "edges-window", "traverse-window", "traverse-limit", "traverse-chain", "traverse-edges-feeder", "traverse-nested"} {
		t.Run(name, func(t *testing.T) {
			var fullRecords []map[string]interface{}
			var fullDQL string
			for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
				t.Run(disclosure, func(t *testing.T) {
					api, original, effective := newTopologyMockAPI(t, name, true)
					sink := &replayTestSink{}
					fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure, replayTopologyStart, replayTopologyNow, replayTopologyEnd,
						func(string) session.ProvenanceSink { return sink })
					result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{AgentMode: true})
					if err != nil || result == nil || result.Response == nil || result.Replay == nil {
						t.Fatalf("result=%#v error=%v", result, err)
					}
					parses, executions := api.queries()
					coverage, _ := api.coverage()
					if len(parses) != 2 || len(executions) != 1 || len(coverage) != 0 || executions[0].Query != effective {
						t.Fatalf("parses=%#v executions=%#v coverage=%#v; expected %q", parses, executions, coverage, effective)
					}
					if !reflect.DeepEqual(result.Response.Result.Records, api.executeResponse.Result.Records) {
						t.Fatalf("topology records were altered: %#v", result.Response.Result.Records)
					}
					topologySources := 0
					for _, source := range result.Replay.Output.Sources {
						if source.Class != "topology" {
							continue
						}
						topologySources++
						if source.BoundaryPolicy != "window_only" || source.PhysicalFrom != source.EffectiveFrom || source.PhysicalTo != source.EffectiveTo || source.NaturalIntervalNS != 0 {
							t.Fatalf("topology source contract = %#v", source)
						}
					}
					if topologySources != 1 {
						t.Fatalf("topology source count = %d", topologySources)
					}
					bindings := result.Replay.Output.Traversals
					wantBindings := strings.Count(original, "traverse ")
					if len(bindings) != wantBindings {
						t.Fatalf("traversal bindings = %#v, want %d", bindings, wantBindings)
					}
					for _, binding := range bindings {
						source := result.Replay.Output.Sources[binding.FeederOrdinal]
						if binding.FeederPath != source.Path || binding.Path == "" || binding.EffectiveFrom != source.EffectiveFrom || binding.EffectiveTo != source.EffectiveTo {
							t.Fatalf("binding=%#v source=%#v", binding, source)
						}
					}
					if disclosure == session.ReplayDisclosureFull {
						fullDQL, fullRecords = effective, result.Response.Result.Records
						if ReplayOutputMetadata(result.Replay) == nil {
							t.Fatal("full metadata missing")
						}
						return
					}
					if fullDQL != effective || !reflect.DeepEqual(fullRecords, result.Response.Result.Records) || ReplayOutputMetadata(result.Replay) != nil {
						t.Fatal("disclosures changed effective DQL/data or exposed restricted metadata")
					}
					record := topologyExecutionRecord(t, sink)
					if record.Fields["effective_dql"] != effective || record.Fields["original_dql"] != original {
						t.Fatalf("query provenance=%#v", record.Fields)
					}
					if contracts, ok := record.Fields["validated_result_contracts"]; ok && len(contracts.([]map[string]any)) != 0 {
						t.Fatalf("unexpected topology result validation: %#v", contracts)
					}
					if wantBindings > 0 && !reflect.DeepEqual(record.Fields["traversals"], replayTraversalBindingsFromOutput(result.Replay)) {
						t.Fatalf("private traversal bindings differ: %#v", record.Fields["traversals"])
					}
					if name == "traverse-limit" {
						record.SessionID = "0123456789abcdef0123456789abcdef"
						record.Fields["session"].(map[string]any)["id"] = record.SessionID
						encoded, err := json.Marshal(record)
						if err != nil {
							t.Fatal(err)
						}
						testutil.AssertGolden(t, "replay/restricted-topology-provenance-jsonl", string(encoded)+"\n")
					}
				})
			}
		})
	}
}

func replayTraversalBindingsFromOutput(info *ReplayExecutionInfo) []map[string]any {
	var bindings []map[string]any
	for _, binding := range info.Output.Traversals {
		bindings = append(bindings, map[string]any{
			"path": binding.Path, "feeder_path": binding.FeederPath, "feeder_ordinal": binding.FeederOrdinal,
			"effective_range": map[string]string{"start": binding.EffectiveFrom, "end": binding.EffectiveTo},
		})
	}
	return bindings
}

func TestDQLExecutorTopologyWidthFailuresAreHardAndRecorded(t *testing.T) {
	for _, test := range []struct {
		name, fixture, from, to string
		start, virtual          time.Time
	}{
		{"five seconds", "nodes-subminute", "2026-08-01T11:59:55Z", "2026-08-01T12:00:00Z", replayTopologyStart, replayTopologyNow},
		{"intersection leaves twenty seconds", "nodes-window", "2026-08-01T11:00:00Z", "2026-08-01T11:00:20Z", replayTopologyStart, mustReplayTestTime("2026-08-01T11:00:20Z")},
		{"aligned endpoint", "nodes-aligned-start", "2026-08-01T00:00:00Z", "2026-08-01T00:00:20Z", mustReplayTestTime("2026-07-31T23:59:00Z"), mustReplayTestTime("2026-08-01T00:00:20Z")},
	} {
		for _, clockMode := range []string{session.ReplayClockManual, session.ReplayClockRealtime} {
			for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
				for _, mode := range []ReplayExecutionMode{ReplayExecutionOneShot, ReplayExecutionWait, ReplayExecutionLive} {
					if test.fixture == "nodes-window" && clockMode == session.ReplayClockRealtime && mode != ReplayExecutionOneShot {
						continue // These widening windows have their own retry test below.
					}
					t.Run(fmt.Sprintf("%s/%s/%s/%s", test.name, clockMode, disclosure, mode), func(t *testing.T) {
						api, original, _ := newTopologyMockAPI(t, test.fixture, false)
						sink := &replayTestSink{}
						fixture := newReplayExecutorFixture(t, api, clockMode, disclosure, test.start, test.virtual, replayTopologyEnd,
							func(string) session.ProvenanceSink { return sink })
						result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{AgentMode: true, ReplayMode: mode})
						var detail *execreplay.ReplayError
						var classified *execreplay.NonOverlapError
						if result != nil || !errors.As(err, &detail) || detail.Code != execreplay.ErrorTimeframe || ReplayTemporaryNonOverlap(err) || ReplayRetryAfter(err) != 0 || !ReplayLoopHardFailure(err) || !errors.As(err, &classified) {
							t.Fatalf("result=%#v error=%v detail=%#v", result, err, detail)
						}
						wantClass := execreplay.OverlapPermanent
						if test.fixture == "nodes-window" {
							wantClass = execreplay.OverlapTemporary
						} else if test.fixture == "nodes-aligned-start" {
							wantClass = execreplay.OverlapUnknown
						}
						assertTopologyWidthProof(t, classified, wantClass, test.from, test.to)
						if parses, executions, _ := api.counts(); parses != 1 || executions != 0 {
							t.Fatalf("parse=%d execute=%d", parses, executions)
						}
						if disclosure == session.ReplayDisclosureFull {
							if !strings.Contains(err.Error(), "60 seconds") || !strings.Contains(err.Error(), test.from) || !strings.Contains(err.Error(), test.to) {
								t.Fatalf("full width error=%v", err)
							}
							return
						}
						if err.Error() != restrictedQueryInvalidMessage {
							t.Fatalf("restricted width error=%v", err)
						}
						_, _, records := sink.snapshot()
						if len(records) != 1 || !strings.Contains(fmt.Sprint(records[0].Fields["detail"]), "60 seconds") {
							t.Fatalf("width provenance=%#v", records)
						}
						fields := records[0].Fields["sources"].([]map[string]any)[0]
						if got := fields["logical_effective_range"]; !reflect.DeepEqual(got, map[string]string{"start": test.from, "end": test.to}) {
							t.Fatalf("failed effective window=%#v", got)
						}
						proof := fields["overlap"].(map[string]any)
						if proof["classification"] != wantClass || proof["reason"] == "" {
							t.Fatalf("width proof=%#v", proof)
						}
					})
				}
			}
		}
	}
}

func assertTopologyWidthProof(t *testing.T, classified *execreplay.NonOverlapError, want execreplay.OverlapClassification, from, to string) {
	t.Helper()
	if classified == nil || classified.NarrowWindow == nil || classified.Classification != want || len(classified.Sources) != 1 {
		t.Fatalf("width classification=%#v", classified)
	}
	source := classified.Sources[0]
	if source.Effective == nil || source.Effective.Start.Format(time.RFC3339Nano) != from || source.Effective.End.Format(time.RFC3339Nano) != to ||
		source.Classification != want || source.Proof.Classification != want || source.Proof.Reason == "" {
		t.Fatalf("width source proof=%#v", source)
	}
}

func TestDQLExecutorTopologyMixedNonOverlapUsesDecidingSources(t *testing.T) {
	const fullNonOverlap = "The requested source timeframe does not overlap the currently visible replay interval.\nThe query was not executed."
	for _, test := range []struct {
		name, start, now, end, from, to  string
		clockMode                        string
		mode                             ReplayExecutionMode
		recordClass, topologyClass       execreplay.OverlapClassification
		retryable, narrow, emptyTopology bool
	}{
		{
			name: "permanent logs and temporary topology", start: "2026-08-10T11:00:00Z", now: "2026-08-10T11:01:20Z", end: "2026-08-10T14:00:00Z",
			from: "2026-08-10T11:01:00Z", to: "2026-08-10T11:10:00Z", recordClass: execreplay.OverlapPermanent, topologyClass: execreplay.OverlapTemporary,
		},
		{
			name: "temporary logs and temporary topology", start: "2026-08-10T09:00:00Z", now: "2026-08-10T09:01:20Z", end: "2026-08-10T12:00:00Z",
			from: "2026-08-10T09:01:00Z", to: "2026-08-10T09:10:00Z", recordClass: execreplay.OverlapTemporary, topologyClass: execreplay.OverlapTemporary, retryable: true,
		},
		{
			name: "temporary logs and permanent topology", start: "2026-08-10T09:00:00Z", now: "2026-08-10T09:01:20Z", end: "2026-08-10T12:00:00Z",
			from: "2026-08-10T09:01:00Z", to: "2026-08-10T09:01:30Z", recordClass: execreplay.OverlapTemporary, topologyClass: execreplay.OverlapPermanent, narrow: true,
		},
		{
			name: "one-shot temporary logs and topology", start: "2026-08-10T09:00:00Z", now: "2026-08-10T09:01:20Z", end: "2026-08-10T12:00:00Z",
			from: "2026-08-10T09:01:00Z", to: "2026-08-10T09:10:00Z", recordClass: execreplay.OverlapTemporary, topologyClass: execreplay.OverlapTemporary,
			mode: ReplayExecutionOneShot,
		},
		{
			name: "manual temporary logs and topology", start: "2026-08-10T09:00:00Z", now: "2026-08-10T09:01:20Z", end: "2026-08-10T12:00:00Z",
			from: "2026-08-10T09:01:00Z", to: "2026-08-10T09:10:00Z", recordClass: execreplay.OverlapTemporary, topologyClass: execreplay.OverlapTemporary,
			clockMode: session.ReplayClockManual,
		},
		{
			name: "permanent logs and empty future subminute topology", start: "2026-08-10T11:00:00Z", now: "2026-08-10T12:00:00Z", end: "2026-08-10T14:00:00Z",
			from: "2026-08-10T12:30:00Z", to: "2026-08-10T12:30:30Z", recordClass: execreplay.OverlapPermanent, topologyClass: execreplay.OverlapPermanent, emptyTopology: true,
		},
	} {
		if test.clockMode == "" {
			test.clockMode = session.ReplayClockRealtime
		}
		if test.mode == "" {
			test.mode = ReplayExecutionWait
		}
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			t.Run(test.name+"/"+disclosure, func(t *testing.T) {
				api, original, _ := newTopologyMockAPI(t, "old-nodes-control", false)
				sink := &replayTestSink{}
				fixture := newReplayExecutorFixture(t, api, test.clockMode, disclosure,
					mustReplayTestTime(test.start), mustReplayTestTime(test.now), mustReplayTestTime(test.end),
					func(string) session.ProvenanceSink { return sink })
				result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{
					AgentMode: true, ReplayMode: test.mode, DefaultTimeframeStart: test.from, DefaultTimeframeEnd: test.to,
				})
				var classified *execreplay.NonOverlapError
				if result != nil || !errors.As(err, &classified) || ReplayTemporaryNonOverlap(err) != test.retryable ||
					ReplayLoopHardFailure(err) == test.retryable || ReplayRetryAfter(err) != 0 {
					t.Fatalf("result=%#v error=%v classification=%#v", result, err, classified)
				}
				wantClass := execreplay.OverlapPermanent
				if test.recordClass == execreplay.OverlapTemporary && test.topologyClass == execreplay.OverlapTemporary {
					wantClass = execreplay.OverlapTemporary
				}
				if classified.Classification != wantClass || (classified.NarrowWindow != nil) != test.narrow || len(classified.Sources) != 2 {
					t.Fatalf("mixed classification=%#v", classified)
				}
				record, topology := classified.Sources[0], classified.Sources[1]
				topologyWindowOK := topology.Effective == nil
				if !test.emptyTopology {
					topologyWindowOK = topology.Effective != nil && topology.Effective.Start.Format(time.RFC3339Nano) == test.from &&
						topology.Effective.End.Format(time.RFC3339Nano) == test.now
				}
				if record.Class != execreplay.SourceRecord || record.Classification != test.recordClass || record.Effective != nil ||
					topology.Class != execreplay.SourceTopology || topology.Classification != test.topologyClass || !topologyWindowOK {
					t.Fatalf("record=%#v topology=%#v", record, topology)
				}
				if test.emptyTopology && (!strings.Contains(topology.Proof.Reason, "empty") || !strings.Contains(topology.Proof.Reason, "60 seconds")) {
					t.Fatalf("empty topology lost its width proof: %#v", topology.Proof)
				}
				assertTopologyRejectedBeforeEffectiveParse(t, api, original)
				if disclosure == session.ReplayDisclosureFull {
					switch {
					case test.narrow:
						if !strings.Contains(err.Error(), "60 seconds") || strings.Contains(err.Error(), "does not overlap") {
							t.Fatalf("full topology error=%v", err)
						}
					case test.retryable:
						if err.Error() != "no visible overlap yet" {
							t.Fatalf("full retry=%v", err)
						}
					default:
						if err.Error() != fullNonOverlap {
							t.Fatalf("full non-overlap=%v", err)
						}
					}
					return
				}
				wantMessage, wantOutcome := "No data is available for the requested timeframe.", "non_overlap"
				if test.narrow {
					wantMessage, wantOutcome = "The query could not be run as written.", "preparation"
				} else if test.retryable {
					wantMessage = "The requested timeframe is not available yet."
				}
				if err.Error() != wantMessage {
					t.Fatalf("restricted error=%v, want %q", err, wantMessage)
				}
				assertTopologyRejectionProvenance(t, sink, classified, wantOutcome)
			})
		}
	}
}

func TestDQLExecutorTopologyEmptyFutureSubminuteFailsWithoutWaiting(t *testing.T) {
	const fullNonOverlap = "The requested source timeframe does not overlap the currently visible replay interval.\nThe query was not executed."
	for _, clockMode := range []string{session.ReplayClockManual, session.ReplayClockRealtime} {
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			for _, mode := range []ReplayExecutionMode{ReplayExecutionOneShot, ReplayExecutionWait, ReplayExecutionLive} {
				t.Run(fmt.Sprintf("%s/%s/%s", clockMode, disclosure, mode), func(t *testing.T) {
					api, original, _ := newTopologyMockAPI(t, "nodes-bare", false)
					sink := &replayTestSink{}
					fixture := newReplayExecutorFixture(t, api, clockMode, disclosure, replayTopologyStart, replayTopologyNow, replayTopologyEnd,
						func(string) session.ProvenanceSink { return sink })
					waits := 0
					fixture.executor.preparer.(*ReplayQueryPreparer).config.WaitFunc = func(context.Context, time.Duration) error {
						waits++
						return nil
					}
					result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{
						AgentMode: true, ReplayMode: mode,
						DefaultTimeframeStart: "2026-08-01T12:30:00Z", DefaultTimeframeEnd: "2026-08-01T12:30:30Z",
					})
					var classified *execreplay.NonOverlapError
					if result != nil || !errors.As(err, &classified) || !ReplayLoopHardFailure(err) || ReplayTemporaryNonOverlap(err) ||
						ReplayRetryAfter(err) != 0 || waits != 0 {
						t.Fatalf("result=%#v error=%v waits=%d", result, err, waits)
					}
					if classified.Classification != execreplay.OverlapPermanent || classified.NarrowWindow != nil || len(classified.Sources) != 1 {
						t.Fatalf("empty intersection classification=%#v", classified)
					}
					source := classified.Sources[0]
					if source.Effective != nil || source.Class != execreplay.SourceTopology || source.Classification != execreplay.OverlapPermanent ||
						source.Proof.Classification != execreplay.OverlapPermanent || !strings.Contains(source.Proof.Reason, "empty") || !strings.Contains(source.Proof.Reason, "60 seconds") {
						t.Fatalf("empty intersection proof=%#v", source)
					}
					assertTopologyRejectedBeforeEffectiveParse(t, api, original)
					if disclosure == session.ReplayDisclosureFull {
						if err.Error() != fullNonOverlap {
							t.Fatalf("full non-overlap=%v", err)
						}
						return
					}
					if err.Error() != "No data is available for the requested timeframe." {
						t.Fatalf("restricted non-overlap=%v", err)
					}
					assertTopologyRejectionProvenance(t, sink, classified, "non_overlap")
				})
			}
		}
	}
}

func assertTopologyRejectedBeforeEffectiveParse(t *testing.T, api *replayMockAPI, original string) {
	t.Helper()
	parses, executions := api.queries()
	coverage, order := api.coverage()
	if len(parses) != 1 || parses[0].Query != original || len(executions) != 0 || len(coverage) != 0 ||
		!reflect.DeepEqual(order, []string{"original_parse"}) {
		t.Fatalf("parse=%#v execute=%#v coverage=%#v order=%v", parses, executions, coverage, order)
	}
}

func assertTopologyRejectionProvenance(t *testing.T, sink *replayTestSink, classified *execreplay.NonOverlapError, outcome string) {
	t.Helper()
	_, _, records := sink.snapshot()
	if len(records) != 1 || records[0].Fields["outcome"] != outcome || records[0].Fields["effective_dql"] != "" {
		t.Fatalf("rejection provenance=%#v, want outcome %s without effective DQL", records, outcome)
	}
	sources, ok := records[0].Fields["sources"].([]map[string]any)
	if !ok || len(sources) != len(classified.Sources) {
		t.Fatalf("rejection sources=%#v", records[0].Fields["sources"])
	}
	for index, source := range classified.Sources {
		fields := sources[index]
		proof, ok := fields["overlap"].(map[string]any)
		if !ok || fields["ordinal"] != source.Ordinal || fields["class"] != source.Class || proof["classification"] != source.Classification ||
			proof["reason"] != source.Proof.Reason || proof["reason"] == "" || proof["requested_now"] == nil || proof["visible_now"] == nil ||
			proof["replay_interval"] == nil || proof["terminal_requested"] == nil || fields["requested_range"] == nil {
			t.Fatalf("source %d lost classification or proof: %#v", index, fields)
		}
		if source.Effective == nil {
			if _, exists := fields["logical_effective_range"]; exists {
				t.Fatalf("source %d invented an effective range: %#v", index, fields)
			}
			continue
		}
		want := map[string]string{"start": source.Effective.Start.Format(time.RFC3339Nano), "end": source.Effective.End.Format(time.RFC3339Nano)}
		if !reflect.DeepEqual(fields["logical_effective_range"], want) || !reflect.DeepEqual(fields["physical_range"], want) {
			t.Fatalf("source %d lost or widened its effective range: %#v", index, fields)
		}
	}
}

func TestDQLExecutorTopologyTemporaryWidthRecomputesAndThenExecutes(t *testing.T) {
	for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
		for _, mode := range []ReplayExecutionMode{ReplayExecutionWait, ReplayExecutionLive} {
			t.Run(disclosure+"/"+string(mode), func(t *testing.T) {
				api, original, _ := newTopologyMockAPI(t, "nodes-window", false)
				effective := topologyFixtureQuery(t, "nodes-window", "effective-first-minute.dql")
				api.validationBody = replayFixtureBody(t, "topology/fixtures/nodes-window/validation-first-minute-parse.json")
				sink := &replayTestSink{}
				virtual := mustReplayTestTime("2026-08-01T11:00:20Z")
				fixture := newReplayExecutorFixture(t, api, session.ReplayClockRealtime, disclosure, replayTopologyStart, virtual, replayTopologyEnd,
					func(string) session.ProvenanceSink { return sink })
				var waits []time.Duration
				fixture.executor.preparer.(*ReplayQueryPreparer).config.WaitFunc = func(_ context.Context, delay time.Duration) error {
					waits = append(waits, delay)
					fixture.clock.Add(delay)
					return nil
				}
				opts := DQLExecuteOptions{AgentMode: true, ReplayMode: mode}
				first, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, opts)
				var classified *execreplay.NonOverlapError
				if first != nil || !ReplayTemporaryNonOverlap(err) || ReplayLoopHardFailure(err) || !errors.As(err, &classified) {
					t.Fatalf("first result=%#v error=%v", first, err)
				}
				assertTopologyWidthProof(t, classified, execreplay.OverlapTemporary, "2026-08-01T11:00:00Z", "2026-08-01T11:00:20Z")
				if parses, executes, _ := api.counts(); parses != 1 || executes != 0 {
					t.Fatalf("temporary parse=%d execute=%d", parses, executes)
				}
				if disclosure == session.ReplayDisclosureRestricted {
					if err.Error() != restrictedTemporaryNoDataMessage {
						t.Fatalf("restricted retry=%v", err)
					}
					_, _, records := sink.snapshot()
					if len(records) != 1 {
						t.Fatalf("retry provenance=%#v", records)
					}
					fields := records[0].Fields["sources"].([]map[string]any)[0]
					proof := fields["overlap"].(map[string]any)
					if !reflect.DeepEqual(fields["logical_effective_range"], map[string]string{"start": "2026-08-01T11:00:00Z", "end": "2026-08-01T11:00:20Z"}) ||
						proof["classification"] != execreplay.OverlapTemporary || proof["reason"] == "" {
						t.Fatalf("retry window and proof=%#v", fields)
					}
				} else if !strings.Contains(err.Error(), "60 seconds") || !strings.Contains(err.Error(), "2026-08-01T11:00:00Z") || !strings.Contains(err.Error(), "2026-08-01T11:00:20Z") {
					t.Fatalf("full retry=%v", err)
				}
				info, ok := ReplayErrorInfo(err)
				if !ok || !info.VirtualNow.Equal(virtual) {
					t.Fatalf("retry info=%#v", info)
				}
				cadence := 40 * time.Second
				if err := fixture.executor.WaitForNextAttempt(context.Background(), cadence, &info, err); err != nil {
					t.Fatal(err)
				}
				second, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, opts)
				if err != nil || second == nil || second.Response == nil {
					t.Fatalf("second result=%#v error=%v", second, err)
				}
				if parses, executes, _ := api.counts(); parses != 2 || executes != 1 {
					t.Fatalf("after widening parse=%d execute=%d", parses, executes)
				}
				source := second.Replay.Output.Sources[0]
				_, requests := api.queries()
				if len(waits) != 1 || waits[0] != cadence || source.EffectiveFrom != "2026-08-01T11:00:00Z" || source.EffectiveTo != "2026-08-01T11:01:00Z" || len(requests) != 1 || requests[0].Query != effective {
					t.Fatalf("waits=%v source=%#v requests=%#v", waits, source, requests)
				}
			})
		}
	}
}

func TestDQLExecutorTopologyTerminalWidthNeverRetries(t *testing.T) {
	for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
		for _, mode := range []ReplayExecutionMode{ReplayExecutionOneShot, ReplayExecutionWait, ReplayExecutionLive} {
			t.Run(disclosure+"/"+string(mode), func(t *testing.T) {
				terminal := mustReplayTestTime("2026-08-01T11:00:20Z")
				api, original, _ := newTopologyMockAPI(t, "nodes-window", false)
				fixture := newReplayExecutorFixture(t, api, session.ReplayClockRealtime, disclosure, replayTopologyStart, terminal, terminal,
					func(string) session.ProvenanceSink { return &replayTestSink{} })
				result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{ReplayMode: mode})
				var classified *execreplay.NonOverlapError
				if result != nil || !errors.As(err, &classified) || ReplayTemporaryNonOverlap(err) || !ReplayLoopHardFailure(err) {
					t.Fatalf("terminal result=%#v error=%v", result, err)
				}
				assertTopologyWidthProof(t, classified, execreplay.OverlapPermanent, "2026-08-01T11:00:00Z", "2026-08-01T11:00:20Z")
				if parses, executes, _ := api.counts(); parses != 1 || executes != 0 {
					t.Fatalf("terminal parse=%d execute=%d", parses, executes)
				}
			})
		}
	}
}

func TestDQLExecutorTopologyAuthorizationRejectsBeforeExecute(t *testing.T) {
	for _, name := range []string{"edges-wildcard", "edges-contains", "edges-pattern", "traverse-wildcard", "traverse-contains", "traverse-after-data", "traverse-after-fetch", "traverse-after-append", "traverse-after-join", "traverse-after-lookup", "traverse-nested-no-feeder"} {
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			t.Run(name+"/"+disclosure, func(t *testing.T) {
				api, original, _ := newTopologyMockAPI(t, name, false)
				sink := &replayTestSink{}
				fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure, replayTopologyStart, replayTopologyNow, replayTopologyEnd,
					func(string) session.ProvenanceSink { return sink })
				result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{AgentMode: true})
				if err == nil || result != nil || ReplayTemporaryNonOverlap(err) {
					t.Fatalf("result=%#v error=%v", result, err)
				}
				if parses, executions, _ := api.counts(); parses != 1 || executions != 0 {
					t.Fatalf("parse=%d execute=%d", parses, executions)
				}
				if strings.Contains(name, "wildcard") || strings.Contains(name, "contains") || name == "edges-pattern" {
					if !strings.Contains(err.Error(), "calls") || !strings.Contains(err.Error(), "runs_on") {
						t.Fatalf("edge error does not name supported types: %v", err)
					}
				} else if !strings.Contains(err.Error(), "traverse") {
					t.Fatalf("feeder error does not name traverse: %v", err)
				}
				if disclosure == session.ReplayDisclosureRestricted {
					_, _, records := sink.snapshot()
					if len(records) != 1 || records[0].Fields["detail"] == "" {
						t.Fatalf("missing private rejection: %#v", records)
					}
				}
			})
		}
	}
}

func TestDQLExecutorTopologyFailurePathsWithholdResults(t *testing.T) {
	for _, test := range []struct {
		name     string
		mutate   func(*replayMockAPI, *replayTestSink)
		parses   int
		executes int
	}{
		{"sink preflight", func(_ *replayMockAPI, sink *replayTestSink) { sink.preflightErr = errors.New("synthetic sink failure") }, 0, 0},
		{"original parse", func(api *replayMockAPI, _ *replayTestSink) { api.parseStatus = http.StatusBadRequest }, 1, 0},
		{"effective parse", func(api *replayMockAPI, _ *replayTestSink) { api.effectiveStatus = http.StatusBadRequest }, 2, 0},
		{"audit changed selector", func(api *replayMockAPI, _ *replayTestSink) {
			api.validationBody = bytes.ReplaceAll(api.validationBody, []byte(`\"calls\"`), []byte(`\"runs_on\"`))
		}, 2, 0},
		{"remote execution", func(api *replayMockAPI, _ *replayTestSink) { api.executeStatus = http.StatusBadRequest }, 2, 1},
		{"notice append before execution", func(_ *replayMockAPI, sink *replayTestSink) {
			sink.appendFailures = map[int]error{1: errors.New("synthetic notice append failure")}
		}, 2, 0},
		{"sink append", func(_ *replayMockAPI, sink *replayTestSink) {
			sink.appendFailures = map[int]error{2: errors.New("synthetic append failure")}
		}, 2, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			api, original, _ := newTopologyMockAPI(t, "traverse-limit", true)
			sink := &replayTestSink{}
			test.mutate(api, sink)
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureRestricted, replayTopologyStart, replayTopologyNow, replayTopologyEnd,
				func(string) session.ProvenanceSink { return sink })
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{AgentMode: true})
			if err == nil || result != nil {
				t.Fatalf("result=%#v error=%v", result, err)
			}
			if parses, executions, _ := api.counts(); parses != test.parses || executions != test.executes {
				t.Fatalf("parse=%d execute=%d, want %d/%d: %v", parses, executions, test.parses, test.executes, err)
			}
			if strings.Contains(err.Error(), "2026-") || strings.Contains(err.Error(), "synthetic") || strings.Contains(strings.ToLower(err.Error()), "replay") {
				t.Fatalf("restricted failure leaked details: %v", err)
			}
		})
	}
}

func TestDQLExecutorTopologyTraverseOwnBoundsParserRejection(t *testing.T) {
	for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
		t.Run(disclosure, func(t *testing.T) {
			api, original, _ := newTopologyMockAPI(t, "traverse-own-from", false)
			// This real capture is a parser rejection, not a successful AST.
			api.parseStatus, api.parseErrorBody = http.StatusBadRequest, api.originalBody
			sink := &replayTestSink{}
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure, replayTopologyStart, replayTopologyNow, replayTopologyEnd,
				func(string) session.ProvenanceSink { return sink })
			result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{AgentMode: true})
			var queryErr *sdkquery.QueryError
			if result != nil || !errors.As(err, &queryErr) || queryErr.ErrorType != "UNKNOWN_PARAMETER_DEFINED" || !strings.Contains(err.Error(), "from") {
				t.Fatalf("result=%#v error=%v", result, err)
			}
			if parses, executions, _ := api.counts(); parses != 1 || executions != 0 {
				t.Fatalf("parse=%d execute=%d", parses, executions)
			}
		})
	}
}

func TestDQLExecutorTopologyTimeframePrecedence(t *testing.T) {
	for _, test := range []struct {
		name, fixture, from, to string
		defaults                bool
	}{
		{"bare fallback", "nodes-bare", "2026-08-01T11:59:00Z", "2026-08-01T12:00:00Z", false},
		{"request defaults", "nodes-defaults", "2026-08-01T10:30:00Z", "2026-08-01T11:00:00Z", true},
		{"source bounds override defaults", "nodes-window", "2026-08-01T11:00:00Z", "2026-08-01T12:00:00Z", true},
		{"source timeframe overrides defaults", "nodes-timeframe", "2026-08-01T11:00:00Z", "2026-08-01T12:00:00Z", true},
	} {
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			t.Run(test.name+"/"+disclosure, func(t *testing.T) {
				api, original, effective := newTopologyMockAPI(t, test.fixture, true)
				fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure, replayTopologyStart, replayTopologyNow, replayTopologyEnd,
					func(string) session.ProvenanceSink { return &replayTestSink{} })
				opts := DQLExecuteOptions{AgentMode: true}
				if test.defaults {
					opts.DefaultTimeframeStart, opts.DefaultTimeframeEnd = "2026-08-01T10:30:00Z", "2026-08-01T11:00:00Z"
				}
				result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, opts)
				if err != nil || result == nil {
					t.Fatalf("result=%#v error=%v", result, err)
				}
				source := result.Replay.Output.Sources[0]
				_, requests := api.queries()
				if source.EffectiveFrom != test.from || source.EffectiveTo != test.to || len(requests) != 1 || requests[0].Query != effective {
					t.Fatalf("source=%#v requests=%#v expected=%q", source, requests, effective)
				}
			})
		}
	}
	for _, options := range []DQLExecuteOptions{
		{DefaultTimeframeStart: "2026-08-01T10:30:00Z"},
		{DefaultTimeframeEnd: "2026-08-01T11:00:00Z"},
		{DefaultTimeframeStart: "invalid", DefaultTimeframeEnd: "2026-08-01T11:00:00Z"},
		{DefaultTimeframeStart: "2026-08-01T12:00:00Z", DefaultTimeframeEnd: "2026-08-01T11:00:00Z"},
	} {
		t.Run(fmt.Sprintf("invalid-defaults/%s/%s", options.DefaultTimeframeStart, options.DefaultTimeframeEnd), func(t *testing.T) {
			api, original, _ := newTopologyMockAPI(t, "nodes-bare", false)
			fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayTopologyStart, replayTopologyNow, replayTopologyEnd, nil)
			if result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, options); err == nil || result != nil {
				t.Fatalf("invalid defaults used fallback: result=%#v error=%v", result, err)
			}
			if parses, executes, _ := api.counts(); parses != 1 || executes != 0 {
				t.Fatalf("parse=%d execute=%d", parses, executes)
			}
		})
	}
}

func TestDQLExecutorTopologyOldNodesRejectionControlNowExecutes(t *testing.T) {
	api, original, effective := newTopologyMockAPI(t, "old-nodes-control", true)
	// Keep the August fixture as an actual regression control for its old shape.
	api.originalBody = replayFixtureBody(t, "phase0b/fixtures/current-state-rejection/01-smartscape-nodes/historical-context/parse.json")
	fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, session.ReplayDisclosureFull, replayRecordDataStart, replayRecordVirtual, replayRecordDataEnd, nil)
	result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{AgentMode: true})
	if err != nil || result == nil {
		t.Fatalf("old topology control failed: %v", err)
	}
	_, requests := api.queries()
	if len(requests) != 1 || requests[0].Query != effective || len(result.Replay.Output.Sources) != 2 || result.Replay.Output.Sources[1].Class != "topology" {
		t.Fatalf("requests=%#v metadata=%#v", requests, result.Replay.Output)
	}
}

func TestDQLExecutorTopologyRetentionIsNotVerifiedForPureAndMixedQueries(t *testing.T) {
	for _, failInspection := range []bool{false, true} {
		for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
			t.Run(fmt.Sprintf("inspection-fails=%t/%s", failInspection, disclosure), func(t *testing.T) {
				var firstWarnings []string
				for _, name := range []string{"nodes-bare", "traverse-nested"} {
					api, original, _ := newTopologyMockAPI(t, name, true)
					sink := &replayTestSink{}
					fixture := newReplayExecutorFixture(t, api, session.ReplayClockManual, disclosure, replayTopologyStart, replayTopologyNow, replayTopologyEnd,
						func(string) session.ProvenanceSink { return sink })
					calls := 0
					fixture.executor.preparer.(*ReplayQueryPreparer).config.RetentionInspector = replayRetentionInspectorFunc(func(ctx context.Context) (RetentionInspection, error) {
						calls++
						if failInspection {
							return RetentionInspection{}, errors.New("synthetic retention failure")
						}
						return replayVerifiedRetentionInspector().Inspect(ctx)
					})
					result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{AgentMode: true})
					if err != nil || result == nil || calls != 1 {
						t.Fatalf("result=%#v error=%v inspection calls=%d", result, err, calls)
					}
					warnings := result.Replay.Output.Warnings
					if len(warnings) != 1 || !strings.Contains(warnings[0], "not verified") {
						t.Fatalf("topology retention warnings=%#v", warnings)
					}
					if firstWarnings == nil {
						firstWarnings = warnings
					} else if !reflect.DeepEqual(firstWarnings, warnings) {
						t.Fatalf("pure and mixed retention outcomes differ: %v vs %v", firstWarnings, warnings)
					}
					if disclosure == session.ReplayDisclosureRestricted {
						record := topologyExecutionRecord(t, sink)
						notices := record.Fields["notices"].([]map[string]any)
						if len(notices) != 1 || notices[0]["code"] != execreplay.NoticeRetentionNotVerified || ReplayOutputMetadata(result.Replay) != nil {
							t.Fatalf("restricted retention notices=%#v", notices)
						}
					}
				}
			})
		}
	}
}
