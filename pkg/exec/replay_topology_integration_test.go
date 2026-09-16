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
		virtual                 time.Time
	}{
		{"five seconds", "nodes-subminute", "2026-08-01T11:59:55Z", "2026-08-01T12:00:00Z", replayTopologyNow},
		{"intersection leaves twenty seconds", "nodes-window", "2026-08-01T11:00:00Z", "2026-08-01T11:00:20Z", mustReplayTestTime("2026-08-01T11:00:20Z")},
	} {
		for _, clockMode := range []string{session.ReplayClockManual, session.ReplayClockRealtime} {
			for _, disclosure := range []string{session.ReplayDisclosureFull, session.ReplayDisclosureRestricted} {
				for _, mode := range []ReplayExecutionMode{ReplayExecutionOneShot, ReplayExecutionWait, ReplayExecutionLive} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", test.name, clockMode, disclosure, mode), func(t *testing.T) {
						api, original, _ := newTopologyMockAPI(t, test.fixture, false)
						sink := &replayTestSink{}
						fixture := newReplayExecutorFixture(t, api, clockMode, disclosure, replayTopologyStart, test.virtual, replayTopologyEnd,
							func(string) session.ProvenanceSink { return sink })
						result, err := fixture.executor.ExecuteQueryDetailedWithContext(context.Background(), original, DQLExecuteOptions{AgentMode: true, ReplayMode: mode})
						var detail *execreplay.ReplayError
						if result != nil || !errors.As(err, &detail) || detail.Code != execreplay.ErrorTimeframe || ReplayTemporaryNonOverlap(err) || ReplayRetryAfter(err) != 0 {
							t.Fatalf("result=%#v error=%v detail=%#v", result, err, detail)
						}
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
					})
				}
			}
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
